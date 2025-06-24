//! Ensures that `Pod`s are configured and running for each [`v1alpha1::ZookeeperCluster`]
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    hash::Hasher,
    str::FromStr,
    sync::Arc,
};

use const_format::concatcp;
use fnv::FnvHasher;
use indoc::formatdoc;
use product_config::{
    ProductConfigManager,
    types::PropertyNameKind,
    writer::{PropertiesWriterError, to_java_properties_string},
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            volume::{ListenerOperatorVolumeSourceBuilder, ListenerReference},
        },
    },
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{product_image_selection::ResolvedProductImage, rbac::build_rbac_resources},
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, EmptyDirVolumeSource, EnvVar, EnvVarSource,
                ExecAction, ObjectFieldSelector, PersistentVolumeClaim, PodSecurityContext, Probe,
                Service, ServiceAccount, ServicePort, ServiceSpec, Volume,
            },
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::{
        Resource, ResourceExt,
        api::DynamicObject,
        core::{DeserializeGuard, error_boundary},
        runtime::controller,
    },
    kvp::{Label, LabelError, Labels},
    logging::controller::ReconcilerError,
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{
        self,
        framework::{
            LoggingError, create_vector_shutdown_file_command, remove_vector_shutdown_file_command,
        },
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::RoleGroupRef,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    time::Duration,
    utils::{COMMON_BASH_TRAP_FUNCTIONS, cluster_info::KubernetesClusterInfo},
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    APP_NAME, OPERATOR_NAME, ObjectRef,
    command::create_init_container_command_args,
    config::jvm::{construct_non_heap_jvm_args, construct_zk_server_heap_env},
    crd::{
        DOCKER_IMAGE_BASE_NAME, JVM_SECURITY_PROPERTIES_FILE, MAX_PREPARE_LOG_FILE_SIZE,
        MAX_ZK_LOG_FILES_SIZE, METRICS_PORT, METRICS_PORT_NAME, STACKABLE_CONFIG_DIR,
        STACKABLE_DATA_DIR, STACKABLE_LOG_CONFIG_DIR, STACKABLE_LOG_DIR, STACKABLE_RW_CONFIG_DIR,
        ZOOKEEPER_PROPERTIES_FILE, ZookeeperRole,
        security::{self, ZookeeperSecurity},
        v1alpha1::{self, ZookeeperServerRoleConfig},
    },
    discovery::{self, build_discovery_configmap, build_headless_role_group_metrics_service_name},
    listener::build_role_listener,
    operations::{graceful_shutdown::add_graceful_shutdown_config, pdb::add_pdbs},
    product_logging::extend_role_group_config_map,
    utils::build_recommended_labels,
};

pub const ZK_CONTROLLER_NAME: &str = "zookeepercluster";
pub const ZK_FULL_CONTROLLER_NAME: &str = concatcp!(ZK_CONTROLLER_NAME, '.', OPERATOR_NAME);
pub const LISTENER_VOLUME_NAME: &str = "listener";
pub const LISTENER_VOLUME_DIR: &str = "/stackable/listener";

pub const ZK_UID: i64 = 1000;
pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("ZookeeperCluster object is invalid"))]
    InvalidZookeeperCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("crd validation failure"))]
    CrdValidationFailure { source: crate::crd::Error },

    #[snafu(display("object defines no server role"))]
    NoServerRole,

    #[snafu(display("could not parse role [{role}]"))]
    RoleParseFailure {
        source: strum::ParseError,
        role: String,
    },

    #[snafu(display("internal operator failure"))]
    InternalOperatorFailure { source: crate::crd::Error },

    #[snafu(display("failed to calculate global service name"))]
    GlobalServiceNameNotFound,

    #[snafu(display("failed to calculate service name for role {}", rolegroup))]
    RoleGroupServiceNameNotFound {
        rolegroup: RoleGroupRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("failed to apply ConfigMap for {}", rolegroup))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("failed to serialize [{ZOOKEEPER_PROPERTIES_FILE}] for {}", rolegroup))]
    SerializeZooCfg {
        source: PropertiesWriterError,
        rolegroup: RoleGroupRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig { source: discovery::Error },

    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to create RBAC service account"))]
    ApplyServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to create RBAC role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphans {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,

    #[snafu(display("failed to add the logging configuration to the ConfigMap {cm_name}"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },

    #[snafu(display("failed to initialize security context"))]
    FailedToInitializeSecurityContext { source: crate::crd::security::Error },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::crd::Error },

    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::operations::pdb::Error,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::operations::graceful_shutdown::Error,
    },

    #[snafu(display("failed to build label"))]
    BuildLabel { source: LabelError },

    #[snafu(display("failed to build object  meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to add TLS volume mounts"))]
    AddTlsVolumeMounts { source: security::Error },

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging { source: LoggingError },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArguments { source: crate::config::jvm::Error },

    #[snafu(display("failed to apply group listener"))]
    ApplyGroupListener {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to configure listener"))]
    ListenerConfiguration { source: crate::listener::Error },

    // #[snafu(display("failed to configure listener"))]
    // ListenerConfiguration { source: crate::listener::Error },
    #[snafu(display("failed to build listener volume"))]
    BuildListenerPersistentVolume {
        source: stackable_operator::builder::pod::volume::ListenerOperatorVolumeSourceBuilderError,
    },
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::MissingSecretLifetime => None,
            Error::InvalidZookeeperCluster { .. } => None,
            Error::CrdValidationFailure { .. } => None,
            Error::NoServerRole => None,
            Error::RoleParseFailure { .. } => None,
            Error::InternalOperatorFailure { .. } => None,
            Error::GlobalServiceNameNotFound => None,
            Error::RoleGroupServiceNameNotFound { .. } => None,
            Error::ApplyRoleService { .. } => None,
            Error::ApplyRoleGroupService { .. } => None,
            Error::BuildRoleGroupConfig { .. } => None,
            Error::ApplyRoleGroupConfig { .. } => None,
            Error::ApplyRoleGroupStatefulSet { .. } => None,
            Error::GenerateProductConfig { .. } => None,
            Error::InvalidProductConfig { .. } => None,
            Error::SerializeZooCfg { .. } => None,
            Error::ObjectMissingMetadataForOwnerRef { .. } => None,
            Error::BuildDiscoveryConfig { .. } => None,
            Error::ApplyDiscoveryConfig { .. } => None,
            Error::ApplyStatus { .. } => None,
            Error::ApplyServiceAccount { .. } => None,
            Error::ApplyRoleBinding { .. } => None,
            Error::BuildRbacResources { .. } => None,
            Error::DeleteOrphans { .. } => None,
            Error::VectorAggregatorConfigMapMissing { .. } => None,
            Error::InvalidLoggingConfig { .. } => None,
            Error::FailedToInitializeSecurityContext { .. } => None,
            Error::FailedToResolveConfig { .. } => None,
            Error::FailedToCreatePdb { .. } => None,
            Error::GracefulShutdown { .. } => None,
            Error::BuildLabel { .. } => None,
            Error::ObjectMeta { .. } => None,
            Error::AddTlsVolumeMounts { .. } => None,
            Error::ConfigureLogging { .. } => None,
            Error::AddVolume { .. } => None,
            Error::AddVolumeMount { .. } => None,
            Error::CreateClusterResources { .. } => None,
            Error::ConstructJvmArguments { .. } => None,
            Error::ApplyGroupListener { .. } => None,
            Error::BuildListenerPersistentVolume { .. } => None,
            Error::ListenerConfiguration { .. } => None,
        }
    }
}

pub async fn reconcile_zk(
    zk: Arc<DeserializeGuard<v1alpha1::ZookeeperCluster>>,
    ctx: Arc<Ctx>,
) -> Result<controller::Action> {
    tracing::info!("Starting reconcile");
    let zk =
        zk.0.as_ref()
            .map_err(error_boundary::InvalidObject::clone)
            .context(InvalidZookeeperClusterSnafu)?;
    let client = &ctx.client;

    let resolved_product_image = zk
        .spec
        .image
        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION);

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        ZK_CONTROLLER_NAME,
        &zk.object_ref(&()),
        ClusterResourceApplyStrategy::from(&zk.spec.cluster_operation),
    )
    .context(CreateClusterResourcesSnafu)?;

    let validated_config = validate_all_roles_and_groups_config(
        &resolved_product_image.app_version_label,
        &transform_all_roles_to_config(
            zk,
            [(
                ZookeeperRole::Server.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::File(ZOOKEEPER_PROPERTIES_FILE.to_string()),
                        PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
                    ],
                    zk.spec.servers.clone().context(NoServerRoleSnafu)?,
                ),
            )]
            .into(),
        )
        .context(GenerateProductConfigSnafu)?,
        &ctx.product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let role_server_config = validated_config
        .get(&ZookeeperRole::Server.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let zookeeper_security = ZookeeperSecurity::new_from_zookeeper_cluster(client, zk)
        .await
        .context(FailedToInitializeSecurityContextSnafu)?;

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        zk,
        APP_NAME,
        cluster_resources
            .get_required_labels()
            .context(BuildLabelSnafu)?,
    )
    .context(BuildRbacResourcesSnafu)?;

    cluster_resources
        .add(client, rbac_sa.clone())
        .await
        .context(ApplyServiceAccountSnafu)?;

    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    let zk_role = ZookeeperRole::Server;
    for (rolegroup_name, rolegroup_config) in role_server_config.iter() {
        let rolegroup = zk.server_rolegroup_ref(rolegroup_name);
        let merged_config = zk
            .merged_config(&ZookeeperRole::Server, &rolegroup)
            .context(FailedToResolveConfigSnafu)?;

        let rg_service = build_server_rolegroup_service(zk, &rolegroup, &resolved_product_image)?;
        let rg_configmap = build_server_rolegroup_config_map(
            zk,
            &rolegroup,
            rolegroup_config,
            &resolved_product_image,
            &zookeeper_security,
            &client.kubernetes_cluster_info,
        )?;
        let rg_statefulset = build_server_rolegroup_statefulset(
            zk,
            &zk_role,
            &rolegroup,
            rolegroup_config,
            &zookeeper_security,
            &resolved_product_image,
            &merged_config,
            &rbac_sa,
        )?;

        cluster_resources
            .add(client, rg_service)
            .await
            .with_context(|_| ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup.clone(),
            })?;
        cluster_resources
            .add(client, rg_configmap)
            .await
            .with_context(|_| ApplyRoleGroupConfigSnafu {
                rolegroup: rolegroup.clone(),
            })?;

        ss_cond_builder.add(
            cluster_resources
                .add(client, rg_statefulset)
                .await
                .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                    rolegroup: rolegroup.clone(),
                })?,
        );
    }

    let role_config = zk.role_config(&zk_role);
    if let Some(ZookeeperServerRoleConfig {
        common,
        listener_class: _,
    }) = role_config
    {
        add_pdbs(
            &common.pod_disruption_budget,
            zk,
            &zk_role,
            client,
            &mut cluster_resources,
        )
        .await
        .context(FailedToCreatePdbSnafu)?;
    }

    let listener = build_role_listener(zk, &zk_role, &resolved_product_image, &zookeeper_security)
        .context(ListenerConfigurationSnafu)?;
    cluster_resources
        .add(client, listener.clone())
        .await
        .context(ApplyGroupListenerSnafu)?;

    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    let mut discovery_hash = FnvHasher::with_key(0);
    let discovery_cm = build_discovery_configmap(
        zk,
        zk,
        ZK_CONTROLLER_NAME,
        listener,
        None,
        &resolved_product_image,
        &zookeeper_security,
    )
    .context(BuildDiscoveryConfigSnafu)?;

    let discovery_cm = cluster_resources
        .add(client, discovery_cm)
        .await
        .context(ApplyDiscoveryConfigSnafu)?;
    if let Some(generation) = discovery_cm.metadata.resource_version {
        discovery_hash.write(generation.as_bytes())
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&zk.spec.cluster_operation);

    let status = v1alpha1::ZookeeperClusterStatus {
        // Serialize as a string to discourage users from trying to parse the value,
        // and to keep things flexible if we end up changing the hasher at some point.
        discovery_hash: Some(discovery_hash.finish().to_string()),
        conditions: compute_conditions(zk, &[&ss_cond_builder, &cluster_operation_cond_builder]),
    };

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphansSnafu)?;
    client
        .apply_patch_status(OPERATOR_NAME, zk, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(controller::Action::await_change())
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_server_rolegroup_config_map(
    zk: &v1alpha1::ZookeeperCluster,
    rolegroup: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
    server_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    resolved_product_image: &ResolvedProductImage,
    zookeeper_security: &ZookeeperSecurity,
    cluster_info: &KubernetesClusterInfo,
) -> Result<ConfigMap> {
    let mut zoo_cfg: BTreeMap<_, _> = zk
        .pods()
        .into_iter()
        .flatten()
        .map(|pod| {
            (
                format!("server.{}", pod.zookeeper_myid),
                format!(
                    "{}:2888:3888;{}",
                    pod.fqdn(cluster_info),
                    zookeeper_security.client_port()
                ),
            )
        })
        .collect();

    zoo_cfg.extend(zookeeper_security.config_settings());

    let jvm_sec_props: BTreeMap<String, Option<String>> = server_config
        .get(&PropertyNameKind::File(
            JVM_SECURITY_PROPERTIES_FILE.to_string(),
        ))
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect();

    let role =
        ZookeeperRole::from_str(&rolegroup.role).with_context(|_| RoleParseFailureSnafu {
            role: rolegroup.role.to_string(),
        })?;

    // configOverrides need to go last
    zoo_cfg.extend(
        server_config
            .get(&PropertyNameKind::File(
                ZOOKEEPER_PROPERTIES_FILE.to_string(),
            ))
            .cloned()
            .unwrap_or_default(),
    );

    let zk_data: BTreeMap<String, Option<String>> =
        zoo_cfg.into_iter().map(|(k, v)| (k, Some(v))).collect();

    let mut cm_builder = ConfigMapBuilder::new();
    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(zk)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(zk, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(build_recommended_labels(
                    zk,
                    ZK_CONTROLLER_NAME,
                    &resolved_product_image.app_version_label,
                    &rolegroup.role,
                    &rolegroup.role_group,
                ))
                .context(ObjectMetaSnafu)?
                .build(),
        )
        .add_data(
            JVM_SECURITY_PROPERTIES_FILE,
            to_java_properties_string(jvm_sec_props.iter()).with_context(|_| {
                SerializeZooCfgSnafu {
                    rolegroup: rolegroup.clone(),
                }
            })?,
        )
        .add_data(
            ZOOKEEPER_PROPERTIES_FILE,
            to_java_properties_string(zk_data.iter()).with_context(|_| SerializeZooCfgSnafu {
                rolegroup: rolegroup.clone(),
            })?,
        );

    extend_role_group_config_map(zk, role, rolegroup, &mut cm_builder).context(
        InvalidLoggingConfigSnafu {
            cm_name: rolegroup.object_name(),
        },
    )?;

    cm_builder
        .build()
        .with_context(|_| BuildRoleGroupConfigSnafu {
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_server_rolegroup_service(
    zk: &v1alpha1::ZookeeperCluster,
    rolegroup: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Service> {
    let prometheus_label =
        Label::try_from(("prometheus.io/scrape", "true")).context(BuildLabelSnafu)?;

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(zk)
        .name(build_headless_role_group_metrics_service_name(
            rolegroup.object_name(),
        ))
        .ownerreference_from_resource(zk, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(build_recommended_labels(
            zk,
            ZK_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            &rolegroup.role,
            &rolegroup.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .with_label(prometheus_label)
        .build();

    let service_selector_labels =
        Labels::role_group_selector(zk, APP_NAME, &rolegroup.role, &rolegroup.role_group)
            .context(BuildLabelSnafu)?;

    let service_spec = ServiceSpec {
        // Internal communication does not need to be exposed
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        ports: Some(vec![ServicePort {
            name: Some(METRICS_PORT_NAME.to_string()),
            port: METRICS_PORT.into(),
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        }]),
        selector: Some(service_selector_labels.into()),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata,
        spec: Some(service_spec),
        status: None,
    })
}

pub fn build_role_listener_pvc(
    group_listener_name: &str,
    unversioned_recommended_labels: &Labels,
) -> Result<PersistentVolumeClaim, Error> {
    ListenerOperatorVolumeSourceBuilder::new(
        &ListenerReference::ListenerName(group_listener_name.to_string()),
        unversioned_recommended_labels,
    )
    .expect("ListenerOperatorVolumeSourceBuilder::new always returns Ok()")
    .build_pvc(LISTENER_VOLUME_NAME.to_string())
    .context(BuildListenerPersistentVolumeSnafu)
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_server_rolegroup_service`]).
#[allow(clippy::too_many_arguments)]
fn build_server_rolegroup_statefulset(
    zk: &v1alpha1::ZookeeperCluster,
    zk_role: &ZookeeperRole,
    rolegroup_ref: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
    server_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    zookeeper_security: &ZookeeperSecurity,
    resolved_product_image: &ResolvedProductImage,
    merged_config: &v1alpha1::ZookeeperConfig,
    service_account: &ServiceAccount,
) -> Result<StatefulSet> {
    let role = zk.role(zk_role).context(InternalOperatorFailureSnafu)?;
    let rolegroup = zk
        .rolegroup(rolegroup_ref)
        .context(InternalOperatorFailureSnafu)?;

    let logging = zk
        .logging(zk_role, rolegroup_ref)
        .context(CrdValidationFailureSnafu)?;

    let env_vars = server_config
        .get(&PropertyNameKind::Env)
        .into_iter()
        .flatten()
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect::<Vec<_>>();

    let (original_pvcs, resources) = zk
        .resources(zk_role, rolegroup_ref)
        .context(CrdValidationFailureSnafu)?;

    let mut cb_prepare =
        ContainerBuilder::new("prepare").expect("invalid hard-coded container name");
    let mut cb_zookeeper =
        ContainerBuilder::new(APP_NAME).expect("invalid hard-coded container name");
    let mut pod_builder = PodBuilder::new();

    // Used for PVC templates that cannot be modified once they are deployed
    let unversioned_recommended_labels = Labels::recommended(build_recommended_labels(
        zk,
        ZK_CONTROLLER_NAME,
        // A version value is required, but we need to use something constant so that we don't run into immutabile field issues.
        "none",
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    ))
    .expect("todo: LabelBuildSnafu");
    // .context(LabelBuildSnafu)?;

    let listener_pvc = build_role_listener_pvc(
        &zk.server_role_listener_name()
            .expect("todo: get role from zk_role"),
        &unversioned_recommended_labels,
    )?;

    let mut pvcs = original_pvcs;
    pvcs.extend([listener_pvc]);

    cb_zookeeper
        .add_volume_mount(LISTENER_VOLUME_NAME, LISTENER_VOLUME_DIR)
        .context(AddVolumeMountSnafu)?;

    let requested_secret_lifetime = merged_config
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    // add volumes and mounts depending on tls / auth settings
    zookeeper_security
        .add_volume_mounts(
            &mut pod_builder,
            &mut cb_zookeeper,
            &requested_secret_lifetime,
        )
        .context(AddTlsVolumeMountsSnafu)?;

    let mut args = Vec::new();

    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&v1alpha1::Container::Prepare)
    {
        args.push(product_logging::framework::capture_shell_output(
            STACKABLE_LOG_DIR,
            "prepare",
            log_config,
        ));
    }
    args.extend(create_init_container_command_args());

    let container_prepare = cb_prepare
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![args.join("\n")])
        .add_env_vars(env_vars.clone())
        .add_env_vars(vec![EnvVar {
            name: "POD_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_string()),
                    field_path: "metadata.name".to_string(),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        }])
        .add_volume_mount("data", STACKABLE_DATA_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("config", STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("rwconfig", STACKABLE_RW_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("200m")
                .with_cpu_limit("800m")
                .with_memory_request("512Mi")
                .with_memory_limit("512Mi")
                .build(),
        )
        .build();

    let container_zk = cb_zookeeper
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![formatdoc! {"
            {COMMON_BASH_TRAP_FUNCTIONS}
            {remove_vector_shutdown_file_command}
            prepare_signal_handlers
            containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &
            bin/zkServer.sh start-foreground {STACKABLE_RW_CONFIG_DIR}/zoo.cfg &
            wait_for_termination $!
            {create_vector_shutdown_file_command}
            ",
            remove_vector_shutdown_file_command =
                remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
            create_vector_shutdown_file_command =
                create_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        }])
        .add_env_vars(env_vars)
        .add_env_var(
            "ZK_SERVER_HEAP",
            construct_zk_server_heap_env(merged_config).context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            "SERVER_JVMFLAGS",
            construct_non_heap_jvm_args(zk, role, &rolegroup_ref.role_group)
                .context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            "CONTAINERDEBUG_LOG_DIRECTORY",
            format!("{STACKABLE_LOG_DIR}/containerdebug"),
        )
        // Only allow the global load balancing service to send traffic to pods that are members of the quorum
        // This also acts as a hint to the StatefulSet controller to wait for each pod to enter quorum before taking down the next
        .readiness_probe(Probe {
            exec: Some(ExecAction {
                command: Some(vec![
                    "bash".to_string(),
                    "-c".to_string(),
                    // We don't have telnet or netcat in the container images, but
                    // we can use Bash's virtual /dev/tcp filesystem to accomplish the same thing
                    format!(
                        "exec 3<>/dev/tcp/127.0.0.1/{} && echo srvr >&3 && grep '^Mode: ' <&3",
                        zookeeper_security.client_port()
                    ),
                ]),
            }),
            period_seconds: Some(1),
            ..Probe::default()
        })
        .add_container_port("zk", zookeeper_security.client_port().into())
        .add_container_port("zk-leader", 2888)
        .add_container_port("zk-election", 3888)
        .add_container_port("metrics", 9505)
        .add_volume_mount("data", STACKABLE_DATA_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("config", STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log-config", STACKABLE_LOG_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("rwconfig", STACKABLE_RW_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
        .resources(resources)
        .build();

    let pb_metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(build_recommended_labels(
            zk,
            ZK_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .build();

    pod_builder
        .metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_init_container(container_prepare)
        .add_container(container_zk)
        .affinity(&merged_config.affinity)
        .add_volume(Volume {
            name: "config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: rolegroup_ref.object_name(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        .add_volume(Volume {
            empty_dir: Some(EmptyDirVolumeSource {
                medium: None,
                size_limit: None,
            }),
            name: "rwconfig".to_string(),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        .add_empty_dir_volume(
            "log",
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_ZK_LOG_FILES_SIZE, MAX_PREPARE_LOG_FILE_SIZE],
            )),
        )
        .context(AddVolumeSnafu)?
        .security_context(PodSecurityContext {
            run_as_user: Some(ZK_UID),
            run_as_group: Some(0),
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        })
        .service_account_name(service_account.name_any());

    if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = logging.containers.get(&v1alpha1::Container::Zookeeper)
    {
        pod_builder
            .add_volume(Volume {
                name: "log-config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: config_map.into(),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            })
            .context(AddVolumeSnafu)?;
    } else {
        pod_builder
            .add_volume(Volume {
                name: "log-config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: rolegroup_ref.object_name(),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            })
            .context(AddVolumeSnafu)?;
    }

    if logging.enable_vector_agent {
        match &zk.spec.cluster_config.vector_aggregator_config_map_name {
            Some(vector_aggregator_config_map_name) => {
                pod_builder.add_container(
                    product_logging::framework::vector_container(
                        resolved_product_image,
                        "config",
                        "log",
                        logging.containers.get(&v1alpha1::Container::Vector),
                        ResourceRequirementsBuilder::new()
                            .with_cpu_request("250m")
                            .with_cpu_limit("500m")
                            .with_memory_request("128Mi")
                            .with_memory_limit("128Mi")
                            .build(),
                        vector_aggregator_config_map_name,
                    )
                    .context(ConfigureLoggingSnafu)?,
                );
            }
            None => {
                VectorAggregatorConfigMapMissingSnafu.fail()?;
            }
        }
    }

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;

    let mut pod_template = pod_builder.build_template();
    pod_template.merge_from(role.config.pod_overrides.clone());
    pod_template.merge_from(rolegroup.config.pod_overrides.clone());

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(zk)
        .name(rolegroup_ref.object_name())
        .ownerreference_from_resource(zk, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(build_recommended_labels(
            zk,
            ZK_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        // The initial restart muddles up the integration tests. This can be re-enabled as soon
        // as https://github.com/stackabletech/commons-operator/issues/111 is implemented.
        // .with_label("restarter.stackable.tech/enabled", "true")
        .context(ObjectMetaSnafu)?
        .build();

    let statefulset_match_labels =
        Labels::role_group_selector(zk, APP_NAME, &rolegroup_ref.role, &rolegroup_ref.role_group)
            .context(BuildLabelSnafu)?;

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("Parallel".to_string()),
        replicas: rolegroup.replicas.map(i32::from),
        selector: LabelSelector {
            match_labels: Some(statefulset_match_labels.into()),
            ..LabelSelector::default()
        },
        service_name: Some(build_headless_role_group_metrics_service_name(
            rolegroup_ref.object_name(),
        )),
        template: pod_template,
        volume_claim_templates: Some(pvcs),
        ..StatefulSetSpec::default()
    };

    Ok(StatefulSet {
        metadata,
        spec: Some(statefulset_spec),
        status: None,
    })
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::ZookeeperCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> controller::Action {
    match error {
        // root object is invalid, will be requeued when modified anyway
        Error::InvalidZookeeperCluster { .. } => controller::Action::await_change(),

        _ => controller::Action::requeue(*Duration::from_secs(5)),
    }
}

#[cfg(test)]
mod tests {
    use stackable_operator::commons::networking::DomainName;

    use super::*;

    #[test]
    fn test_default_config() {
        let zookeeper_yaml = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.3"
          servers:
            roleGroups:
              default:
                replicas: 3
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();
        let config = cm.get("zoo.cfg").unwrap();
        assert!(config.contains(
            "authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider"
        ));
        assert!(config.contains("ssl.hostnameVerification=true"));
        // Default value
        assert!(config.contains("ssl.quorum.hostnameVerification=true"));

        assert!(cm.contains_key("security.properties"));
    }

    #[test]
    fn test_config_overrides() {
        let zookeeper_yaml = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.3"
          servers:
            configOverrides:
              zoo.cfg:
                foo: bar
                level: role
                hello-from-role: "true"
            roleGroups:
              default:
                configOverrides:
                  zoo.cfg:
                    foo: bar
                    level: role-group
                    ssl.quorum.hostnameVerification: "false"
                    hello-from-role-group: "true"
                replicas: 3
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();
        let config = cm.get("zoo.cfg").unwrap();
        assert!(config.contains("foo=bar"));
        assert!(config.contains("level=role-group"));
        assert!(config.contains("hello-from-role=true"));
        assert!(config.contains("hello-from-role-group=true"));
        assert!(config.contains(
            "authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider"
        ));
        assert!(config.contains("ssl.hostnameVerification=true"));
        // Overwritten by configOverride
        assert!(config.contains("ssl.quorum.hostnameVerification=false"));

        assert!(cm.contains_key("security.properties"));
    }

    fn build_config_map(zookeeper_yaml: &str) -> ConfigMap {
        let mut zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(zookeeper_yaml).expect("illegal test input");
        zookeeper.metadata.uid = Some("42".to_owned());
        let cluster_info = KubernetesClusterInfo {
            cluster_domain: DomainName::try_from("cluster.local").unwrap(),
        };
        let resolved_product_image = zookeeper
            .spec
            .image
            .resolve(DOCKER_IMAGE_BASE_NAME, "0.0.0-dev");

        let validated_config = validate_all_roles_and_groups_config(
            &resolved_product_image.app_version_label,
            &transform_all_roles_to_config(
                &zookeeper,
                [(
                    ZookeeperRole::Server.to_string(),
                    (
                        vec![
                            PropertyNameKind::Env,
                            PropertyNameKind::File(ZOOKEEPER_PROPERTIES_FILE.to_string()),
                            PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
                        ],
                        zookeeper.spec.servers.clone().unwrap(),
                    ),
                )]
                .into(),
            )
            .unwrap(),
            // Using this instead of ProductConfigManager::from_yaml_file, as that did not find the file
            &ProductConfigManager::from_str(include_str!(
                "../../../deploy/config-spec/properties.yaml"
            ))
            .unwrap(),
            false,
            false,
        )
        .unwrap();

        let rolegroup_ref = RoleGroupRef {
            cluster: ObjectRef::from_obj(&zookeeper),
            role: ZookeeperRole::Server.to_string(),
            role_group: "default".to_string(),
        };

        let zookeeper_security = ZookeeperSecurity::new_for_tests();

        build_server_rolegroup_config_map(
            &zookeeper,
            &rolegroup_ref,
            validated_config
                .get("server")
                .unwrap()
                .get("default")
                .unwrap(),
            &resolved_product_image,
            &zookeeper_security,
            &cluster_info,
        )
        .unwrap()
    }
}
