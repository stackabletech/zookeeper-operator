//! Ensures that `Pod`s are configured and running for each [`v1alpha1::ZookeeperCluster`]
use std::{collections::BTreeMap, hash::Hasher, sync::Arc};

use const_format::concatcp;
use fnv::FnvHasher;
use indoc::formatdoc;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            volume::{ListenerOperatorVolumeSourceBuilder, ListenerReference},
        },
    },
    cli::OperatorEnvironmentOptions,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{product_image_selection::ResolvedProductImage, rbac::build_rbac_resources},
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMapVolumeSource, EmptyDirVolumeSource, EnvVar, EnvVarSource, ExecAction,
                ObjectFieldSelector, PersistentVolumeClaim, PodSecurityContext, Probe,
                ServiceAccount, Volume,
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
    kvp::{LabelError, Labels},
    logging::controller::ReconcilerError,
    memory::{BinaryMultiple, MemoryQuantity},
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
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    utils::COMMON_BASH_TRAP_FUNCTIONS,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    APP_NAME, OPERATOR_NAME, ObjectRef,
    command::create_init_container_command_args,
    config::jvm::{construct_non_heap_jvm_args, construct_zk_server_heap_env},
    crd::{
        JMX_METRICS_PORT_NAME, METRICS_PROVIDER_HTTP_PORT_NAME, STACKABLE_CONFIG_DIR,
        STACKABLE_DATA_DIR, STACKABLE_LOG_CONFIG_DIR, STACKABLE_LOG_DIR, STACKABLE_RW_CONFIG_DIR,
        ZOOKEEPER_ELECTION_PORT, ZOOKEEPER_ELECTION_PORT_NAME, ZOOKEEPER_LEADER_PORT,
        ZOOKEEPER_LEADER_PORT_NAME, ZOOKEEPER_SERVER_PORT_NAME, ZookeeperRole,
        security::{self, ZookeeperSecurity},
        v1alpha1::{self, ZookeeperServerRoleConfig},
    },
    listener::{build_role_listener, role_listener_name},
    operations::{graceful_shutdown::add_graceful_shutdown_config, pdb::add_pdbs},
    service::{
        self, build_server_rolegroup_headless_service, build_server_rolegroup_metrics_service,
    },
    utils::build_recommended_labels,
};

pub(crate) mod build;
mod dereference;
mod validate;

pub const ZK_CONTROLLER_NAME: &str = "zookeepercluster";
pub const ZK_FULL_CONTROLLER_NAME: &str = concatcp!(ZK_CONTROLLER_NAME, '.', OPERATOR_NAME);
pub const LISTENER_VOLUME_NAME: &str = "listener";
pub const LISTENER_VOLUME_DIR: &str = "/stackable/listener";

/// Maximum size of the `prepare` init container log file (before rotation).
pub const MAX_PREPARE_LOG_FILE_SIZE: MemoryQuantity = MemoryQuantity {
    value: 1.0,
    unit: BinaryMultiple::Mebi,
};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub operator_environment: OperatorEnvironmentOptions,
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

    #[snafu(display("failed to dereference resources"))]
    Dereference { source: dereference::Error },

    #[snafu(display("failed to validate cluster"))]
    ValidateCluster { source: validate::Error },

    #[snafu(display("crd validation failure"))]
    CrdValidationFailure { source: crate::crd::Error },

    #[snafu(display("internal operator failure"))]
    InternalOperatorFailure { source: crate::crd::Error },

    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfigMap {
        source: build::config_map::Error,
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

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig {
        source: build::discovery::Error,
    },

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

    #[snafu(display("failed to build object meta data"))]
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

    #[snafu(display("failed to build service"))]
    BuildService { source: service::Error },
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::MissingSecretLifetime => None,
            Error::InvalidZookeeperCluster { .. } => None,
            Error::Dereference { .. } => None,
            Error::ValidateCluster { .. } => None,
            Error::CrdValidationFailure { .. } => None,
            Error::InternalOperatorFailure { .. } => None,
            Error::ApplyRoleGroupService { .. } => None,
            Error::BuildRoleGroupConfigMap { .. } => None,
            Error::ApplyRoleGroupConfig { .. } => None,
            Error::ApplyRoleGroupStatefulSet { .. } => None,
            Error::ObjectMissingMetadataForOwnerRef { .. } => None,
            Error::BuildDiscoveryConfig { .. } => None,
            Error::ApplyDiscoveryConfig { .. } => None,
            Error::ApplyStatus { .. } => None,
            Error::ApplyServiceAccount { .. } => None,
            Error::ApplyRoleBinding { .. } => None,
            Error::BuildRbacResources { .. } => None,
            Error::DeleteOrphans { .. } => None,
            Error::VectorAggregatorConfigMapMissing => None,
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
            Error::BuildService { .. } => None,
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

    // dereference (client required)
    let dereferenced_objects = dereference::dereference(client, zk)
        .await
        .context(DereferenceSnafu)?;

    // validate (no client required)
    let validated_cluster = validate::validate(
        zk,
        &dereferenced_objects,
        &ctx.operator_environment,
        &client.kubernetes_cluster_info,
    )
    .context(ValidateClusterSnafu)?;
    let resolved_product_image = &validated_cluster.image;
    let zookeeper_security = &validated_cluster.cluster_config.zookeeper_security;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        ZK_CONTROLLER_NAME,
        &zk.object_ref(&()),
        ClusterResourceApplyStrategy::from(&zk.spec.cluster_operation),
        &zk.spec.object_overrides,
    )
    .context(CreateClusterResourcesSnafu)?;

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
    let server_role_group_configs = validated_cluster
        .role_group_configs
        .get(&zk_role)
        .into_iter()
        .flatten();
    for (rolegroup_name, rolegroup_config) in server_role_group_configs {
        let rolegroup = zk.server_rolegroup_ref(rolegroup_name);
        let merged_config = &rolegroup_config.config;
        let metrics_port =
            build::properties::zoo_cfg::metrics_http_port(&validated_cluster, rolegroup_config);

        let rg_headless_service =
            build_server_rolegroup_headless_service(zk, &rolegroup, resolved_product_image)
                .context(BuildServiceSnafu)?;
        let rg_metrics_service = build_server_rolegroup_metrics_service(
            zk,
            &rolegroup,
            resolved_product_image,
            metrics_port,
        )
        .context(BuildServiceSnafu)?;
        let rg_configmap = build::config_map::build_server_rolegroup_config_map(
            &validated_cluster,
            &zk_role,
            &rolegroup,
            rolegroup_config,
            zk,
        )
        .context(BuildRoleGroupConfigMapSnafu {
            rolegroup: rolegroup.clone(),
        })?;
        let rg_statefulset = build_server_rolegroup_statefulset(
            zk,
            &zk_role,
            &rolegroup,
            &rolegroup_config.env_overrides,
            zookeeper_security,
            resolved_product_image,
            merged_config,
            metrics_port,
            &rbac_sa,
        )?;

        cluster_resources
            .add(client, rg_headless_service)
            .await
            .with_context(|_| ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup.clone(),
            })?;
        cluster_resources
            .add(client, rg_metrics_service)
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

        // Note: The StatefulSet needs to be applied after all ConfigMaps and Secrets it mounts
        // to prevent unnecessary Pod restarts.
        // See https://github.com/stackabletech/commons-operator/issues/111 for details.
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
    if let Some(ZookeeperServerRoleConfig { common, .. }) = role_config {
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

    let listener = build_role_listener(zk, &zk_role, resolved_product_image, zookeeper_security)
        .context(ListenerConfigurationSnafu)?;
    let applied_listener = cluster_resources
        .add(client, listener)
        .await
        .context(ApplyGroupListenerSnafu)?;

    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    let mut discovery_hash = FnvHasher::with_key(0);
    let discovery_cm = build::discovery::build_discovery_configmap(
        zk,
        ZK_CONTROLLER_NAME,
        applied_listener,
        None,
        resolved_product_image,
        zookeeper_security,
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

pub fn build_role_listener_pvc(
    group_listener_name: &str,
    unversioned_recommended_labels: &Labels,
) -> Result<PersistentVolumeClaim, Error> {
    ListenerOperatorVolumeSourceBuilder::new(
        &ListenerReference::ListenerName(group_listener_name.to_string()),
        unversioned_recommended_labels,
    )
    .build_pvc(LISTENER_VOLUME_NAME.to_string())
    .context(BuildListenerPersistentVolumeSnafu)
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding headless [`stackable_operator::k8s_openapi::api::core::v1::Service`] (from [`build_server_rolegroup_headless_service`]).
#[allow(clippy::too_many_arguments)]
fn build_server_rolegroup_statefulset(
    zk: &v1alpha1::ZookeeperCluster,
    zk_role: &ZookeeperRole,
    rolegroup_ref: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
    env_overrides: &BTreeMap<String, String>,
    zookeeper_security: &ZookeeperSecurity,
    resolved_product_image: &ResolvedProductImage,
    merged_config: &v1alpha1::ZookeeperConfig,
    metrics_port: u16,
    service_account: &ServiceAccount,
) -> Result<StatefulSet> {
    let role = zk.role(zk_role).context(InternalOperatorFailureSnafu)?;
    let rolegroup = zk
        .rolegroup(rolegroup_ref)
        .context(InternalOperatorFailureSnafu)?;

    let logging = zk
        .logging(zk_role, rolegroup_ref)
        .context(CrdValidationFailureSnafu)?;

    // The operator-injected environment variables (formerly produced by the
    // product-config `Configuration::compute_env` implementation) plus the
    // user-provided `envOverrides` (which win on conflict).
    let mut env_map: BTreeMap<String, String> = BTreeMap::new();
    env_map.insert(
        v1alpha1::ZookeeperConfig::MYID_OFFSET.to_string(),
        merged_config.myid_offset.to_string(),
    );
    // Used by zkEnv.sh and the shell scripts in bin/. If unset it tries to find the
    // conf directory automatically and that fails.
    env_map.insert("ZOOCFGDIR".to_string(), STACKABLE_RW_CONFIG_DIR.to_string());
    env_map.extend(env_overrides.clone());
    let env_vars = env_map
        .into_iter()
        .map(|(name, value)| EnvVar {
            name,
            value: Some(value),
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
    let unversioned_recommended_labels = Labels::recommended(&build_recommended_labels(
        zk,
        ZK_CONTROLLER_NAME,
        // A version value is required, but we need to use something constant so that we don't run into immutabile field issues.
        "none",
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    ))
    .context(BuildLabelSnafu)?;

    let listener_pvc = build_role_listener_pvc(
        &role_listener_name(zk, &ZookeeperRole::Server),
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
            construct_non_heap_jvm_args(
                zk,
                role,
                &rolegroup_ref.role_group,
                &resolved_product_image.product_version,
            )
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
        .add_container_port(
            ZOOKEEPER_SERVER_PORT_NAME,
            zookeeper_security.client_port() as i32,
        )
        .add_container_port(ZOOKEEPER_LEADER_PORT_NAME, ZOOKEEPER_LEADER_PORT as i32)
        .add_container_port(ZOOKEEPER_ELECTION_PORT_NAME, ZOOKEEPER_ELECTION_PORT as i32)
        .add_container_port(JMX_METRICS_PORT_NAME, 9505)
        .add_container_port(METRICS_PROVIDER_HTTP_PORT_NAME, metrics_port.into())
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
        .with_recommended_labels(&build_recommended_labels(
            zk,
            ZK_CONTROLLER_NAME,
            &resolved_product_image.app_version_label_value,
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
                &[
                    build::properties::logging::MAX_ZK_LOG_FILES_SIZE,
                    MAX_PREPARE_LOG_FILE_SIZE,
                ],
            )),
        )
        .context(AddVolumeSnafu)?
        .security_context(PodSecurityContext {
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
        .with_recommended_labels(&build_recommended_labels(
            zk,
            ZK_CONTROLLER_NAME,
            &resolved_product_image.app_version_label_value,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
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
        service_name: Some(rolegroup_ref.rolegroup_headless_service_name()),
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
    use stackable_operator::{
        commons::networking::DomainName, k8s_openapi::api::core::v1::ConfigMap,
        role_utils::JavaCommonConfig, utils::cluster_info::KubernetesClusterInfo,
    };

    use super::*;
    use crate::{
        crd::CONTAINER_IMAGE_BASE_NAME,
        framework::role_utils::with_validated_config,
        zk_controller::validate::{ValidatedCluster, ValidatedClusterConfig},
    };

    #[test]
    fn test_default_config() {
        let zookeeper_yaml = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.5"
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
            productVersion: "3.9.5"
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

    #[test]
    fn test_seeded_operator_defaults() {
        // These values used to be injected by product-config from
        // `deploy/config-spec/properties.yaml`. They are now seeded directly by the
        // ConfigMap builder and must stay byte-identical (pinned by the kuttl
        // snapshot `tests/templates/kuttl/smoke/14-assert.yaml.j2`).
        let zookeeper_yaml = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.5"
          servers:
            roleGroups:
              default:
                replicas: 3
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();

        // `security.properties` is fully operator-injected; assert it byte-for-byte.
        assert_eq!(
            cm.get("security.properties").unwrap(),
            "networkaddress.cache.negative.ttl=0\nnetworkaddress.cache.ttl=5\n"
        );

        let zoo_cfg = cm.get("zoo.cfg").unwrap();
        for expected in [
            "admin.serverPort=8080",
            // new_for_tests() enables server TLS, so the secure client port is used.
            "clientPort=2282",
            "dataDir=/stackable/data",
            "initLimit=5",
            "syncLimit=2",
            "tickTime=3000",
            "metricsProvider.className=org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider",
            "metricsProvider.httpPort=7000",
        ] {
            assert!(
                zoo_cfg.contains(expected),
                "missing {expected:?} in:\n{zoo_cfg}"
            );
        }
    }

    #[test]
    fn test_user_config_overrides_seeded_default() {
        // A value set on the typed config must win over the seeded default.
        let zookeeper_yaml = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.5"
          servers:
            roleGroups:
              default:
                replicas: 3
                config:
                  tickTime: 4000
                  initLimit: 7
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();
        let zoo_cfg = cm.get("zoo.cfg").unwrap();
        assert!(zoo_cfg.contains("tickTime=4000"), "{zoo_cfg}");
        assert!(zoo_cfg.contains("initLimit=7"), "{zoo_cfg}");
        // Untouched default stays.
        assert!(zoo_cfg.contains("syncLimit=2"), "{zoo_cfg}");
    }

    fn build_config_map(zookeeper_yaml: &str) -> ConfigMap {
        let mut zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(zookeeper_yaml).expect("illegal test input");
        zookeeper.metadata.uid = Some("42".to_owned());
        zookeeper.metadata.namespace = Some("default".to_owned());
        let cluster_info = KubernetesClusterInfo {
            cluster_domain: DomainName::try_from("cluster.local").unwrap(),
        };
        let image = zookeeper
            .spec
            .image
            .resolve(CONTAINER_IMAGE_BASE_NAME, "oci.example.org", "0.0.0-dev")
            .expect("test resolved product image is always valid");
        let zookeeper_security = ZookeeperSecurity::new_for_tests();

        let zk_role = ZookeeperRole::Server;
        let role = zookeeper.role(&zk_role).unwrap();
        let default_config =
            v1alpha1::ZookeeperConfig::default_server_config(&zookeeper.name_any(), &zk_role);
        let mut groups = BTreeMap::new();
        for (rg_name, rg) in &role.role_groups {
            let validated_rg = with_validated_config::<
                v1alpha1::ZookeeperConfig,
                JavaCommonConfig,
                v1alpha1::ZookeeperConfigFragment,
                _,
                v1alpha1::ZookeeperConfigOverrides,
            >(rg, role, &default_config)
            .unwrap();
            groups.insert(rg_name.clone(), validated_rg);
        }
        let mut role_group_configs = BTreeMap::new();
        role_group_configs.insert(zk_role.clone(), groups);

        let server_addresses = zookeeper
            .pods()
            .unwrap()
            .map(|pod| {
                (
                    format!("server.{id}", id = pod.zookeeper_myid),
                    format!(
                        "{fqdn}:{ZOOKEEPER_LEADER_PORT}:{ZOOKEEPER_ELECTION_PORT};{client_port}",
                        fqdn = pod.internal_fqdn(&cluster_info),
                        client_port = zookeeper_security.client_port()
                    ),
                )
            })
            .collect();

        let validated_cluster = ValidatedCluster {
            name: zookeeper.name_any(),
            image,
            cluster_config: ValidatedClusterConfig {
                zookeeper_security,
                server_addresses,
            },
            role_group_configs,
        };

        let rolegroup_ref = zookeeper.server_rolegroup_ref("default");
        let rolegroup_config = &validated_cluster.role_group_configs[&zk_role]["default"];

        build::config_map::build_server_rolegroup_config_map(
            &validated_cluster,
            &zk_role,
            &rolegroup_ref,
            rolegroup_config,
            &zookeeper,
        )
        .unwrap()
    }
}
