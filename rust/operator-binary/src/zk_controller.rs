//! Ensures that `Pod`s are configured and running for each [`ZookeeperCluster`]
use crate::{
    command::create_init_container_command_args,
    discovery::{self, build_discovery_configmaps},
    product_logging::{extend_role_group_config_map, resolve_vector_aggregator_address},
    utils::build_recommended_labels,
    ObjectRef, APP_NAME, OPERATOR_NAME,
};

use fnv::FnvHasher;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder,
        SecretOperatorVolumeSourceBuilder, VolumeBuilder,
    },
    cluster_resources::ClusterResources,
    commons::{
        authentication::{AuthenticationClass, AuthenticationClassProvider},
        product_image_selection::ResolvedProductImage,
        tls::TlsAuthenticationProvider,
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, EmptyDirVolumeSource, EnvVar, EnvVarSource,
                ExecAction, ObjectFieldSelector, PodSecurityContext, Probe, Service,
                ServiceAccount, ServicePort, ServiceSpec, Volume,
            },
            rbac::v1::{RoleBinding, RoleRef, Subject},
        },
        apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
    },
    kube::{api::DynamicObject, runtime::controller, Resource, ResourceExt},
    labels::{role_group_selector_labels, role_selector_labels},
    logging::controller::ReconcilerError,
    product_config::{
        types::PropertyNameKind, writer::to_java_properties_string, ProductConfigManager,
    },
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{
        self,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::RoleGroupRef,
};
use stackable_zookeeper_crd::{
    Container, ZookeeperCluster, ZookeeperClusterStatus, ZookeeperConfig, ZookeeperRole,
    CLIENT_TLS_DIR, CLIENT_TLS_MOUNT_DIR, DOCKER_IMAGE_BASE_NAME, LOG_VOLUME_SIZE_IN_MIB,
    QUORUM_TLS_DIR, QUORUM_TLS_MOUNT_DIR, STACKABLE_CONFIG_DIR, STACKABLE_DATA_DIR,
    STACKABLE_LOG_CONFIG_DIR, STACKABLE_LOG_DIR, STACKABLE_RW_CONFIG_DIR,
    ZOOKEEPER_PROPERTIES_FILE,
};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    hash::Hasher,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

pub const ZK_CONTROLLER_NAME: &str = "zookeepercluster";
const SERVICE_ACCOUNT: &str = "zookeeper-serviceaccount";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("crd validation failure"))]
    CrdValidationFailure {
        source: stackable_zookeeper_crd::Error,
    },
    #[snafu(display("object defines no server role"))]
    NoServerRole,
    #[snafu(display("could not parse role [{role}]"))]
    RoleParseFailure {
        source: strum::ParseError,
        role: String,
    },
    #[snafu(display("failed to calculate global service name"))]
    GlobalServiceNameNotFound,
    #[snafu(display("failed to calculate service name for role {}", rolegroup))]
    RoleGroupServiceNameNotFound {
        rolegroup: RoleGroupRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to apply ConfigMap for {}", rolegroup))]
    ApplyRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to serialize [{ZOOKEEPER_PROPERTIES_FILE}] for {}", rolegroup))]
    SerializeZooCfg {
        source: stackable_operator::product_config::writer::PropertiesWriterError,
        rolegroup: RoleGroupRef<ZookeeperCluster>,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig { source: discovery::Error },
    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to retrieve {}", authentication_class))]
    AuthenticationClassRetrieval {
        source: stackable_operator::error::Error,
        authentication_class: ObjectRef<AuthenticationClass>,
    },
    #[snafu(display(
        "failed to use authentication mechanism {} - supported methods: {:?}",
        method,
        supported
    ))]
    AuthenticationMethodNotSupported {
        authentication_class: ObjectRef<AuthenticationClass>,
        supported: Vec<String>,
        method: String,
    },
    #[snafu(display("invalid java heap config"))]
    InvalidJavaHeapConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to create RBAC service account"))]
    ApplyServiceAccount {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to create RBAC role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphans {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress {
        source: crate::product_logging::Error,
    },
    #[snafu(display("failed to add the logging configuration to the ConfigMap {cm_name}"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::CrdValidationFailure { .. } => None,
            Error::NoServerRole => None,
            Error::RoleParseFailure { .. } => None,
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
            Error::AuthenticationClassRetrieval {
                authentication_class,
                ..
            } => Some(authentication_class.clone().erase()),
            Error::AuthenticationMethodNotSupported {
                authentication_class,
                ..
            } => Some(authentication_class.clone().erase()),
            Error::InvalidJavaHeapConfig { .. } => None,
            Error::ApplyServiceAccount { .. } => None,
            Error::ApplyRoleBinding { .. } => None,
            Error::DeleteOrphans { .. } => None,
            Error::ResolveVectorAggregatorAddress { .. } => None,
            Error::InvalidLoggingConfig { .. } => None,
        }
    }
}

pub async fn reconcile_zk(zk: Arc<ZookeeperCluster>, ctx: Arc<Ctx>) -> Result<controller::Action> {
    tracing::info!("Starting reconcile");
    let client = &ctx.client;

    let resolved_product_image = zk.spec.image.resolve(DOCKER_IMAGE_BASE_NAME);

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        ZK_CONTROLLER_NAME,
        &zk.object_ref(&()),
    )
    .unwrap();

    let validated_config = validate_all_roles_and_groups_config(
        &resolved_product_image.app_version_label,
        &transform_all_roles_to_config(
            zk.as_ref(),
            [(
                ZookeeperRole::Server.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::File(ZOOKEEPER_PROPERTIES_FILE.to_string()),
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

    let vector_aggregator_address = resolve_vector_aggregator_address(&zk, client)
        .await
        .context(ResolveVectorAggregatorAddressSnafu)?;

    let client_authentication_class = if let Some(auth_class) = zk.client_tls_authentication_class()
    {
        Some(
            AuthenticationClass::resolve(client, auth_class)
                .await
                .context(AuthenticationClassRetrievalSnafu {
                    authentication_class: ObjectRef::<AuthenticationClass>::new(auth_class),
                })?,
        )
    } else {
        None
    };

    let (rbac_sa, rbac_rolebinding) = build_zk_rbac_resources(&zk, &resolved_product_image)?;
    cluster_resources
        .add(client, &rbac_sa)
        .await
        .with_context(|_| ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, &rbac_rolebinding)
        .await
        .with_context(|_| ApplyRoleBindingSnafu)?;

    let server_role_service = cluster_resources
        .add(
            client,
            &build_server_role_service(&zk, &resolved_product_image)?,
        )
        .await
        .context(ApplyRoleServiceSnafu)?;

    for (rolegroup_name, rolegroup_config) in role_server_config.iter() {
        let rolegroup = zk.server_rolegroup_ref(rolegroup_name);

        let rg_service = build_server_rolegroup_service(&zk, &rolegroup, &resolved_product_image)?;
        let rg_configmap = build_server_rolegroup_config_map(
            &zk,
            &rolegroup,
            rolegroup_config,
            &resolved_product_image,
            vector_aggregator_address.as_deref(),
        )?;
        let rg_statefulset = build_server_rolegroup_statefulset(
            &zk,
            &rolegroup,
            rolegroup_config,
            client_authentication_class.as_ref(),
            &resolved_product_image,
        )?;
        cluster_resources
            .add(client, &rg_service)
            .await
            .with_context(|_| ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup.clone(),
            })?;
        cluster_resources
            .add(client, &rg_configmap)
            .await
            .with_context(|_| ApplyRoleGroupConfigSnafu {
                rolegroup: rolegroup.clone(),
            })?;
        cluster_resources
            .add(client, &rg_statefulset)
            .await
            .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                rolegroup: rolegroup.clone(),
            })?;
    }

    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    let mut discovery_hash = FnvHasher::with_key(0);
    for discovery_cm in build_discovery_configmaps(
        &zk,
        &*zk,
        client,
        ZK_CONTROLLER_NAME,
        &server_role_service,
        None,
        &resolved_product_image,
    )
    .await
    .context(BuildDiscoveryConfigSnafu)?
    {
        let discovery_cm = cluster_resources
            .add(client, &discovery_cm)
            .await
            .context(ApplyDiscoveryConfigSnafu)?;
        if let Some(generation) = discovery_cm.metadata.resource_version {
            discovery_hash.write(generation.as_bytes())
        }
    }

    let status = ZookeeperClusterStatus {
        // Serialize as a string to discourage users from trying to parse the value,
        // and to keep things flexible if we end up changing the hasher at some point.
        discovery_hash: Some(discovery_hash.finish().to_string()),
    };
    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphansSnafu)?;
    client
        .apply_patch_status(OPERATOR_NAME, &*zk, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(controller::Action::await_change())
}

pub fn build_zk_rbac_resources(
    zk: &ZookeeperCluster,
    resolved_product_image: &ResolvedProductImage,
) -> Result<(ServiceAccount, RoleBinding)> {
    let service_account = ServiceAccount {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(zk)
            .name(SERVICE_ACCOUNT.to_string())
            .with_recommended_labels(build_recommended_labels(
                zk,
                ZK_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                "global",
                "global",
            ))
            .build(),
        ..ServiceAccount::default()
    };

    let role_binding = RoleBinding {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(zk)
            .name("zookeeper-rolebinding".to_string())
            .with_recommended_labels(build_recommended_labels(
                zk,
                ZK_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                "global",
                "global",
            ))
            .build(),
        role_ref: RoleRef {
            kind: "ClusterRole".to_string(),
            name: "zookeeper-clusterrole".to_string(),
            api_group: "rbac.authorization.k8s.io".to_string(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_string(),
            name: SERVICE_ACCOUNT.to_string(),
            namespace: zk.namespace(),
            ..Subject::default()
        }]),
    };

    Ok((service_account, role_binding))
}

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
///
/// Note that you should generally *not* hard-code clients to use these services; instead, create a [`ZookeeperZnode`](`stackable_zookeeper_crd::ZookeeperZnode`)
/// and use the connection string that it gives you.
pub fn build_server_role_service(
    zk: &ZookeeperCluster,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Service> {
    let role_name = ZookeeperRole::Server.to_string();
    let role_svc_name = zk
        .server_role_service_name()
        .context(GlobalServiceNameNotFoundSnafu)?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(zk)
            .name(&role_svc_name)
            .ownerreference_from_resource(zk, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                zk,
                ZK_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &role_name,
                "global",
            ))
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("zk".to_string()),
                port: zk.client_port().into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
            selector: Some(role_selector_labels(zk, APP_NAME, &role_name)),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_server_rolegroup_config_map(
    zk: &ZookeeperCluster,
    rolegroup: &RoleGroupRef<ZookeeperCluster>,
    server_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    resolved_product_image: &ResolvedProductImage,
    vector_aggregator_address: Option<&str>,
) -> Result<ConfigMap> {
    let mut zoo_cfg = server_config
        .get(&PropertyNameKind::File(
            ZOOKEEPER_PROPERTIES_FILE.to_string(),
        ))
        .cloned()
        .unwrap_or_default();
    zoo_cfg.extend(zk.pods().into_iter().flatten().map(|pod| {
        (
            format!("server.{}", pod.zookeeper_myid),
            format!("{}:2888:3888;{}", pod.fqdn(), zk.client_port()),
        )
    }));

    let role =
        ZookeeperRole::from_str(&rolegroup.role).with_context(|_| RoleParseFailureSnafu {
            role: rolegroup.role.to_string(),
        })?;

    let zoo_cfg = zoo_cfg
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect::<Vec<_>>();
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
                .build(),
        )
        .add_data(
            ZOOKEEPER_PROPERTIES_FILE,
            to_java_properties_string(zoo_cfg.iter().map(|(k, v)| (k, v))).with_context(|_| {
                SerializeZooCfgSnafu {
                    rolegroup: rolegroup.clone(),
                }
            })?,
        );

    extend_role_group_config_map(
        zk,
        role,
        rolegroup,
        vector_aggregator_address,
        &mut cm_builder,
    )
    .context(InvalidLoggingConfigSnafu {
        cm_name: rolegroup.object_name(),
    })?;

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
    zk: &ZookeeperCluster,
    rolegroup: &RoleGroupRef<ZookeeperCluster>,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Service> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(zk)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(zk, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                zk,
                ZK_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &rolegroup.role,
                &rolegroup.role_group,
            ))
            .with_label("prometheus.io/scrape", "true")
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(vec![
                ServicePort {
                    name: Some("zk".to_string()),
                    port: zk.client_port().into(),
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                },
                ServicePort {
                    name: Some("metrics".to_string()),
                    port: 9505,
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                },
            ]),
            selector: Some(role_group_selector_labels(
                zk,
                APP_NAME,
                &rolegroup.role,
                &rolegroup.role_group,
            )),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_server_rolegroup_service`]).
fn build_server_rolegroup_statefulset(
    zk: &ZookeeperCluster,
    rolegroup_ref: &RoleGroupRef<ZookeeperCluster>,
    server_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    client_authentication_class: Option<&AuthenticationClass>,
    resolved_product_image: &ResolvedProductImage,
) -> Result<StatefulSet> {
    let zk_role =
        ZookeeperRole::from_str(&rolegroup_ref.role).with_context(|_| RoleParseFailureSnafu {
            role: rolegroup_ref.role.to_string(),
        })?;
    let rolegroup = zk
        .spec
        .servers
        .as_ref()
        .context(NoServerRoleSnafu)?
        .role_groups
        .get(&rolegroup_ref.role_group);

    let logging = zk
        .logging(&zk_role, rolegroup_ref)
        .context(CrdValidationFailureSnafu)?;

    let mut env_vars = server_config
        .get(&PropertyNameKind::Env)
        .into_iter()
        .flatten()
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect::<Vec<_>>();

    let (pvc, resources) = zk
        .resources(&zk_role, rolegroup_ref)
        .context(CrdValidationFailureSnafu)?;
    // set heap size if available
    let heap_limits = zk
        .heap_limits(&resources)
        .context(InvalidJavaHeapConfigSnafu)?;
    if let Some(heap_limits) = heap_limits {
        env_vars.push(EnvVar {
            name: ZookeeperConfig::ZK_SERVER_HEAP.to_string(),
            value: Some(heap_limits.to_string()),
            ..EnvVar::default()
        });
    }

    let mut cb_prepare =
        ContainerBuilder::new("prepare").expect("invalid hard-coded container name");
    let mut cb_zookeeper =
        ContainerBuilder::new(APP_NAME).expect("invalid hard-coded container name");
    let mut pod_builder = PodBuilder::new();

    // add volumes and mounts depending on tls / auth settings
    tls_volume_mounts(
        zk,
        &mut pod_builder,
        &mut cb_prepare,
        &mut cb_zookeeper,
        client_authentication_class,
    )?;

    let mut args = Vec::new();

    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&Container::Prepare)
    {
        args.push(product_logging::framework::capture_shell_output(
            STACKABLE_LOG_DIR,
            "prepare",
            log_config,
        ));
    }
    args.push(create_init_container_command_args(zk));

    let container_prepare = cb_prepare
        .image_from_product_image(resolved_product_image)
        .command(vec!["bash".to_string(), "-c".to_string()])
        .args(vec![args.join(" && ")])
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
        .add_volume_mount("config", STACKABLE_CONFIG_DIR)
        .add_volume_mount("rwconfig", STACKABLE_RW_CONFIG_DIR)
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .build();

    let container_zk = cb_zookeeper
        .image_from_product_image(resolved_product_image)
        .args(vec![
            "bin/zkServer.sh".to_string(),
            "start-foreground".to_string(),
            format!("{dir}/zoo.cfg", dir = STACKABLE_RW_CONFIG_DIR),
        ])
        .add_env_vars(env_vars)
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
                        zk.client_port()
                    ),
                ]),
            }),
            period_seconds: Some(1),
            ..Probe::default()
        })
        .add_container_port("zk", zk.client_port().into())
        .add_container_port("zk-leader", 2888)
        .add_container_port("zk-election", 3888)
        .add_container_port("metrics", 9505)
        .add_volume_mount("data", STACKABLE_DATA_DIR)
        .add_volume_mount("config", STACKABLE_CONFIG_DIR)
        .add_volume_mount("log-config", STACKABLE_LOG_CONFIG_DIR)
        .add_volume_mount("rwconfig", STACKABLE_RW_CONFIG_DIR)
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .resources(resources)
        .build();

    pod_builder
        .metadata_builder(|m| {
            m.with_recommended_labels(build_recommended_labels(
                zk,
                ZK_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
        })
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_init_container(container_prepare)
        .add_container(container_zk)
        .node_selector_opt(rolegroup.and_then(|rg| rg.selector.clone()))
        .add_volume(Volume {
            name: "config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(rolegroup_ref.object_name()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .add_volume(Volume {
            empty_dir: Some(EmptyDirVolumeSource {
                medium: None,
                size_limit: None,
            }),
            name: "rwconfig".to_string(),
            ..Volume::default()
        })
        .add_volume(Volume {
            name: "log".to_string(),
            empty_dir: Some(EmptyDirVolumeSource {
                medium: None,
                size_limit: Some(Quantity(format!("{LOG_VOLUME_SIZE_IN_MIB}Mi"))),
            }),
            ..Volume::default()
        })
        .security_context(PodSecurityContext {
            run_as_user: Some(1000),
            run_as_group: Some(1000),
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        })
        .service_account_name(SERVICE_ACCOUNT);

    if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = logging.containers.get(&Container::Zookeeper)
    {
        pod_builder.add_volume(Volume {
            name: "log-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(config_map.into()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    } else {
        pod_builder.add_volume(Volume {
            name: "log-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(rolegroup_ref.object_name()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    }

    if logging.enable_vector_agent {
        pod_builder.add_container(product_logging::framework::vector_container(
            resolved_product_image,
            "config",
            "log",
            logging.containers.get(&Container::Vector),
        ));
    }

    let pod_template = pod_builder.build_template();

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(zk)
            .name(&rolegroup_ref.object_name())
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
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: if zk.spec.stopped.unwrap_or(false) {
                Some(0)
            } else {
                rolegroup.and_then(|rg| rg.replicas).map(i32::from)
            },
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    zk,
                    APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )),
                ..LabelSelector::default()
            },
            service_name: rolegroup_ref.object_name(),
            template: pod_template,
            volume_claim_templates: Some(pvc),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

fn tls_volume_mounts(
    zk: &ZookeeperCluster,
    pod_builder: &mut PodBuilder,
    cb_prepare: &mut ContainerBuilder,
    cb_zookeeper: &mut ContainerBuilder,
    client_authentication_class: Option<&AuthenticationClass>,
) -> Result<()> {
    let tls_secret_class = if let Some(auth_class) = client_authentication_class {
        match &auth_class.spec.provider {
            AuthenticationClassProvider::Tls(TlsAuthenticationProvider {
                client_cert_secret_class: Some(secret_class),
            }) => Some(secret_class),
            _ => {
                return Err(Error::AuthenticationMethodNotSupported {
                    authentication_class: ObjectRef::from_obj(auth_class),
                    supported: vec!["tls".to_string()],
                    method: auth_class.spec.provider.to_string(),
                })
            }
        }
    } else {
        zk.server_tls_secret_class()
    };

    if let Some(secret_class) = tls_secret_class {
        // mounts for secret volume
        cb_prepare.add_volume_mount("client-tls-mount", CLIENT_TLS_MOUNT_DIR);
        cb_zookeeper.add_volume_mount("client-tls-mount", CLIENT_TLS_MOUNT_DIR);
        pod_builder.add_volume(create_tls_volume("client-tls-mount", secret_class));
        // empty mount for trust and keystore
        cb_prepare.add_volume_mount("client-tls", CLIENT_TLS_DIR);
        cb_zookeeper.add_volume_mount("client-tls", CLIENT_TLS_DIR);
        pod_builder.add_volume(
            VolumeBuilder::new("client-tls")
                .with_empty_dir(Some(""), None)
                .build(),
        );
    }

    // quorum
    // mounts for secret volume
    if let Some(quorum_tls) = zk.quorum_tls_secret_class() {
        cb_prepare.add_volume_mount("quorum-tls-mount", QUORUM_TLS_MOUNT_DIR);
        cb_zookeeper.add_volume_mount("quorum-tls-mount", QUORUM_TLS_MOUNT_DIR);
        pod_builder.add_volume(create_tls_volume("quorum-tls-mount", quorum_tls));
        // empty mount for trust and keystore
        cb_prepare.add_volume_mount("quorum-tls", QUORUM_TLS_DIR);
        cb_zookeeper.add_volume_mount("quorum-tls", QUORUM_TLS_DIR);
        pod_builder.add_volume(
            VolumeBuilder::new("quorum-tls")
                .with_empty_dir(Some(""), None)
                .build(),
        );
    }
    Ok(())
}

fn create_tls_volume(volume_name: &str, secret_class_name: &str) -> Volume {
    VolumeBuilder::new(volume_name)
        .ephemeral(
            SecretOperatorVolumeSourceBuilder::new(secret_class_name)
                .with_pod_scope()
                .with_node_scope()
                .build(),
        )
        .build()
}

pub fn error_policy(
    _obj: Arc<ZookeeperCluster>,
    _error: &Error,
    _ctx: Arc<Ctx>,
) -> controller::Action {
    controller::Action::requeue(Duration::from_secs(5))
}
