//! Ensures that `Pod`s are configured and running for each [`ZookeeperCluster`]

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    time::Duration,
};

use crate::utils::apply_owned;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder},
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, EnvVar, EnvVarSource, ExecAction,
                ObjectFieldSelector, PersistentVolumeClaim, PersistentVolumeClaimSpec, Probe,
                ResourceRequirements, Service, ServicePort, ServiceSpec, Volume,
            },
        },
        apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
    },
    kube::{
        self,
        api::ObjectMeta,
        runtime::{
            controller::{Context, ReconcilerAction},
            reflector::ObjectRef,
        },
    },
    labels::role_group_selector_labels,
    product_config::{
        types::PropertyNameKind, writer::to_java_properties_string, ProductConfigManager,
    },
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::Role,
};
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperRole};

const FIELD_MANAGER: &str = "zookeeper.stackable.tech/zookeepercluster";
const APP_NAME: &str = "zookeeper";
const APP_ROLE_SERVERS: &str = "servers";
const APP_ROLEGROUP_SERVERS: &str = "servers";

pub struct Ctx {
    pub kube: kube::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object {} has no namespace", obj_ref))]
    ObjectHasNoNamespace {
        obj_ref: ObjectRef<ZookeeperCluster>,
    },
    #[snafu(display("object {} defines no version", obj_ref))]
    ObjectHasNoVersion {
        obj_ref: ObjectRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to calculate global service name for {}", obj_ref))]
    GlobalServiceNameNotFound {
        obj_ref: ObjectRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to calculate service name for role {}", rolegroup))]
    RoleGroupServiceNameNotFound { rolegroup: RoleGroupRef },
    #[snafu(display("failed to apply global Service for {}", zk))]
    ApplyGlobalService {
        source: kube::Error,
        zk: ObjectRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: kube::Error,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("failed to apply ConfigMap for {}", rolegroup))]
    ApplyRoleGroupConfig {
        source: kube::Error,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: kube::Error,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("invalid product config for {}", zk))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
        zk: ObjectRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to serialize zoo.cfg for {}", rolegroup))]
    SerializeZooCfg {
        source: stackable_operator::product_config::writer::PropertiesWriterError,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("object {} is missing metadata to build owner reference", zk))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        zk: ObjectRef<ZookeeperCluster>,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

const PROPERTIES_FILE: &str = "zoo.cfg";

pub async fn reconcile_zk(zk: ZookeeperCluster, ctx: Context<Ctx>) -> Result<ReconcilerAction> {
    let zk_ref = ObjectRef::from_obj(&zk);
    let kube = ctx.get_ref().kube.clone();

    let zk_version = zk
        .spec
        .version
        .as_deref()
        .with_context(|| ObjectHasNoVersion {
            obj_ref: zk_ref.clone(),
        })?;
    let mut validated_config = validate_all_roles_and_groups_config(
        zk_version,
        &transform_all_roles_to_config(
            &zk,
            [(
                ZookeeperRole::Server.to_string(),
                (
                    vec![PropertyNameKind::File(PROPERTIES_FILE.to_string())],
                    Role {
                        config: None,
                        role_groups: [(APP_ROLEGROUP_SERVERS.to_string(), zk.spec.servers.clone())]
                            .into(),
                    },
                ),
            )]
            .into(),
        ),
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .with_context(|| InvalidProductConfig { zk: zk_ref.clone() })?;
    let rolegroup_server = RoleGroupRef {
        cluster: ObjectRef::from_obj(&zk),
        role: APP_ROLE_SERVERS.to_string(),
        role_group: APP_ROLEGROUP_SERVERS.to_string(),
    };
    let rolegroup_server_config = validated_config
        .remove(&ZookeeperRole::Server.to_string())
        .and_then(|mut role_cfg| role_cfg.remove(APP_ROLE_SERVERS))
        .unwrap_or_default();

    apply_owned(&kube, FIELD_MANAGER, &build_global_service(&zk)?)
        .await
        .with_context(|| ApplyGlobalService { zk: zk_ref.clone() })?;
    apply_owned(
        &kube,
        FIELD_MANAGER,
        &build_rolegroup_service(&rolegroup_server, &zk)?,
    )
    .await
    .with_context(|| ApplyRoleGroupService {
        rolegroup: rolegroup_server.clone(),
    })?;
    apply_owned(
        &kube,
        FIELD_MANAGER,
        &build_rolegroup_config_map(&rolegroup_server, &zk, &rolegroup_server_config)?,
    )
    .await
    .with_context(|| ApplyRoleGroupConfig {
        rolegroup: rolegroup_server.clone(),
    })?;
    apply_owned(
        &kube,
        FIELD_MANAGER,
        &build_rolegroup_statefulset(&rolegroup_server, &zk)?,
    )
    .await
    .with_context(|| ApplyRoleGroupStatefulSet {
        rolegroup: rolegroup_server.clone(),
    })?;

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

/// The "global service" is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
///
/// Note that you should generally *not* hard-code clients to use these services; instead, create a [`ZookeeperZnode`](`stackable_zookeeper_crd::ZookeeperZnode`)
/// and use the connection string that it gives you.
pub fn build_global_service(zk: &ZookeeperCluster) -> Result<Service> {
    let global_svc_name = zk
        .global_service_name()
        .with_context(|| GlobalServiceNameNotFound {
            obj_ref: ObjectRef::from_obj(zk),
        })?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(zk)
            .name(&global_svc_name)
            .ownerreference_from_resource(zk, None, Some(true))
            .with_context(|| ObjectMissingMetadataForOwnerRef {
                zk: ObjectRef::from_obj(zk),
            })?
            .with_recommended_labels(
                zk,
                APP_NAME,
                zk_version(zk)?,
                APP_ROLE_SERVERS,
                APP_ROLEGROUP_SERVERS,
            )
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("zk".to_string()),
                port: 2181,
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
            selector: Some(role_group_selector_labels(
                zk,
                APP_NAME,
                APP_ROLE_SERVERS,
                APP_ROLEGROUP_SERVERS,
            )),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_rolegroup_config_map(
    rolegroup: &RoleGroupRef,
    zk: &ZookeeperCluster,
    server_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<ConfigMap> {
    let mut zoo_cfg = server_config
        .get(&PropertyNameKind::File(PROPERTIES_FILE.to_string()))
        .cloned()
        .unwrap_or_default();
    zoo_cfg.insert("dataDir".to_string(), "/stackable/data".to_string());
    zoo_cfg.insert("clientPort".to_string(), "2181".to_string());
    zoo_cfg.extend(zk.pods().into_iter().flatten().map(|pod| {
        (
            format!("server.{}", pod.zookeeper_id),
            format!("{}:2888:3888;2181", pod.fqdn()),
        )
    }));
    let zoo_cfg = zoo_cfg
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect::<Vec<_>>();
    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(zk)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(zk, None, Some(true))
                .with_context(|| ObjectMissingMetadataForOwnerRef {
                    zk: ObjectRef::from_obj(zk),
                })?
                .with_recommended_labels(
                    zk,
                    APP_NAME,
                    zk_version(zk)?,
                    APP_ROLE_SERVERS,
                    APP_ROLEGROUP_SERVERS,
                )
                .build(),
        )
        .add_data(
            "zoo.cfg",
            to_java_properties_string(zoo_cfg.iter().map(|(k, v)| (k, v))).with_context(|| {
                SerializeZooCfg {
                    rolegroup: rolegroup.clone(),
                }
            })?,
        )
        .build()
        .with_context(|| BuildRoleGroupConfig {
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_rolegroup_service(rolegroup: &RoleGroupRef, zk: &ZookeeperCluster) -> Result<Service> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(zk)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(zk, None, Some(true))
            .with_context(|| ObjectMissingMetadataForOwnerRef {
                zk: ObjectRef::from_obj(zk),
            })?
            .with_recommended_labels(
                zk,
                APP_NAME,
                zk_version(zk)?,
                APP_ROLE_SERVERS,
                APP_ROLEGROUP_SERVERS,
            )
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(vec![ServicePort {
                name: Some("zk".to_string()),
                port: 2181,
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
            selector: Some(role_group_selector_labels(
                zk,
                APP_NAME,
                APP_ROLE_SERVERS,
                APP_ROLEGROUP_SERVERS,
            )),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_rolegroup_service`]).
fn build_rolegroup_statefulset(
    rolegroup: &RoleGroupRef,
    zk: &ZookeeperCluster,
) -> Result<StatefulSet> {
    let zk_version = zk_version(zk)?;
    let container_decide_myid = ContainerBuilder::new("decide-myid")
        .image("alpine")
        .args(vec![
            "sh".to_string(),
            "-c".to_string(),
            "expr 1 + $(echo $POD_NAME | sed 's/.*-//') > /stackable/data/myid".to_string(),
        ])
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
        .add_volume_mount("data", "/stackable/data")
        .build();
    let container_zk = ContainerBuilder::new("zookeeper")
        .image(format!(
            "docker.stackable.tech/stackable/zookeeper:{}-stackable0",
            zk_version
        ))
        .args(vec![
            "bin/zkServer.sh".to_string(),
            "start-foreground".to_string(),
            "/stackable/config/zoo.cfg".to_string(),
        ])
        // Only allow the global load balancing service to send traffic to pods that are members of the quorum
        // This also acts as a hint to the StatefulSet controller to wait for each pod to enter quorum before taking down the next
        .readiness_probe(Probe {
            exec: Some(ExecAction {
                command: Some(vec![
                    "bash".to_string(),
                    "-c".to_string(),
                    // We don't have telnet or netcat in the container images, but
                    // we can use Bash's virtual /dev/tcp filesystem to accomplish the same thing
                    "exec 3<>/dev/tcp/localhost/2181 && echo srvr >&3 && grep '^Mode: ' <&3"
                        .to_string(),
                ]),
            }),
            period_seconds: Some(1),
            ..Probe::default()
        })
        .add_container_port("zk", 2181)
        .add_container_port("zk-leader", 2888)
        .add_container_port("zk-election", 3888)
        .add_volume_mount("data", "/stackable/data")
        .add_volume_mount("config", "/stackable/config")
        .build();
    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(zk)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(zk, None, Some(true))
            .with_context(|| ObjectMissingMetadataForOwnerRef {
                zk: ObjectRef::from_obj(zk),
            })?
            .with_recommended_labels(
                zk,
                APP_NAME,
                zk_version,
                APP_ROLE_SERVERS,
                APP_ROLEGROUP_SERVERS,
            )
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: if zk.spec.stopped.unwrap_or(false) {
                Some(0)
            } else {
                zk.spec.servers.replicas.map(i32::from)
            },
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    zk,
                    APP_NAME,
                    APP_ROLE_SERVERS,
                    APP_ROLEGROUP_SERVERS,
                )),
                ..LabelSelector::default()
            },
            service_name: rolegroup.object_name(),
            template: PodBuilder::new()
                .metadata_builder(|m| {
                    m.with_recommended_labels(
                        zk,
                        APP_NAME,
                        zk_version,
                        APP_ROLE_SERVERS,
                        APP_ROLEGROUP_SERVERS,
                    )
                })
                .add_init_container(container_decide_myid)
                .add_container(container_zk)
                .add_volume(Volume {
                    name: "config".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: Some(rolegroup.object_name()),
                        ..ConfigMapVolumeSource::default()
                    }),
                    ..Volume::default()
                })
                .build_template(),
            volume_claim_templates: Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some("data".to_string()),
                    ..ObjectMeta::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    resources: Some(ResourceRequirements {
                        requests: Some({
                            let mut map = BTreeMap::new();
                            map.insert("storage".to_string(), Quantity("1Gi".to_string()));
                            map
                        }),
                        ..ResourceRequirements::default()
                    }),
                    ..PersistentVolumeClaimSpec::default()
                }),
                ..PersistentVolumeClaim::default()
            }]),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

fn zk_version(zk: &ZookeeperCluster) -> Result<&str> {
    zk.spec
        .version
        .as_deref()
        .with_context(|| ObjectHasNoVersion {
            obj_ref: ObjectRef::from_obj(zk),
        })
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}

#[derive(Debug, Clone)]
pub struct RoleGroupRef {
    cluster: ObjectRef<ZookeeperCluster>,
    role: String,
    role_group: String,
}

impl RoleGroupRef {
    fn object_name(&self) -> String {
        format!("{}-{}-{}", self.cluster.name, self.role, self.role_group)
    }
}

impl Display for RoleGroupRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "rolegroup {}.{} of {}",
            self.role, self.role_group, self.cluster
        ))
    }
}
