//! Ensures that `Pod`s are configured and running for each [`ZookeeperCluster`]

use std::{collections::BTreeMap, time::Duration};

use crate::utils::{apply_owned, controller_reference_to_obj};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder},
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMapVolumeSource, EnvVar, EnvVarSource, ExecAction, ObjectFieldSelector,
                PersistentVolumeClaim, PersistentVolumeClaimSpec, Probe, ResourceRequirements,
                Service, ServicePort, ServiceSpec, Volume,
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
};
use stackable_zookeeper_crd::ZookeeperCluster;

const FIELD_MANAGER: &str = "zookeeper.stackable.tech/zookeepercluster";
const APP_NAME: &str = "zookeeper";
const APP_ROLE_SERVERS: &str = "servers";
const APP_ROLEGROUP_SERVERS: &str = "servers";

pub struct Ctx {
    pub kube: kube::Client,
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
    #[snafu(display("failed to calculate service name for role {} of {}", role, obj_ref))]
    RoleServiceNameNotFound {
        obj_ref: ObjectRef<ZookeeperCluster>,
        role: String,
    },
    #[snafu(display("failed to apply global Service for {}", zk))]
    ApplyGlobalService {
        source: kube::Error,
        zk: ObjectRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to apply Service for role {} of {}", role, zk))]
    ApplyRoleService {
        source: kube::Error,
        zk: ObjectRef<ZookeeperCluster>,
        role: String,
    },
    #[snafu(display("failed to apply ConfigMap for role {} of {}", role, zk))]
    ApplyRoleConfig {
        source: kube::Error,
        zk: ObjectRef<ZookeeperCluster>,
        role: String,
    },
    #[snafu(display("failed to apply StatefulSet for role {} of {}", role, zk))]
    ApplyRoleStatefulSet {
        source: kube::Error,
        zk: ObjectRef<ZookeeperCluster>,
        role: String,
    },
}

pub async fn reconcile_zk(
    zk: ZookeeperCluster,
    ctx: Context<Ctx>,
) -> Result<ReconcilerAction, Error> {
    let zk_ref = ObjectRef::from_obj(&zk);
    let ns = zk
        .metadata
        .namespace
        .as_deref()
        .with_context(|| ObjectHasNoNamespace {
            obj_ref: zk_ref.clone(),
        })?;
    let kube = ctx.get_ref().kube.clone();

    let zk_version = zk
        .spec
        .version
        .as_deref()
        .with_context(|| ObjectHasNoVersion {
            obj_ref: zk_ref.clone(),
        })?;
    let global_svc_name = zk
        .global_service_name()
        .with_context(|| GlobalServiceNameNotFound {
            obj_ref: zk_ref.clone(),
        })?;
    let role_svc_servers_name =
        zk.server_role_service_name()
            .with_context(|| RoleServiceNameNotFound {
                obj_ref: zk_ref.clone(),
                role: "servers",
            })?;
    let zk_owner_ref = controller_reference_to_obj(&zk);
    apply_owned(
        &kube,
        FIELD_MANAGER,
        &Service {
            metadata: ObjectMetaBuilder::new()
                .name(&global_svc_name)
                .namespace(ns)
                .ownerreference(zk_owner_ref.clone())
                .with_recommended_labels(
                    &zk,
                    APP_NAME,
                    zk_version,
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
                    &zk,
                    APP_NAME,
                    APP_ROLE_SERVERS,
                    APP_ROLEGROUP_SERVERS,
                )),
                type_: Some("NodePort".to_string()),
                ..ServiceSpec::default()
            }),
            status: None,
        },
    )
    .await
    .with_context(|| ApplyGlobalService { zk: zk_ref.clone() })?;
    apply_owned(
        &kube,
        FIELD_MANAGER,
        &Service {
            metadata: ObjectMetaBuilder::new()
                .name(&role_svc_servers_name)
                .namespace(ns)
                .ownerreference(zk_owner_ref.clone())
                .with_recommended_labels(
                    &zk,
                    APP_NAME,
                    zk_version,
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
                    &zk,
                    APP_NAME,
                    APP_ROLE_SERVERS,
                    APP_ROLEGROUP_SERVERS,
                )),
                publish_not_ready_addresses: Some(true),
                ..ServiceSpec::default()
            }),
            status: None,
        },
    )
    .await
    .with_context(|| ApplyRoleService {
        role: "servers",
        zk: zk_ref.clone(),
    })?;
    apply_owned(
        &kube,
        FIELD_MANAGER,
        &ConfigMapBuilder::new()
            .metadata(
                ObjectMetaBuilder::new()
                    .name(&role_svc_servers_name)
                    .namespace(ns)
                    .ownerreference(zk_owner_ref.clone())
                    .with_recommended_labels(
                        &zk,
                        APP_NAME,
                        zk_version,
                        APP_ROLE_SERVERS,
                        APP_ROLEGROUP_SERVERS,
                    )
                    .build(),
            )
            .add_data(
                "zoo.cfg",
                format!(
                    "
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/data
clientPort=2181
{}
",
                    zk.pods()
                        .unwrap()
                        .into_iter()
                        .map(|pod| format!(
                            "server.{}={}:2888:3888;2181",
                            pod.zookeeper_id,
                            pod.fqdn()
                        ))
                        .collect::<Vec<_>>()
                        .join("\n")
                ),
            )
            .build()
            .unwrap(),
    )
    .await
    .with_context(|| ApplyRoleConfig {
        role: "servers",
        zk: zk_ref.clone(),
    })?;
    let container_decide_myid = ContainerBuilder::new("decide-myid")
        .image("alpine")
        .args(vec![
            "sh".to_string(),
            "-c".to_string(),
            "expr 1 + $(echo $POD_NAME | sed 's/.*-//') > /data/myid".to_string(),
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
        .add_volume_mount("data", "/data")
        .build();
    let container_zk = ContainerBuilder::new("zookeeper")
        .image(format!(
            "docker.stackable.tech/stackable/zookeeper:{}-stackable0",
            zk_version
        ))
        .args(vec![
            "bin/zkServer.sh".to_string(),
            "start-foreground".to_string(),
            "/config/zoo.cfg".to_string(),
        ])
        .readiness_probe(Probe {
            exec: Some(ExecAction {
                command: Some(vec![
                    "sh".to_string(),
                    "-c".to_string(),
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
        .add_volume_mount("data", "/data")
        .add_volume_mount("config", "/config")
        .build();
    apply_owned(
        &kube,
        FIELD_MANAGER,
        &StatefulSet {
            metadata: ObjectMetaBuilder::new()
                .name(&role_svc_servers_name)
                .namespace(ns)
                .ownerreference(zk_owner_ref.clone())
                .with_recommended_labels(
                    &zk,
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
                        &zk,
                        APP_NAME,
                        APP_ROLE_SERVERS,
                        APP_ROLEGROUP_SERVERS,
                    )),
                    ..LabelSelector::default()
                },
                service_name: role_svc_servers_name.clone(),
                template: PodBuilder::new()
                    .metadata_builder(|m| {
                        m.with_recommended_labels(
                            &zk,
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
                            name: Some(role_svc_servers_name.clone()),
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
        },
    )
    .await
    .with_context(|| ApplyRoleStatefulSet {
        role: "servers",
        zk: zk_ref.clone(),
    })?;

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}
