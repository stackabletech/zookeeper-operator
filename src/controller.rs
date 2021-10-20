use std::{collections::BTreeMap, time::Duration};

use crate::crd::ZookeeperCluster;
use k8s_openapi::{
    api::{
        apps::v1::{StatefulSet, StatefulSetSpec},
        core::v1::{
            Container, ContainerPort, EnvVar, EnvVarSource, ObjectFieldSelector,
            PersistentVolumeClaim, PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec,
            ResourceRequirements, Service, ServicePort, ServiceSpec, VolumeMount,
        },
    },
    apimachinery::pkg::{
        api::resource::Quantity,
        apis::meta::v1::{LabelSelector, OwnerReference},
    },
};
use kube::{
    api::{ObjectMeta, Patch, PatchParams},
    Resource,
};
use kube_runtime::controller::{Context, ReconcilerAction};
use snafu::{ResultExt, Snafu};

pub struct Ctx {
    pub kube: kube::Client,
}

#[derive(Snafu, Debug)]
pub enum Error {
    ApplyService { source: kube::Error },
    ApplyStatefulSet { source: kube::Error },
}

fn controller_reference_to_obj<K: Resource<DynamicType = ()>>(obj: &K) -> OwnerReference {
    OwnerReference {
        api_version: K::api_version(&()).into_owned(),
        kind: K::kind(&()).into_owned(),
        controller: Some(true),
        name: obj.meta().name.clone().unwrap(),
        uid: obj.meta().uid.clone().unwrap(),
        ..OwnerReference::default()
    }
}

pub async fn reconcile_zk(
    zk: ZookeeperCluster,
    ctx: Context<Ctx>,
) -> Result<ReconcilerAction, Error> {
    let ns = zk.metadata.namespace.as_deref().unwrap();
    let stses = kube::Api::<StatefulSet>::namespaced(ctx.get_ref().kube.clone(), ns);
    let svcs = kube::Api::<Service>::namespaced(ctx.get_ref().kube.clone(), ns);

    let name = zk.metadata.name.clone().unwrap();
    let zk_owner_ref = controller_reference_to_obj(&zk);
    let pod_labels = {
        let mut map = BTreeMap::new();
        map.insert("app".to_string(), "zookeeper".to_string());
        map
    };
    svcs.patch(
        &name,
        &PatchParams {
            force: true,
            field_manager: Some("zookeeper.stackable.tech/zookeepercluster".to_string()),
            ..PatchParams::default()
        },
        &Patch::Apply(Service {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                owner_references: Some(vec![zk_owner_ref.clone()]),
                ..ObjectMeta::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    name: Some("zk".to_string()),
                    port: 2181,
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                }]),
                selector: Some(pod_labels.clone()),
                type_: Some("NodePort".to_string()),
                ..ServiceSpec::default()
            }),
            status: None,
        }),
    )
    .await
    .unwrap();
    let pod_template = PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: Some(pod_labels.clone()),
            ..ObjectMeta::default()
        }),
        spec: Some(PodSpec {
            init_containers: Some(vec![Container {
                name: "decide-myid".to_string(),
                image: Some("alpine".to_string()),
                args: Some(vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "expr 1 + $(echo $POD_NAME | sed 's/.*-//') > /data/myid".to_string(),
                ]),
                env: Some(vec![EnvVar {
                    name: "POD_NAME".to_string(),
                    value_from: Some(EnvVarSource {
                        field_ref: Some(ObjectFieldSelector {
                            api_version: Some("v1".to_string()),
                            field_path: "metadata.name".to_string(),
                        }),
                        ..EnvVarSource::default()
                    }),
                    ..EnvVar::default()
                }]),
                volume_mounts: Some(vec![VolumeMount {
                    mount_path: "/data".to_string(),
                    name: "data".to_string(),
                    ..VolumeMount::default()
                }]),
                ..Container::default()
            }]),
            containers: vec![Container {
                name: "zookeeper".to_string(),
                image: Some("zookeeper:3.7.0".to_string()),
                env: Some(vec![EnvVar {
                    name: "ZOO_SERVERS".to_string(),
                    value: Some(
                        (1..=zk.spec.replicas.unwrap())
                            .map(|i| {
                                format!(
                                    "server.{}={}-{}.{}.{}.svc.cluster.local:2888:3888;2181",
                                    i,
                                    name,
                                    i - 1,
                                    name,
                                    ns
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(" "),
                    ),
                    ..EnvVar::default()
                }]),
                ports: Some(vec![
                    ContainerPort {
                        container_port: 2181,
                        name: Some("zk".to_string()),
                        protocol: Some("TCP".to_string()),
                        ..ContainerPort::default()
                    },
                    ContainerPort {
                        container_port: 2888,
                        name: Some("zk-leader".to_string()),
                        protocol: Some("TCP".to_string()),
                        ..ContainerPort::default()
                    },
                    ContainerPort {
                        container_port: 3888,
                        name: Some("zk-election".to_string()),
                        protocol: Some("TCP".to_string()),
                        ..ContainerPort::default()
                    },
                ]),
                volume_mounts: Some(vec![VolumeMount {
                    mount_path: "/data".to_string(),
                    name: "data".to_string(),
                    ..VolumeMount::default()
                }]),
                ..Container::default()
            }],
            ..PodSpec::default()
        }),
    };
    stses
        .patch(
            &name,
            &PatchParams {
                force: true,
                field_manager: Some("zookeeper.stackable.tech/zookeepercluster".to_string()),
                ..PatchParams::default()
            },
            &Patch::Apply(StatefulSet {
                metadata: ObjectMeta {
                    name: Some(name.clone()),
                    owner_references: Some(vec![zk_owner_ref.clone()]),
                    ..ObjectMeta::default()
                },
                spec: Some(StatefulSetSpec {
                    pod_management_policy: Some("Parallel".to_string()),
                    replicas: zk.spec.replicas,
                    selector: LabelSelector {
                        match_labels: Some(pod_labels.clone()),
                        ..LabelSelector::default()
                    },
                    service_name: name.clone(),
                    template: pod_template,
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
                    // volume_claim_templates: todo!(),
                    ..StatefulSetSpec::default()
                }),
                status: None,
            }),
        )
        .await
        .context(ApplyStatefulSet)?;

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}
