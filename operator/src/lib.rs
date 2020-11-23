mod error;
mod finalizer;

use crate::error::Error;

use kube::{Api, Client};
use tracing::{error, info};

use futures::StreamExt;
use handlebars::Handlebars;
use k8s_openapi::api::core::v1::{
    Affinity, ConfigMap, ConfigMapVolumeSource, Container, Pod, PodAffinityTerm, PodAntiAffinity,
    PodSpec, Toleration, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use kube::api::{ListParams, Meta, ObjectMeta, PatchParams, PatchStrategy};
use kube_runtime::controller::{Context, ReconcilerAction};
use kube_runtime::Controller;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use stackable_zookeeper_crd::{ZooKeeperCluster, ZooKeeperClusterSpec};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

/// Context data inserted into the reconciliation handler with each call.
struct ContextData {
    /// Kubernetes client to manipulate Kubernetes resources
    client: Client,
}

impl ContextData {
    /// Creates a new instance of `ContextData`.
    ///
    /// # Arguments
    ///
    /// - `client` - Kubernetes client to manipulate Kubernetes resources
    pub fn new(client: Client) -> Self {
        ContextData { client }
    }
}

/// Action to be taken by the controller if there is a new event on one of the watched resources
enum ControllerAction {
    /// A resource was created
    Create,

    /// A resource was updated
    Update,

    /// The resource is about to be deleted
    Delete,
}

const FIELD_MANAGER: &str = "zookeeper.stackable.de";

/// This creates an instance of a [`Controller`] which waits for incoming events and reconciles them.
///
/// This is an async method and the returned future needs to be consumed to make progress.
pub async fn create_controller(client: Client) {
    let zk_api: Api<ZooKeeperCluster> = Api::all(client.clone());
    let pods_api: Api<Pod> = Api::all(client.clone());
    let context = Context::new(ContextData::new(client));

    Controller::new(zk_api, ListParams::default())
        .owns(pods_api, ListParams::default())
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconciled {:?}", o),
                Err(e) => info!("Reconcile failed: {}", e),
            };
        })
        .await
}

/// This method contains the logic of reconciling an object (the desired state) we received with the actual state.
///
/// We distinguish between two different types:
/// * Creation and update (these are handled the same)
/// * Deletion
async fn reconcile(
    zk_cluster: ZooKeeperCluster,
    context: Context<ContextData>,
) -> Result<ReconcilerAction, Error> {
    match decide_controller_action(&zk_cluster) {
        Some(ControllerAction::Create) => {
            create_deployment(&zk_cluster, &context).await?;
        }
        Some(ControllerAction::Update) => {
            update_deployment(&zk_cluster, &context).await?;
        }
        Some(ControllerAction::Delete) => {
            delete_deployment(&zk_cluster, &context).await?;
        }
        None => {
            //TODO: debug!
        }
    }

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

/// This method is being called by the Controller whenever there's an error during reconcilation.
/// We just log the error and requeue the event.
fn error_policy(error: &Error, _context: Context<ContextData>) -> ReconcilerAction {
    error!("Reconciliation error:\n{}", error);
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(10)),
    }
}

/// Examines the incoming resource and determines the `ControllerAction` to be taken upon it.
fn decide_controller_action(zk_cluster: &ZooKeeperCluster) -> Option<ControllerAction> {
    let has_finalizer: bool = finalizer::has_finalizer(&zk_cluster);
    let has_deletion_timestamp: bool = finalizer::has_deletion_stamp(zk_cluster);
    return if has_finalizer && has_deletion_timestamp {
        Some(ControllerAction::Delete)
    } else if !has_finalizer && !has_deletion_timestamp {
        Some(ControllerAction::Create)
    } else if has_finalizer && !has_deletion_timestamp {
        Some(ControllerAction::Update)
    } else {
        // The object is being deleted but we've already finished our finalizer
        // So there's nothing left to do for us on this one.
        None
    };
}

async fn create_deployment(
    zk_cluster: &ZooKeeperCluster,
    context: &Context<ContextData>,
) -> Result<ReconcilerAction, Error> {
    finalizer::add_finalizer(
        context.get_ref().client.clone(),
        &Meta::name(zk_cluster),
        &Meta::namespace(zk_cluster).expect("ZooKeeperCluster is namespaced"),
    )
    .await?;

    update_deployment(zk_cluster, context).await
}

async fn update_deployment(
    zk_cluster: &ZooKeeperCluster,
    context: &Context<ContextData>,
) -> Result<ReconcilerAction, Error> {
    let zk_spec: ZooKeeperClusterSpec = zk_cluster.spec.clone();
    let client = context.get_ref().client.clone();

    let name = Meta::name(zk_cluster);
    let ns = Meta::namespace(zk_cluster).expect("ZooKeeperCluster is namespaced");

    let mut labels = BTreeMap::new();
    labels.insert("zookeeper-name".to_string(), name.clone());

    let mut options = HashMap::new();
    options.insert("tickTime".to_string(), "2000".to_string());
    options.insert("dataDir".to_string(), "/tmp/zookeeper".to_string()); // TODO: Agent needs to know that this must exist (?) and belong to proper user
    options.insert("initLimit".to_string(), "5".to_string());
    options.insert("syncLimit".to_string(), "2".to_string());

    for (i, server) in zk_spec.servers.iter().enumerate() {
        options.insert(
            format!("server.{}", i + 1),
            format!("{}:2888:3888", server.node_name),
        );
    }

    let mut handlebars = Handlebars::new();
    handlebars
        .register_template_string("conf", "{{#each options}}{{@key}}={{this}}\n{{/each}}")
        .expect("template should work");

    let pods_api: Api<Pod> = Api::namespaced(client.clone(), &ns);
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), &ns);

    for (i, server) in zk_spec.servers.iter().enumerate() {
        let pod_name = format!("{}-{}", name, server.node_name);
        let cm_name = format!("zk-{}", pod_name);

        let pod = build_pod(zk_cluster, &labels, &pod_name, &cm_name)?;

        patch_resource(&pods_api, &pod_name, &pod).await?;

        let config = handlebars
            .render("conf", &json!({ "options": options }))
            .unwrap();
        let mut data = BTreeMap::new();
        data.insert("zoo.cfg".to_string(), config);
        data.insert("myid".to_string(), i.to_string());

        let cm = ConfigMap {
            data: Some(data),
            metadata: ObjectMeta {
                name: Some(cm_name.clone()),
                owner_references: Some(vec![OwnerReference {
                    controller: Some(true),
                    ..object_to_owner_reference::<ZooKeeperCluster>(zk_cluster.metadata.clone())?
                }]),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        };

        patch_resource(&cm_api, &cm_name, &cm).await?;
    }

    Ok(ReconcilerAction {
        requeue_after: Option::None,
    })
}

async fn patch_resource<T>(api: &Api<T>, resource_name: &String, resource: &T) -> Result<T, Error>
where
    T: Clone + Meta + DeserializeOwned + Serialize,
{
    api.patch(
        &resource_name,
        &PatchParams {
            patch_strategy: PatchStrategy::Apply,
            field_manager: Some(FIELD_MANAGER.to_string()),
            ..PatchParams::default()
        },
        serde_json::to_vec(&resource)?,
    )
    .await
    .map_err(Error::from)
}

fn build_pod(
    zk_cluster: &ZooKeeperCluster,
    labels: &BTreeMap<String, String>,
    pod_name: &String,
    cm_name: &String,
) -> Result<Pod, Error> {
    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.clone()),
            owner_references: Some(vec![OwnerReference {
                controller: Some(true),
                ..object_to_owner_reference::<ZooKeeperCluster>(zk_cluster.metadata.clone())?
            }]),
            labels: Some(labels.clone()),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            tolerations: Some(create_tolerations()),
            containers: vec![Container {
                image: Some(format!(
                    "stackable/zookeeper:{}",
                    serde_json::json!(zk_cluster.spec.version).as_str().unwrap()
                )),
                name: "zookeeper".to_string(),
                command: Some(vec![
                    "bin/zkServer.sh".to_string(),
                    "--config".to_string(),
                    "{{ configroot }}/conf".to_string(),
                    "start-foreground".to_string(),
                ]),
                volume_mounts: Some(vec![VolumeMount {
                    mount_path: "conf".to_string(),
                    name: "config-volume".to_string(),
                    ..VolumeMount::default()
                }]),
                ..Container::default()
            }],
            volumes: Some(vec![Volume {
                name: "config-volume".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(cm_name.clone()),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            }]),
            affinity: Some(Affinity {
                pod_anti_affinity: Some(PodAntiAffinity {
                    required_during_scheduling_ignored_during_execution: Some(vec![
                        PodAffinityTerm {
                            label_selector: Some(LabelSelector {
                                match_labels: Some(labels.clone()),
                                ..LabelSelector::default()
                            }),
                            topology_key: "kubernetes.io/hostname".to_string(),
                            ..PodAffinityTerm::default()
                        },
                    ]),
                    ..PodAntiAffinity::default()
                }),
                ..Affinity::default()
            }),
            ..PodSpec::default()
        }),
        ..Pod::default()
    };
    Ok(pod)
}

fn object_to_owner_reference<K: Meta>(meta: ObjectMeta) -> Result<OwnerReference, Error> {
    Ok(OwnerReference {
        api_version: K::API_VERSION.to_string(),
        kind: K::KIND.to_string(),
        name: meta.name.ok_or_else(|| Error::MissingObjectKey {
            key: ".metadata.name",
        })?,
        uid: meta.uid.ok_or_else(|| Error::MissingObjectKey {
            key: ".metadata.backtrace",
        })?,
        ..OwnerReference::default()
    })
}

fn create_tolerations() -> Vec<Toleration> {
    vec![
        Toleration {
            effect: Some(String::from("NoExecute")),
            key: Some(String::from("kubernetes.io/arch")),
            operator: Some(String::from("Equal")),
            toleration_seconds: None,
            value: Some(String::from("stackable-linux")),
        },
        Toleration {
            effect: Some(String::from("NoSchedule")),
            key: Some(String::from("kubernetes.io/arch")),
            operator: Some(String::from("Equal")),
            toleration_seconds: None,
            value: Some(String::from("stackable-linux")),
        },
        Toleration {
            effect: Some(String::from("NoSchedule")),
            key: Some(String::from("node.kubernetes.io/network-unavailable")),
            operator: Some(String::from("Exists")),
            toleration_seconds: None,
            value: None,
        },
    ]
}

async fn delete_deployment(
    zk_cluster: &ZooKeeperCluster,
    context: &Context<ContextData>,
) -> Result<ReconcilerAction, Error> {
    info!("Deleting deployment");
    finalizer::remove_finalizer(context.get_ref().client.clone(), zk_cluster).await?;

    Ok(ReconcilerAction {
        requeue_after: Option::None,
    })
}
