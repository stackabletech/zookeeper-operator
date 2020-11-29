mod error;

use crate::error::Error;

use kube::{Api, Client};
use tracing::{error, info};

use futures::StreamExt;
use handlebars::Handlebars;
use k8s_openapi::api::core::v1::{
    Affinity, ConfigMap, ConfigMapVolumeSource, Container, Pod, PodAffinityTerm, PodAntiAffinity,
    PodSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use kube::api::{ListParams, Meta, ObjectMeta};
use kube_runtime::controller::{Context, ReconcilerAction};
use kube_runtime::Controller;
use serde_json::json;
use stackable_operator::finalizer::add_finalizer;
use stackable_operator::{
    create_config_map, create_tolerations, decide_controller_action, finalizer,
    object_to_owner_reference, patch_resource, ContextData, ControllerAction,
};
use stackable_zookeeper_crd::{ZooKeeperCluster, ZooKeeperClusterSpec, ZooKeeperServer};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

const FINALIZER_NAME: &str = "zookeeper.stackable.de/cleanup";
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
/// We distinguish between three different types:
/// * Create
/// * Update
/// * Delete
async fn reconcile(
    zk_cluster: ZooKeeperCluster,
    context: Context<ContextData>,
) -> Result<ReconcilerAction, Error> {
    match decide_controller_action(&zk_cluster, FINALIZER_NAME) {
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

async fn create_deployment(
    zk_cluster: &ZooKeeperCluster,
    context: &Context<ContextData>,
) -> Result<ReconcilerAction, Error> {
    add_finalizer(context.get_ref().client.clone(), zk_cluster, FINALIZER_NAME).await?;

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
    options.insert("clientPort".to_string(), "2181".to_string());

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

        // This builds the Pod object
        let pod = build_pod(zk_cluster, server, &labels, &pod_name, &cm_name)?;
        patch_resource(&pods_api, &pod_name, &pod, FIELD_MANAGER).await?;

        let config = handlebars
            .render("conf", &json!({ "options": options }))
            .unwrap();

        // Now we need to create two configmaps per server.
        // The names are "zk-<cluster name>-<node name>-config" and "zk-<cluster name>-<node name>-data"
        // One for the configuration directory...
        let mut data = BTreeMap::new();
        data.insert("zoo.cfg".to_string(), config);

        let tmp_name = cm_name.clone() + "-config"; // TODO: Create these names once and pass them around so we are consistent
        let cm = create_config_map(zk_cluster, &tmp_name, data)?;
        patch_resource(&cm_api, &tmp_name, &cm, FIELD_MANAGER).await?;

        // ...and one for the data directory.
        let mut data = BTreeMap::new();
        data.insert("myid".to_string(), (i + 1).to_string());
        let tmp_name = cm_name + "-data";
        let cm = create_config_map(zk_cluster, &tmp_name, data)?;
        patch_resource(&cm_api, &tmp_name, &cm, FIELD_MANAGER).await?;
    }

    Ok(ReconcilerAction {
        requeue_after: Option::None,
    })
}

async fn delete_deployment(
    zk_cluster: &ZooKeeperCluster,
    context: &Context<ContextData>,
) -> Result<ReconcilerAction, Error> {
    info!("Deleting deployment");
    finalizer::remove_finalizer(context.get_ref().client.clone(), zk_cluster, FINALIZER_NAME)
        .await?;

    Ok(ReconcilerAction {
        requeue_after: Option::None,
    })
}

fn build_pod(
    zk_cluster: &ZooKeeperCluster,
    zk_server: &ZooKeeperServer,
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
            node_name: Some(zk_server.node_name.clone()),
            tolerations: Some(create_tolerations()),
            containers: vec![Container {
                image: Some(format!(
                    "stackable/zookeeper:{}",
                    serde_json::json!(zk_cluster.spec.version).as_str().unwrap()
                )),
                name: "zookeeper".to_string(),
                command: Some(vec![
                    "bin/zkServer.sh".to_string(),
                    "start-foreground".to_string(),
                    // "--config".to_string(), TODO: Version 3.4 does not support --config but later versions do
                    "{{ configroot }}/conf/zoo.cfg".to_string(), // TODO: Later versions can probably point to a directory instead, investigate
                ]),
                volume_mounts: Some(vec![
                    VolumeMount {
                        mount_path: "conf".to_string(),
                        name: "config-volume".to_string(),
                        ..VolumeMount::default()
                    },
                    // We need a second mount for the data directory
                    // because we need to write the myid file into the data directory
                    VolumeMount {
                        mount_path: "/tmp/zookeeper".to_string(),
                        name: "data-volume".to_string(),
                        ..VolumeMount::default()
                    },
                ]),
                ..Container::default()
            }],
            volumes: Some(vec![
                Volume {
                    name: "config-volume".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: Some(format!("{}-config", cm_name)),
                        ..ConfigMapVolumeSource::default()
                    }),
                    ..Volume::default()
                },
                Volume {
                    name: "data-volume".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: Some(format!("{}-data", cm_name)),
                        ..ConfigMapVolumeSource::default()
                    }),
                    ..Volume::default()
                },
            ]),
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
