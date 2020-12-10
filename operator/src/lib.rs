#![feature(backtrace)]
mod error;

use crate::error::Error;

use kube::Api;
use tracing::{debug, error, info, trace};

use futures::future::BoxFuture;
use futures::StreamExt;
use handlebars::Handlebars;
use k8s_openapi::api::apps::v1::ControllerRevision;
use k8s_openapi::api::core::v1::{
    ConfigMapVolumeSource, Container, Pod, PodSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::runtime::RawExtension;
use kube::api::{ListParams, Meta, ObjectMeta};
use kube_runtime::controller::{Context, ReconcilerAction};
use kube_runtime::Controller;
use serde_json::json;
use stackable_operator::client::Client;
use stackable_operator::finalizer::has_deletion_stamp;
use stackable_operator::history::{
    create_controller_revision, list_controller_revisions, next_revision, sort_controller_revisions,
};
use stackable_operator::{
    create_config_map, create_tolerations, finalizer, object_to_owner_reference, podutils,
    ContextData,
};
use stackable_zookeeper_crd::{ZooKeeperCluster, ZooKeeperClusterSpec, ZooKeeperServer};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

const FINALIZER_NAME: &str = "zookeeper.stackable.de/cleanup";

const REVISION_LABEL: &str = "zookeeper.stackable.de/revision";

/// This creates an instance of a [`Controller`] which waits for incoming events and reconciles them.
///
/// This is an async method and the returned future needs to be consumed to make progress.
pub async fn create_controller(client: Client) {
    let zk_api: Api<ZooKeeperCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let context = Context::new(ContextData::new(client));

    Controller::new(zk_api, ListParams::default())
        .owns(pods_api, ListParams::default()) // TODO: Restrict to owner references
        // TODO: .owns(ConfigMaps...)
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconciled {:?}", o),
                Err(e) => info!("Reconcile failed: {}", e),
            };
        })
        .await
}

// https://github.com/rust-lang/rust/issues/21903
//where
//    T: k8s_openapi::Resource + Clone + Meta + DeserializeOwned,
type ReconcileFunction<T> = fn(&mut ReconciliationContext<T>) -> BoxFuture<'_, ReconcileResult>;

type ReconcileResult = std::result::Result<ReconcileFunctionAction, error::Error>;

enum ReconcileFunctionAction {
    Continue,
    Done,
    Reque(Duration),
}

struct ReconciliationContext<T> {
    client: Client,
    pub resource: T,
    pub current_revision: Option<ControllerRevision>,
}

impl<T> ReconciliationContext<T> {
    pub fn new(client: Client, resource: T) -> Self {
        ReconciliationContext {
            client,
            resource,
            current_revision: None,
        }
    }
}

impl<T> ReconciliationContext<T>
where
    T: Meta,
{
    pub fn name(&self) -> String {
        Meta::name(&self.resource)
    }

    pub fn namespace(&self) -> String {
        Meta::namespace(&self.resource).expect("Resources are namespaced")
    }

    pub fn metadata(&self) -> ObjectMeta {
        self.resource.meta().clone()
    }

    pub async fn list_pods(&self) -> Result<Vec<Pod>, error::Error> {
        let api = self.client.get_namespaced_api(&self.namespace());

        // TODO: We need to use a label selector to only get _our_ pods
        // It'd be ideal if we could filter by ownerReferences but that's not possible in K8S today
        // so we apply a custom label to each pod
        let list_params = ListParams {
            label_selector: None,
            ..ListParams::default()
        };

        api.list(&list_params)
            .await
            .map_err(Error::from)
            .map(|result| result.items)
    }
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
    let mut rc = ReconciliationContext::new(context.get_ref().client.clone(), zk_cluster);

    // https://www.openshift.com/blog/kubernetes-operators-best-practices
    // 1. Validate the object if needed
    // 2. Initialize values that might be missing to their defaults/calculated values
    // 3. Manage deletion
    // 4. Normal logic

    // Update the status
    // In case of error send an event

    let reconcilers: Vec<ReconcileFunction<ZooKeeperCluster>> = vec![
        |rc| Box::pin(handle_deletion(rc)),
        |rc| Box::pin(add_finalizer(rc)),
        |rc| Box::pin(reconcile_cluster(rc)),
    ];

    for reconciler in reconcilers {
        match reconciler(&mut rc).await {
            Ok(ReconcileFunctionAction::Continue) => {
                trace!("Reconciler loop: Continue")
            }
            Ok(ReconcileFunctionAction::Done) => {
                trace!("Reconciler loop: Done");
                break;
            }
            Ok(ReconcileFunctionAction::Reque(duration)) => {
                trace!(?duration, "Reconciler loop: Requeue");
                return Ok(ReconcilerAction {
                    requeue_after: Some(duration),
                });
            }
            Err(err) => {
                error!(?err, "Error reconciling");
                return Ok(ReconcilerAction {
                    requeue_after: Some(Duration::from_secs(30)),
                });
            }
        }
    }

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

async fn handle_deletion(context: &ReconciliationContext<ZooKeeperCluster>) -> ReconcileResult {
    trace!(
        "Reconciler [handle_deletion] for [{}]",
        Meta::name(&context.resource)
    );
    if !finalizer::has_deletion_stamp(&context.resource) {
        debug!(
            "[handle_deletion] for [{}]: Not deleted, continuing...",
            Meta::name(&context.resource)
        );
        return Ok(ReconcileFunctionAction::Continue);
    }

    info!(
        "Deleting ZooKeeperCluster [{}/{}]",
        context.namespace(),
        context.name()
    );
    info!("{:?}", &context.resource);
    finalizer::remove_finalizer(context.client.clone(), &context.resource, FINALIZER_NAME).await?;

    Ok(ReconcileFunctionAction::Done)
}

async fn add_finalizer(context: &ReconciliationContext<ZooKeeperCluster>) -> ReconcileResult {
    trace!(
        resource = ?context.resource,
        "Reconciler [add_finalizer] for [{}]",
        Meta::name(&context.resource)
    );

    if finalizer::has_finalizer(&context.resource, FINALIZER_NAME) {
        debug!(
            "[add_finalizer] for [{}]: Finalizer already exists, continuing...",
            Meta::name(&context.resource)
        );
        Ok(ReconcileFunctionAction::Continue)
    } else {
        debug!(
            "[add_finalizer] for [{}]: Finalizer missing, adding now and continuing...",
            Meta::name(&context.resource)
        );
        finalizer::add_finalizer(context.client.clone(), &context.resource, FINALIZER_NAME).await?;

        Ok(ReconcileFunctionAction::Continue)
    }
}

/// This method is being called by the Controller whenever there's an error during reconcilation.
/// We just log the error and requeue the event.
fn error_policy(error: &Error, _context: Context<ContextData>) -> ReconcilerAction {
    error!("Reconciliation error:\n{}", error);
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(10)),
    }
}

async fn reconcile_cluster(
    context: &mut ReconciliationContext<ZooKeeperCluster>,
) -> ReconcileResult {
    let zk_spec: ZooKeeperClusterSpec = context.resource.spec.clone();

    let mut revisions = list_controller_revisions(&context.client, &context.resource).await?;
    sort_controller_revisions(&mut revisions);
    let last_revision = get_current_revision(&mut revisions, context).await?;
    context.current_revision = Some(last_revision);

    // This is a map from pod name to pod object
    let mut pods: HashMap<String, Pod> = context
        .list_pods()
        .await?
        .into_iter()
        .map(|pod| (pod.metadata.name.clone().unwrap(), pod))
        .collect();

    for server in zk_spec.servers.iter() {
        let pod_name = get_pod_name(context, server);
        if !stackable_operator::podutils::is_pod_created(pods.get(&pod_name)) {
            info!(
                "ZooKeeperCluster [{}/{}]: Pod [{}] missing, creating now...",
                context.namespace(),
                context.name(),
                &pod_name
            );
            let created_pod = create_pod(context, &server).await?;
            create_config_maps(context, server).await?;
            pods.insert(pod_name.clone(), created_pod);
        };

        let pod = pods.get(&pod_name).ok_or(error::Error::MissingPod {
            pod_name: pod_name.to_string(),
        })?;

        if has_deletion_stamp(pod) {
            info!(
                "ZooKeeperCluster [{}/{}] is waiting for Pod [{}] to terminate",
                context.namespace(),
                context.name(),
                Meta::name(pod)
            );
            return Ok(ReconcileFunctionAction::Reque(Duration::from_secs(10)));
        }

        // At the moment we'll wait for all pods to be available and ready before we might enact any changes to existing ones.
        // TODO: This should probably be configurable later
        /*
        if !podutils::is_pod_running_and_ready(pod) {
            info!(
                "ZooKeeperCluster [{}/{}] is waiting for Pod [{}] to be running and ready",
                context.namespace(),
                context.name(),
                Meta::name(pod)
            );
            return Ok(ReconcileFunctionAction::Reque(Duration::from_secs(10)));
        }

         */
    }

    for (node_name, pod) in pods.iter() {
        // Check if Pod version is still up-to-date with the ZooKeeperCluster spec
        if let Some(labels) = &pod.metadata.labels {
            if let Some(label) = labels.get(REVISION_LABEL) {
                if label
                    != &context
                        .current_revision
                        .as_ref()
                        .ok_or(error::Error::MissingControllerRevision)?
                        .revision
                        .to_string()
                {
                    info!(
                        "ZooKeeperCluster [{}/{}]: Pod [{}] is outdated, terminating/deleting...",
                        context.namespace(),
                        context.name(),
                        pod.metadata.name.as_ref().unwrap_or(&"NO NAME".to_string())
                    );
                    context.client.delete(pod).await;
                } else {
                    println!("!!!!!!!!! Pod up to date !!!!!!")
                }
            } else {
                // Label is missing: Delete and recreate! It should always have some labels
            }
        } else {
            // Label is missing: Delete and recreate! It should always have some labels
        }
    }

    // TODO: Also iterate over all pods to see if there are extraneous ones
    Ok(ReconcileFunctionAction::Continue)
}

async fn get_current_revision<'a>(
    revisions: &mut Vec<ControllerRevision>,
    context: &ReconciliationContext<ZooKeeperCluster>,
) -> Result<ControllerRevision, Error> {
    // If there are no revisions at all so far we definitely need to create a new one
    if revisions.is_empty() {
        info!(
            "ZooKeeperCluster [{}/{}]: No ControllerRevision, creating first one",
            context.namespace(),
            context.name()
        );

        // We do not want to serialize the `status` part so we only serialize the spec
        // TODO: StatefulSets create patch instead which might make more sense here as well
        //       See pkg/controller/statefulset/stateful_set_utils.go # newRevision for details
        let revision = create_controller_revision(
            &context.client,
            &context.resource,
            RawExtension(serde_json::to_value(&context.resource.spec).unwrap()),
            next_revision(&revisions),
        )
        .await?;

        revisions.insert(0, revision);
    }

    // As we just made sure to create a revision if none exists this should never error
    let mut revision = revisions
        .first()
        .ok_or(error::Error::MissingControllerRevision)?
        .clone();

    let revision_spec: ZooKeeperClusterSpec = serde_json::from_value(
        revision
            .data
            .clone()
            .ok_or(error::Error::MissingControllerRevision)?
            .0,
    )?;

    if revision_spec != context.resource.spec {
        info!(
            "ZooKeeperCluster [{}/{}]: Last ControllerRevision [{}] does not match current spec, creating new one",
            context.namespace(),
            context.name(),
            revision.revision
        );

        revision = create_controller_revision(
            &context.client,
            &context.resource,
            RawExtension(serde_json::to_value(&context.resource.spec).unwrap()),
            next_revision(&revisions),
        )
        .await?;
    }

    Ok(revision)
}

async fn create_config_maps(
    context: &ReconciliationContext<ZooKeeperCluster>,
    zk_server: &ZooKeeperServer,
) -> Result<(), Error> {
    let mut options = HashMap::new();
    options.insert("tickTime".to_string(), "2000".to_string());
    options.insert("dataDir".to_string(), "/tmp/zookeeper".to_string());
    options.insert("initLimit".to_string(), "5".to_string());
    options.insert("syncLimit".to_string(), "2".to_string());
    options.insert("clientPort".to_string(), "2181".to_string());

    for (i, server) in context.resource.spec.servers.iter().enumerate() {
        options.insert(
            format!("server.{}", i + 1),
            format!("{}:2888:3888", server.node_name),
        );
    }

    let mut handlebars = Handlebars::new();
    handlebars
        .register_template_string("conf", "{{#each options}}{{@key}}={{this}}\n{{/each}}")
        .expect("template should work");

    let config = handlebars
        .render("conf", &json!({ "options": options }))
        .unwrap();

    // Now we need to create two configmaps per server.
    // The names are "zk-<cluster name>-<node name>-config" and "zk-<cluster name>-<node name>-data"
    // One for the configuration directory...
    let mut data = BTreeMap::new();
    data.insert("zoo.cfg".to_string(), config);

    let cm_name_prefix = format!("zk-{}", get_pod_name(context, zk_server));
    let cm_name = format!("{}-config", cm_name_prefix);
    let cm = create_config_map(&context.resource, &cm_name, data)?;
    context
        .client
        .apply_patch(&cm, serde_json::to_vec(&cm)?)
        .await?;

    // ...and one for the data directory (which only contains the myid file)
    let mut data = BTreeMap::new();
    //data.insert("myid".to_string(), (i + 1).to_string());
    let cm_name = format!("{}-data", cm_name_prefix);
    let cm = create_config_map(&context.resource, &cm_name, data)?;
    context
        .client
        .apply_patch(&cm, serde_json::to_vec(&cm)?)
        .await?;
    Ok(())
}

async fn create_pod(
    context: &ReconciliationContext<ZooKeeperCluster>,
    zk_server: &ZooKeeperServer,
) -> Result<Pod, Error> {
    let pod = build_pod(context, zk_server)?;
    Ok(context.client.create(&pod).await?)
}

fn build_pod(
    context: &ReconciliationContext<ZooKeeperCluster>,
    zk_server: &ZooKeeperServer,
) -> Result<Pod, Error> {
    let mut pod = Pod::default();

    pod.metadata = build_pod_metadata(context, zk_server)?;
    pod.spec = Some(build_pod_spec(context, zk_server));
    Ok(pod)
}

fn build_pod_metadata(
    context: &ReconciliationContext<ZooKeeperCluster>,
    zk_server: &ZooKeeperServer,
) -> Result<ObjectMeta, Error> {
    Ok(ObjectMeta {
        labels: Some(build_labels(context)?),
        name: Some(get_pod_name(context, zk_server)),
        namespace: Some(context.namespace()),
        owner_references: Some(vec![object_to_owner_reference::<ZooKeeperCluster>(
            context.metadata(),
        )?]),
        ..ObjectMeta::default()
    })
    //TODO: set_owner_references::<Pod, ZooKeeperCluster>(zk_cluster.meta().clone(), &mut pod)?;
}

fn build_pod_spec(
    context: &ReconciliationContext<ZooKeeperCluster>,
    zk_server: &ZooKeeperServer,
) -> PodSpec {
    let (containers, volumes) = build_containers(context, zk_server);

    PodSpec {
        node_name: Some(zk_server.node_name.clone()),
        tolerations: Some(create_tolerations()),
        containers,
        volumes: Some(volumes),
        /*
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
         */
        ..PodSpec::default()
    }
}

fn build_containers(
    context: &ReconciliationContext<ZooKeeperCluster>,
    zk_server: &ZooKeeperServer,
) -> (Vec<Container>, Vec<Volume>) {
    let version = context.resource.spec.version.clone();
    let image_name = format!(
        "stackable/zookeeper:{}",
        serde_json::json!(version).as_str().unwrap()
    );

    let containers = vec![Container {
        image: Some(image_name),
        name: "zookeeper".to_string(),
        command: Some(vec![
            "bin/zkServer.sh".to_string(),
            "start-foreground".to_string(),
            // "--config".to_string(), TODO: Version 3.4 does not support --config but later versions do
            "{{ configroot }}/conf/zoo.cfg".to_string(), // TODO: Later versions can probably point to a directory instead, investigate
        ]),
        volume_mounts: Some(vec![
            // One mount for the config directory, this will be relative to the extracted package
            VolumeMount {
                mount_path: "conf".to_string(),
                name: "config-volume".to_string(),
                ..VolumeMount::default()
            },
            // We need a second mount for the data directory
            // because we need to write the myid file into the data directory
            VolumeMount {
                mount_path: "/tmp/zookeeper".to_string(), // TODO: Make configurable
                name: "data-volume".to_string(),
                ..VolumeMount::default()
            },
        ]),
        ..Container::default()
    }];

    let cm_name_prefix = format!("zk-{}", get_pod_name(context, zk_server));
    let volumes = vec![
        Volume {
            name: "config-volume".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(format!("{}-config", cm_name_prefix)),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        },
        Volume {
            name: "data-volume".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(format!("{}-data", cm_name_prefix)),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        },
    ];

    (containers, volumes)
}

fn build_labels(
    context: &ReconciliationContext<ZooKeeperCluster>,
) -> Result<BTreeMap<String, String>, error::Error> {
    let mut labels = BTreeMap::new();
    labels.insert(
        "zookeeper.stackable.de/cluster-name".to_string(),
        context.name(),
    );
    labels.insert(
        REVISION_LABEL.to_string(),
        context
            .current_revision
            .clone()
            .ok_or(error::Error::MissingControllerRevision)?
            .revision
            .to_string(),
    );
    Ok(labels)
}

/// All pod names follow a simple pattern: <name of ZooKeeperCluster object>-<Node name>
fn get_pod_name(
    context: &ReconciliationContext<ZooKeeperCluster>,
    zk_server: &ZooKeeperServer,
) -> String {
    format!("{}-{}", context.name(), zk_server.node_name)
}
