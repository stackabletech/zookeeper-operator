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

const CLUSTER_NAME_LABEL: &str = "zookeeper.stackable.de/cluster-name";
const REVISION_LABEL: &str = "zookeeper.stackable.de/revision";
const ID_LABEL: &str = "zookeeper.stackable.de/id";

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
                Err(ref e) => error!("Reconcile failed: {:?}", e),
            };
        })
        .await
}

// TODO: Maybe move to operator-rs
/// This method is being called by the Controller whenever there's an error during reconciliation.
/// We just log the error and requeue the event.
fn error_policy<T>(error: &Error, _context: Context<T>) -> ReconcilerAction {
    error!("Reconciliation error:\n{}", error);
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(10)),
    }
}

// I'd like to restrict the <T> type further but that does not seem to work
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

// TODO: Move to operator-rs?
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
async fn reconcile(
    zk_cluster: ZooKeeperCluster,
    context: Context<ContextData>,
) -> Result<ReconcilerAction, Error> {
    let mut rc = ReconciliationContext::new(context.get_ref().client.clone(), zk_cluster);

    // TODO: Validate the object
    // TODO: Update the status, in case of error send an event

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

// TODO: Maybe move to operator-rs
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

// TODO: Maybe move to operator-rs
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

async fn reconcile_cluster(
    context: &mut ReconciliationContext<ZooKeeperCluster>,
) -> ReconcileResult {
    let zk_spec: ZooKeeperClusterSpec = context.resource.spec.clone();
    let zk_server_count = zk_spec.servers.len();

    // Here we check whether the current resource matches the last saved one and if not we create a new one
    let mut revisions = list_controller_revisions(&context.client, &context.resource).await?;
    sort_controller_revisions(&mut revisions);
    let last_revision = get_current_revision(&mut revisions, context).await?;
    context.current_revision = Some(last_revision);

    let existing_pods = context.list_pods().await?;

    // We first create a list of all used ids (`myid`) so we know which we can reuse
    // At the same time we create a map of id to pod for all pods which already exist
    // Later we fill those up.
    // It depends on the ZooKeeper version on whether this requires a full restart of the cluster
    // or whether this can be done dynamically.
    // This list also includes all ids currently in use by terminating or otherwise not-ready pods.
    // We never want to use those as long as there's a chance that some process might be actively
    // using it.
    // There can be a maximum of 255 (I believe) ids.
    let mut used_ids = Vec::with_capacity(zk_server_count);
    let mut node_name_to_pod = HashMap::with_capacity(zk_server_count); // This is going to own the pods
    let mut node_name_to_id = HashMap::with_capacity(zk_server_count);

    // Step 1:
    // Iterate over all existing pods and read the label which contains the `myid`
    // Also create a new HashMap that maps from node_name to pod.
    for pod in existing_pods {
        if let (
            Some(labels),
            Some(PodSpec {
                node_name: Some(node_name),
                ..
            }),
        ) = (&pod.metadata.labels, &pod.spec)
        {
            match labels.get(ID_LABEL) {
                None => {
                    error!("ZooKeeperCluster [{}/{}]: Pod [{:?}] does not have the `id` label, this is illegal, deleting it.",
                           context.namespace(),
                           context.name(),
                           pod);
                    context.client.delete(&pod).await?;
                }
                Some(label) => {
                    let id = label.parse::<usize>()?;
                    used_ids.push(id);
                    node_name_to_id.insert(node_name.clone(), id);
                }
            };
            node_name_to_pod.insert(node_name.clone(), pod);
        } else {
            error!("ZooKeeperCluster [{}/{}]: Pod [{:?}] does not have any labels, this is illegal, deleting it.",
                   context.namespace(),
                   context.name(),
                   pod);
            context.client.delete(&pod).await?;
        }
    }

    debug!(
        "ZooKeeperCluster [{}/{}]: Found these myids in use {:?}",
        context.namespace(),
        context.name(),
        used_ids
    );

    // Step 2:
    // Now that we know the current state of the cluster and its id assignment
    // we can iterate over the requested servers and assign ids to those that are missing one.
    used_ids.sort_unstable();
    for server in &zk_spec.servers {
        match node_name_to_pod.get(&server.node_name) {
            None => {
                // TODO: Need to check whether the topology has changed. If it has we need to restart all servers depending on the ZK version

                // We end up here for servers that don't have an id yet, this must (usually) mean
                // those servers have been recently added

                // This loop is used to find the first unused id
                let mut added_id = None;
                for (index, id) in used_ids.iter().enumerate() {
                    if index + 1 != *id {
                        let new_id = index + 1;
                        added_id = Some(new_id);
                        break;
                    }
                }

                let added_id = added_id.unwrap_or_else(|| used_ids.len() + 1);
                used_ids.push(added_id);
                node_name_to_id.insert(server.node_name.clone(), added_id);
            }
            Some(_) => {
                trace!("ZooKeeperCluster [{}/{}]: Pod for node [{}] already exists and is assigned id [{:?}]",
                       context.namespace(),
                       context.name(),
                       &server.node_name,
                       node_name_to_id.get(&server.node_name));
            }
        }
    }

    // Step 3:
    // Iterate over all servers from the spec and
    // * check if a pod exists for this server
    // * create one if it doesn't exist
    // * check if a pod is in the process of termination, skip the remaining reconciliation if this is the case
    // * check if a pod is up but not running/ready yet, skip the remaining reconciliation if this is the case
    for server in &zk_spec.servers {
        let pod = match node_name_to_pod.remove(&server.node_name) {
            None => {
                info!(
                    "ZooKeeperCluster [{}/{}]: Pod for server [{}] missing, creating now...",
                    context.namespace(),
                    context.name(),
                    &server.node_name
                );

                let id = node_name_to_id.remove(&server.node_name).unwrap();
                let pod = create_pod(context, &server, id).await?;

                create_config_maps(context, server, id).await?;

                pod
            }
            Some(pod) => pod,
        };

        // If the pod for this server is currently terminating (this could be for restarts or
        // upgrades) wait until it's done terminating.
        if has_deletion_stamp(&pod) {
            info!(
                "ZooKeeperCluster [{}/{}] is waiting for Pod [{}] to terminate",
                context.namespace(),
                context.name(),
                Meta::name(&pod)
            );
            return Ok(ReconcileFunctionAction::Reque(Duration::from_secs(10)));
        }

        // At the moment we'll wait for all pods to be available and ready before we might enact any changes to existing ones.
        // TODO: This should probably be configurable later
        if !podutils::is_pod_running_and_ready(&pod) {
            info!(
                "ZooKeeperCluster [{}/{}] is waiting for Pod [{}] to be running and ready",
                context.namespace(),
                context.name(),
                Meta::name(&pod)
            );
            return Ok(ReconcileFunctionAction::Reque(Duration::from_secs(10)));
        }

        // Check if the pod is up-to-date
        let container = pod.spec.as_ref().unwrap().containers.get(0).unwrap();
        match &container.image {
            Some(image) if image != &zk_spec.image_name() => {}
            _ => {}
        }
        // TODO: Rust Experts
        if container.image != Some(zk_spec.image_name()) {
            info!(
                "ZooKeeperCluster [{}/{}]: Image for pod [{}] differs [{:?}] (from container) != [{:?}] (from current spec), deleting old pod",
                context.namespace(),
                context.name(),
                Meta::name(&pod),
                container.image,
                zk_spec.image_name()
            );
            context.client.delete(&pod).await?;
        }
    }

    for (node_name, pod) in node_name_to_pod {
        // TODO: This might already be in the terminating state because earlier we only iterate over the spec servers and not the pods
        info!(
            "ZooKeeperCluster [{}/{}] has extra Pod [{}] for node [{}]: Terminating",
            context.namespace(),
            context.name(),
            Meta::name(&pod),
            node_name
        );

        context.client.delete(&pod).await?;
    }

    Ok(ReconcileFunctionAction::Continue)
}

// TODO: Move to operator-rs?
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
    id: usize,
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
    data.insert("myid".to_string(), id.to_string());
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
    id: usize,
) -> Result<Pod, Error> {
    let pod = build_pod(context, zk_server, id)?;
    Ok(context.client.create(&pod).await?)
}

fn build_pod(
    context: &ReconciliationContext<ZooKeeperCluster>,
    zk_server: &ZooKeeperServer,
    id: usize,
) -> Result<Pod, Error> {
    Ok(Pod {
        metadata: build_pod_metadata(context, zk_server, id)?,
        spec: Some(build_pod_spec(context, zk_server)),
        ..Pod::default()
    })
}

fn build_pod_metadata(
    context: &ReconciliationContext<ZooKeeperCluster>,
    zk_server: &ZooKeeperServer,
    id: usize,
) -> Result<ObjectMeta, Error> {
    Ok(ObjectMeta {
        labels: Some(build_labels(context, id)?),
        name: Some(get_pod_name(context, zk_server)),
        namespace: Some(context.namespace()),
        owner_references: Some(vec![object_to_owner_reference::<ZooKeeperCluster>(
            context.metadata(),
        )?]),
        ..ObjectMeta::default()
    })
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
    id: usize,
) -> Result<BTreeMap<String, String>, error::Error> {
    let mut labels = BTreeMap::new();
    labels.insert(CLUSTER_NAME_LABEL.to_string(), context.name());
    labels.insert(
        REVISION_LABEL.to_string(),
        context
            .current_revision
            .clone()
            .ok_or(error::Error::MissingControllerRevision)?
            .revision
            .to_string(),
    );
    labels.insert(ID_LABEL.to_string(), id.to_string());

    Ok(labels)
}

/// All pod names follow a simple pattern: <name of ZooKeeperCluster object>-<Node name>
fn get_pod_name(
    context: &ReconciliationContext<ZooKeeperCluster>,
    zk_server: &ZooKeeperServer,
) -> String {
    format!("{}-{}", context.name(), zk_server.node_name)
}
