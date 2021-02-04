#![feature(backtrace)]
mod error;

use crate::error::Error;

use kube::Api;
use tracing::{debug, error, info, trace};

use handlebars::Handlebars;
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, Pod, PodSpec, Volume, VolumeMount,
};
use kube::api::{ListParams, Meta};
use serde_json::json;

use stackable_operator::client::Client;
use stackable_operator::controller::Controller;
use stackable_operator::controller::{ControllerStrategy, ReconciliationState};
use stackable_operator::reconcile::{
    ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::{create_config_map, finalizer, metadata, podutils, reconcile};
use stackable_zookeeper_crd::{ZooKeeperCluster, ZooKeeperClusterSpec, ZooKeeperServer};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::time::Duration;
use tokio::macros::support::Future;

const FINALIZER_NAME: &str = "zookeeper.stackable.de/cleanup";

const CLUSTER_NAME_LABEL: &str = "zookeeper.stackable.de/cluster-name";
const ID_LABEL: &str = "zookeeper.stackable.de/id";

type ZooKeeperReconcileResult = ReconcileResult<error::Error>;

struct ZooKeeperState {
    context: ReconciliationContext<ZooKeeperCluster>,
    zk_spec: ZooKeeperClusterSpec,
    id_information: Option<IdInformation>,
}

struct IdInformation {
    used_ids: Vec<usize>,
    node_name_to_pod: HashMap<String, Pod>,
    node_name_to_id: HashMap<String, usize>,
}

impl IdInformation {
    fn new(
        used_ids: Vec<usize>,
        node_name_to_pod: HashMap<String, Pod>,
        node_name_to_id: HashMap<String, usize>,
    ) -> IdInformation {
        IdInformation {
            used_ids,
            node_name_to_pod,
            node_name_to_id,
        }
    }
}

/// This finds the first missing number in a sorted vector.
/// Zero is not a valid input in the vector.
/// If you pass in zero the result will be undefined.
fn find_first_missing(vec: &[usize]) -> usize {
    let mut added_id = None;
    for (index, id) in vec.iter().enumerate() {
        if index + 1 != *id {
            let new_id = index + 1;
            added_id = Some(new_id);
            break;
        }
    }

    // Either we found an unused id above (which would be a "hole" between existing ones, e.g. 1,2,4 could find "3")
    // or we need to create a new one which would be the number of used ids (because we "plug" holes first) plus one.
    added_id.unwrap_or_else(|| vec.len() + 1)
}

impl ZooKeeperState {
    // This looks at all currently existing Pods for the current ZooKeeperCluster object.
    // It checks if all the pods are valid (i.e. contain required labels) and then builds an `IdInformation`
    // object and sets it on the current state.
    async fn read_existing_pod_information(&mut self) -> ZooKeeperReconcileResult {
        trace!(
            "Reading existing pod information for {}",
            self.context.log_name()
        );

        let existing_pods = self.context.list_pods().await?;
        trace!(
            "{}: Found [{}] pods",
            self.context.log_name(),
            existing_pods.len()
        );

        let zk_server_count = self.zk_spec.servers.len();

        // We first create a list of all used ids (`myid`) so we know which we can reuse
        // At the same time we create a map of id to pod for all pods which already exist
        // Later we fill those up.
        // It depends on the ZooKeeper version on whether this requires a full restart of the cluster
        // or whether this can be done dynamically.
        // This list also includes all ids currently in use by terminating or otherwise not-ready pods.
        // We never want to use those as long as there's a chance that some process might be actively
        // using it.
        // There can be a maximum of 255 (I believe) ids.
        let mut used_ids = Vec::with_capacity(existing_pods.len());
        let mut node_name_to_pod = HashMap::with_capacity(zk_server_count); // This is going to own the pods
        let mut node_name_to_id = HashMap::with_capacity(zk_server_count);

        // Iterate over all existing pods and read the label which contains the `myid`
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
                        error!("ZooKeeperCluster {}: Pod [{:?}] does not have the `id` label, this is illegal, deleting it.",
                               self.context.log_name(),
                               pod);
                        self.context.client.delete(&pod).await?;
                    }
                    Some(label) => {
                        let id = match label.parse::<usize>() {
                            Ok(id) => id,
                            Err(_) => {
                                error!("ZooKeeperCluster {}: Pod [{:?}] does have the `id` label but the label ([{}]) cannot be parsed, this is illegal, deleting the pod.",
                                       self.context.log_name(), pod, label);
                                self.context.client.delete(&pod).await?;
                                continue;
                            }
                        };

                        // Check if we have seen the same id before
                        // This should never happen and would currently require manual cleanup
                        if used_ids.contains(&id) {
                            // TODO: Update status
                            error!(
                                "{}: Found a duplicate `myid` [{}] in Pod [{}], we can't recover\
                                 from this error and you need to clean up manually",
                                self.context.log_name(),
                                id,
                                Meta::name(&pod)
                            );
                            return Err(Error::ReconcileError("Found duplicate id".to_string()));
                        }

                        used_ids.push(id);
                        node_name_to_id.insert(node_name.clone(), id);
                        node_name_to_pod.insert(node_name.clone(), pod);
                    }
                };
            } else {
                error!("ZooKeeperCluster {}: Pod [{:?}] does not have any spec or labels, this is illegal, deleting it.",
                       self.context.log_name(),
                       pod);
                self.context.client.delete(&pod).await?;
            }
        }

        debug!(
            "ZooKeeperCluster {}: Found these myids in use [{:?}]",
            self.context.log_name(),
            used_ids
        );

        let id_information = IdInformation::new(used_ids, node_name_to_pod, node_name_to_id);
        self.id_information = Some(id_information);

        Ok(ReconcileFunctionAction::Continue)
    }

    /// This function looks at all the requested servers from the spec and assigns ids to those
    /// that don't have one yet.
    /// We do this here - and not later - because we need the id mapping information for the
    /// ConfigMap generation later.
    /// NOTE: This method will _not_ work if multiple servers should run on a single node
    async fn assign_ids(&mut self) -> ZooKeeperReconcileResult {
        trace!(
            "{}: Assigning ids to new servers from the spec",
            self.context.log_name()
        );

        let id_information = self.id_information.as_mut().ok_or(error::Error::ReconcileError(
                        "id_information missing, this is a programming error and should never happen. Please report in our issue tracker.".to_string(),
                    ))?;

        // We iterate over all servers from the spec and check if we have a pod assigned to this server.
        // If not we find the next unused one and assign that.
        id_information.used_ids.sort_unstable();
        for server in &self.zk_spec.servers {
            match id_information.node_name_to_pod.get(&server.node_name) {
                None => {
                    // TODO: Need to check whether the topology has changed. If it has we need to restart all servers depending on the ZK version

                    let new_id = find_first_missing(&id_information.used_ids);

                    id_information.used_ids.push(new_id);
                    id_information.used_ids.sort_unstable();
                    id_information
                        .node_name_to_id
                        .insert(server.node_name.clone(), new_id);

                    info!(
                        "{}: Assigning new id [{}] to server/node [{}]",
                        self.context.log_name(),
                        new_id,
                        server.node_name
                    )
                }
                Some(_) => {
                    trace!("ZooKeeperCluster {}: Pod for node [{}] already exists and is assigned id [{:?}]",
                           self.context.log_name(),
                           &server.node_name,
                           id_information.node_name_to_id.get(&server.node_name));
                }
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    pub async fn reconcile_cluster(&mut self) -> ZooKeeperReconcileResult {
        trace!("{}: Starting reconciliation", self.context.log_name());

        let id_information = self.id_information.as_mut().ok_or(error::Error::ReconcileError(
                        "id_information missing, this is a programming error and should never happen. Please report in our issue tracker.".to_string(),
                    ))?;

        // Iterate over all servers from the spec and
        // * check if a pod exists for this server
        // * create one if it doesn't exist
        // * check if a pod is in the process of termination, skip the remaining reconciliation if this is the case
        // * check if a pod is up but not running/ready yet, skip the remaining reconciliation if this is the case
        // TODO: Need to deal with crashed workers/pods. They shouldn't block all other actions.
        for server in &self.zk_spec.servers {
            let pod = match id_information.node_name_to_pod.remove(&server.node_name) {
                None => {
                    info!(
                        "{}: Pod for server [{}] missing, creating now...",
                        self.context.log_name(),
                        &server.node_name
                    );

                    let id = id_information
                        .node_name_to_id
                        .remove(&server.node_name)
                        .ok_or(Error::ReconcileError(format!("We didn't find a `myid` for [{}] but it should have been assigned, this is a bug, please report it", server.node_name)))?;

                    self.create_pod(&server, id).await?;
                    self.create_config_maps(server, id).await?;

                    return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
                }
                Some(pod) => pod,
            };

            // If the pod for this server is currently terminating (this could be for restarts or
            // upgrades) wait until it's done terminating.
            if finalizer::has_deletion_stamp(&pod) {
                info!(
                    "ZooKeeperCluster {} is waiting for Pod [{}] to terminate",
                    self.context.log_name(),
                    Meta::name(&pod)
                );
                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }

            // At the moment we'll wait for all pods to be available and ready before we might enact any changes to existing ones.
            // TODO: Only do this next check if we want "rolling" functionality
            if !podutils::is_pod_running_and_ready(&pod) {
                info!(
                    "ZooKeeperCluster {} is waiting for Pod [{}] to be running and ready",
                    self.context.log_name(),
                    Meta::name(&pod)
                );
                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }

            // Check if the pod is up-to-date
            let container = pod.spec.as_ref().unwrap().containers.get(0).unwrap();
            if container.image != Some(self.zk_spec.image_name()) {
                info!(
                    "{}: Image for pod [{}] differs [{:?}] (from container) != [{:?}] (from current spec), deleting old pod",
                    self.context.log_name(),
                    Meta::name(&pod),
                    container.image,
                    self.zk_spec.image_name()
                );
                self.context.client.delete(&pod).await?;
                return Ok(reconcile::create_requeuing_reconcile_function_action(10));
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    pub async fn delete_excess_pods(&self) -> ZooKeeperReconcileResult {
        trace!(
            "{}: Starting to delete excess pods",
            self.context.log_name()
        );
        let id_information = self.id_information.as_ref().ok_or(error::Error::ReconcileError(
                        "id_information missing, this is a programming error and should never happen. Please report in our issue tracker.".to_string(),
                    ))?.clone();

        // This goes through all remaining pods in the Map.
        // Because we delete all pods we "need" in the previous loop this will only have pods that are
        // left over (maybe because of a scale down) and can be deleted.
        for (node_name, pod) in &id_information.node_name_to_pod {
            // TODO: This might already be in the terminating state because earlier we only iterate over the spec servers and not the pods
            info!(
                "ZooKeeperCluster {} has extra Pod [{}] for node [{}]: Terminating",
                self.context.log_name(),
                Meta::name(pod),
                node_name
            );

            // We don't trigger a reconcile requeue here because there should be nothing for us to do
            // in the next loop
            self.context.client.delete(pod).await?;
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    async fn create_config_maps(
        &self,
        zk_server: &ZooKeeperServer,
        id: usize,
    ) -> Result<(), Error> {
        let mut options = HashMap::new();
        options.insert("tickTime".to_string(), "2000".to_string());
        options.insert("dataDir".to_string(), "/tmp/zookeeper".to_string());
        options.insert("initLimit".to_string(), "5".to_string());
        options.insert("syncLimit".to_string(), "2".to_string());
        options.insert("clientPort".to_string(), "2181".to_string());

        // This builds the server string
        // TODO: Does this need to use myid?
        for (i, server) in self.context.resource.spec.servers.iter().enumerate() {
            options.insert(
                format!("server.{}", i + 1),
                format!("{}:2888:3888", server.node_name),
            );
        }

        let mut handlebars = Handlebars::new();
        handlebars.set_strict_mode(true);
        handlebars
            .register_template_string("conf", "{{#each options}}{{@key}}={{this}}\n{{/each}}")
            .expect("Failure rendering the ZooKeeper config template, this should not happen, please report this issue");

        let config = handlebars
            .render("conf", &json!({ "options": options }))
            .expect("Failure rendering the ZooKeeper config template, this should not happen, please report this issue");

        // Now we need to create two configmaps per server.
        // The names are "zk-<cluster name>-<node name>-config" and "zk-<cluster name>-<node name>-data"
        // One for the configuration directory...
        let mut data = BTreeMap::new();
        data.insert("zoo.cfg".to_string(), config);

        let cm_name_prefix = format!("zk-{}", self.get_pod_name(zk_server));
        let cm_name = format!("{}-config", cm_name_prefix);
        let cm = create_config_map(&self.context.resource, &cm_name, data)?;
        self.context
            .client
            .apply_patch(&cm, serde_json::to_vec(&cm)?)
            .await?;

        // ...and one for the data directory (which only contains the myid file)
        let mut data = BTreeMap::new();
        data.insert("myid".to_string(), id.to_string());
        let cm_name = format!("{}-data", cm_name_prefix);
        let cm = create_config_map(&self.context.resource, &cm_name, data)?;
        self.context
            .client
            .apply_patch(&cm, serde_json::to_vec(&cm)?)
            .await?;
        Ok(())
    }

    async fn create_pod(&self, zk_server: &ZooKeeperServer, id: usize) -> Result<Pod, Error> {
        let pod = self.build_pod(zk_server, id)?;
        Ok(self.context.client.create(&pod).await?)
    }

    fn build_pod(&self, zk_server: &ZooKeeperServer, id: usize) -> Result<Pod, Error> {
        let (containers, volumes) = self.build_containers(zk_server);

        Ok(Pod {
            metadata: metadata::build_metadata(
                Some(self.build_labels(id)?),
                &self.context.resource,
            )?,
            spec: Some(PodSpec {
                node_name: Some(zk_server.node_name.clone()),
                tolerations: Some(stackable_operator::create_tolerations()),
                containers,
                volumes: Some(volumes),
                ..PodSpec::default()
            }),

            ..Pod::default()
        })
    }

    fn build_containers(&self, zk_server: &ZooKeeperServer) -> (Vec<Container>, Vec<Volume>) {
        let version = self.context.resource.spec.version.clone();
        let image_name = format!(
            "stackable/zookeeper:{}",
            serde_json::json!(version).as_str().expect("This should not fail as it comes from an enum, if this fails please file a bug report")
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

        let cm_name_prefix = format!("zk-{}", self.get_pod_name(zk_server));
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

    fn build_labels(&self, id: usize) -> Result<BTreeMap<String, String>, error::Error> {
        let mut labels = BTreeMap::new();
        labels.insert(CLUSTER_NAME_LABEL.to_string(), self.context.name());
        labels.insert(ID_LABEL.to_string(), id.to_string());

        Ok(labels)
    }

    /// All pod names follow a simple pattern: <name of ZooKeeperCluster object>-<Node name>
    fn get_pod_name(&self, zk_server: &ZooKeeperServer) -> String {
        format!("{}-{}", self.context.name(), zk_server.node_name)
    }
}

impl ReconciliationState for ZooKeeperState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        Box::pin(async move {
            self.read_existing_pod_information()
                .await?
                .then(self.assign_ids())
                .await?
                .then(self.reconcile_cluster())
                .await?
                .then(self.delete_excess_pods())
                .await
        })
    }
}

#[derive(Debug)]
struct ZooKeeperStrategy {}

impl ZooKeeperStrategy {
    pub fn new() -> ZooKeeperStrategy {
        ZooKeeperStrategy {}
    }
}

impl ControllerStrategy for ZooKeeperStrategy {
    type Item = ZooKeeperCluster;
    type State = ZooKeeperState;

    fn finalizer_name(&self) -> String {
        return FINALIZER_NAME.to_string();
    }

    fn init_reconcile_state(&self, context: ReconciliationContext<Self::Item>) -> Self::State {
        ZooKeeperState {
            zk_spec: context.resource.spec.clone(),
            context,
            id_information: None,
        }
    }
}

/// This creates an instance of a [`Controller`] which waits for incoming events and reconciles them.
///
/// This is an async method and the returned future needs to be consumed to make progress.
pub async fn create_controller(client: Client) {
    let zk_api: Api<ZooKeeperCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();

    let controller = Controller::new(zk_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default());

    let strategy = ZooKeeperStrategy::new();

    controller.run(client, strategy).await;
}

#[cfg(test)]
mod tests {

    use super::*;
    use rstest::rstest;

    #[rstest(input, expected,
        case(vec![1, 2, 3], 4),
        case(vec![1, 3, 5], 2),
        case(vec![], 1),
        case(vec![3, 4, 6], 1),
        case(vec![1], 2),
        case(vec![2], 1),
    )]
    fn test_first_missing(input: Vec<usize>, expected: usize) {
        let first = find_first_missing(&input);
        assert_eq!(first, expected);
    }
}
