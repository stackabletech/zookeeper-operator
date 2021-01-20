#![feature(backtrace)]
mod error;

use crate::error::Error;

use kube::Api;
use tracing::{debug, error, info, trace};

use handlebars::Handlebars;
use k8s_openapi::api::apps::v1::ControllerRevision;
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, Pod, PodSpec, Volume, VolumeMount,
};
use kube::api::{ListParams, Meta, ObjectMeta};
use serde_json::json;
use stackable_operator::client::Client;
use stackable_operator::controller::ControllerStrategy;
use stackable_operator::finalizer::has_deletion_stamp;
use stackable_operator::reconcile::{
    create_requeuing_reconcile_function_action, ReconcileFunctionAction, ReconcileResult,
    ReconciliationContext,
};
use stackable_operator::{
    create_config_map, create_tolerations, object_to_owner_reference, podutils,
};
use stackable_zookeeper_crd::{ZooKeeperCluster, ZooKeeperClusterSpec, ZooKeeperServer};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;
use tokio::macros::support::Future;

const GROUP_NAME: &str = "zookeeper.stackable.de";
const FINALIZER_NAME: &str = "zookeeper.stackable.de/cleanup";

const CLUSTER_NAME_LABEL: &str = "zookeeper.stackable.de/cluster-name";
const ID_LABEL: &str = "zookeeper.stackable.de/id";

type ZooKeeperReconcileResult = ReconcileResult<error::Error>;

struct ZooKeeperState {
    zk_spec: ZooKeeperClusterSpec,
}

struct ZooKeeperStrategy {
    context: ReconciliationContext<ZooKeeperCluster>,
    state: Mutex<ZooKeeperState>,
}

impl ZooKeeperStrategy {
    pub fn new() -> ZooKeeperStrategy {
        ZooKeeperStrategy {
            state: Mutex::new(),
        }
    }

    /*
    pub async fn test1(&self) -> ZooKeeperReconcileResult {
        println!("Test1");
        let mut data = self.state.lock().unwrap();
        *data += 1;

        println!("{}", data);
        Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)))
    }

    pub async fn test2(&self) -> ZooKeeperReconcileResult {
        println!("Test2");
        Ok(ReconcileFunctionAction::Continue)
    }
     */

    pub async fn reconcile_cluster(self) -> ZooKeeperReconcileResult {
        let state = self.state.lock().unwrap();

        let zk_server_count = state.zk_spec.servers.len();

        let existing_pods = self.context.list_pods().await?;

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
                               self.context.namespace(),
                               self.context.name(),
                               pod);
                        self.context.client.delete(&pod).await?;
                    }
                    // TODO: Need to check for duplicates here and bail out if they exist.
                    Some(label) => {
                        let id = label.parse::<usize>()?;
                        used_ids.push(id);
                        node_name_to_id.insert(node_name.clone(), id);
                    }
                };
                node_name_to_pod.insert(node_name.clone(), pod);
            } else {
                error!("ZooKeeperCluster [{}/{}]: Pod [{:?}] does not have any spec or labels, this is illegal, deleting it.",
                       self.context.namespace(),
                       self.context.name(),
                       pod);
                self.context.client.delete(&pod).await?;
            }
        }

        debug!(
            "ZooKeeperCluster [{}/{}]: Found these myids in use {:?}",
            self.context.namespace(),
            self.context.name(),
            used_ids
        );

        // Step 2:
        // Now that we know the current state of the cluster and its id assignment
        // we can iterate over the requested servers and assign ids to those that are missing one.
        used_ids.sort_unstable();
        for server in &state.zk_spec.servers {
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

                    // Either we found an unused id above (which would be a "hole" between existing ones, e.g. 1,2,4 could find "3")
                    // or we need to create a new one which would be the number of used ids (because we "plug" holes first) plus one.
                    let added_id = added_id.unwrap_or_else(|| used_ids.len() + 1);
                    used_ids.push(added_id);
                    used_ids.sort_unstable();
                    node_name_to_id.insert(server.node_name.clone(), added_id);
                }
                Some(_) => {
                    trace!("ZooKeeperCluster [{}/{}]: Pod for node [{}] already exists and is assigned id [{:?}]",
                           self.context.namespace(),
                           self.context.name(),
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
        for server in &state.zk_spec.servers {
            let pod = match node_name_to_pod.remove(&server.node_name) {
                None => {
                    info!(
                        "ZooKeeperCluster [{}/{}]: Pod for server [{}] missing, creating now...",
                        self.context.namespace(),
                        self.context.name(),
                        &server.node_name
                    );

                    let id = node_name_to_id.remove(&server.node_name).unwrap();
                    let pod = self.create_pod(&server, id).await?;

                    self.create_config_maps(server, id).await?;

                    pod
                }
                Some(pod) => pod,
            };

            // If the pod for this server is currently terminating (this could be for restarts or
            // upgrades) wait until it's done terminating.
            if has_deletion_stamp(&pod) {
                info!(
                    "ZooKeeperCluster [{}/{}] is waiting for Pod [{}] to terminate",
                    self.context.namespace(),
                    self.context.name(),
                    Meta::name(&pod)
                );
                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }

            // At the moment we'll wait for all pods to be available and ready before we might enact any changes to existing ones.
            // TODO: This should probably be configurable later
            if !podutils::is_pod_running_and_ready(&pod) {
                info!(
                    "ZooKeeperCluster [{}/{}] is waiting for Pod [{}] to be running and ready",
                    self.context.namespace(),
                    self.context.name(),
                    Meta::name(&pod)
                );
                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }

            // Check if the pod is up-to-date
            let container = pod.spec.as_ref().unwrap().containers.get(0).unwrap();
            match &container.image {
                Some(image) if image != &state.zk_spec.image_name() => {}
                _ => {}
            }
            // TODO: Rust Experts
            if container.image != Some(state.zk_spec.image_name()) {
                info!(
                    "ZooKeeperCluster [{}/{}]: Image for pod [{}] differs [{:?}] (from container) != [{:?}] (from current spec), deleting old pod",
                    self.context.namespace(),
                    self.context.name(),
                    Meta::name(&pod),
                    container.image,
                    state.zk_spec.image_name()
                );
                self.context.client.delete(&pod).await?;
                return Ok(create_requeuing_reconcile_function_action(10));
            }
        }

        // This goes through all remaining pods in the Map.
        // Because we delete all pods we "need" in the previous loop this will only have pods that are
        // left over (maybe because of a scale down) and can be deleted.
        for (node_name, pod) in node_name_to_pod {
            // TODO: This might already be in the terminating state because earlier we only iterate over the spec servers and not the pods
            info!(
                "ZooKeeperCluster [{}/{}] has extra Pod [{}] for node [{}]: Terminating",
                self.context.namespace(),
                self.context.name(),
                Meta::name(&pod),
                node_name
            );

            // We don't trigger a reconcile requeue here because there should be nothing for us to do
            // in the next loop
            self.context.client.delete(&pod).await?;
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

        for (i, server) in self.context.resource.spec.servers.iter().enumerate() {
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
        Ok(Pod {
            metadata: self.build_pod_metadata(zk_server, id)?,
            spec: Some(self.build_pod_spec(zk_server)),
            ..Pod::default()
        })
    }

    fn build_pod_metadata(
        &self,
        zk_server: &ZooKeeperServer,
        id: usize,
    ) -> Result<ObjectMeta, Error> {
        Ok(ObjectMeta {
            labels: Some(self.build_labels(id)?),
            name: Some(self.get_pod_name(zk_server)),
            namespace: Some(self.context.namespace()),
            owner_references: Some(vec![object_to_owner_reference::<ZooKeeperCluster>(
                self.context.metadata(),
            )?]),
            ..ObjectMeta::default()
        })
    }

    fn build_pod_spec(&self, zk_server: &ZooKeeperServer) -> PodSpec {
        let (containers, volumes) = self.build_containers(zk_server);

        PodSpec {
            node_name: Some(zk_server.node_name.clone()),
            tolerations: Some(create_tolerations()),
            containers,
            volumes: Some(volumes),
            ..PodSpec::default()
        }
    }

    fn build_containers(&self, zk_server: &ZooKeeperServer) -> (Vec<Container>, Vec<Volume>) {
        let version = self.context.resource.spec.version.clone();
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

impl ControllerStrategy for ZooKeeperStrategy {
    type Item = ZooKeeperCluster;
    type Error = crate::error::Error;

    fn finalizer_name(&self) -> String {
        return FINALIZER_NAME.to_string();
    }

    // TODO: fn init_state(...)

    fn reconcile_operations(
        &self,
        context: ReconciliationContext<Self::Item>,
    ) -> Vec<Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + '_>>> {
        vec![Box::pin(self.reconcile_cluster())]
    }
}

/// This creates an instance of a [`Controller`] which waits for incoming events and reconciles them.
///
/// This is an async method and the returned future needs to be consumed to make progress.
pub async fn create_controller(client: Client) {
    let zk_api: Api<ZooKeeperCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();
    let controller_revisions_api: Api<ControllerRevision> = client.get_all_api();

    let controller = stackable_operator::controller::Controller::new(zk_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default())
        .owns(controller_revisions_api, ListParams::default());

    let strategy = ZooKeeperStrategy::new();

    controller.run(client, strategy).await;
}
