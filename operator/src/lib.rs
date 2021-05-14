#![feature(backtrace)]
mod error;

use crate::error::Error;

use async_trait::async_trait;
use handlebars::Handlebars;
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, Node, Pod, PodSpec, Volume, VolumeMount,
};
use kube::api::{ListParams, Resource};
use kube::Api;
use serde_json::json;
use tracing::{debug, error, info, trace, warn};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use stackable_operator::client::Client;
use stackable_operator::conditions::ConditionStatus;
use stackable_operator::controller::Controller;
use stackable_operator::controller::{ControllerStrategy, ReconciliationState};
use stackable_operator::error::OperatorResult;
use stackable_operator::k8s_utils::LabelOptionalValueMap;
use stackable_operator::labels;
use stackable_operator::metadata;
use stackable_operator::reconcile::{
    ContinuationStrategy, ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::role_utils::RoleGroup;
use stackable_operator::{config_map, role_utils};
use stackable_operator::{k8s_utils, krustlet};
use stackable_zookeeper_crd::{
    ZookeeperCluster, ZookeeperClusterSpec, ZookeeperClusterStatus, ZookeeperVersion, APP_NAME,
    MANAGED_BY,
};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use strum::IntoEnumIterator;
use strum_macros::Display;
use strum_macros::EnumIter;

const FINALIZER_NAME: &str = "zookeeper.stackable.tech/cleanup";

const ID_LABEL: &str = "zookeeper.stackable.tech/id";

type ZookeeperReconcileResult = ReconcileResult<error::Error>;

#[derive(EnumIter, Debug, Display, PartialEq, Eq, Hash)]
pub enum ZookeeperRole {
    Server,
}

struct ZookeeperState {
    context: ReconciliationContext<ZookeeperCluster>,
    zk_spec: ZookeeperClusterSpec,
    zk_status: Option<ZookeeperClusterStatus>,
    id_information: Option<IdInformation>,
    existing_pods: Vec<Pod>,
    eligible_nodes: HashMap<ZookeeperRole, HashMap<String, Vec<Node>>>,
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

impl ZookeeperState {
    async fn set_upgrading_condition(
        &self,
        conditions: &[Condition],
        message: &str,
        reason: &str,
        status: ConditionStatus,
    ) -> OperatorResult<ZookeeperCluster> {
        let resource = self
            .context
            .build_and_set_condition(
                Some(conditions),
                message.to_string(),
                reason.to_string(),
                status,
                "Upgrading".to_string(),
            )
            .await?;

        Ok(resource)
    }

    async fn set_current_version(
        &self,
        version: Option<&ZookeeperVersion>,
    ) -> OperatorResult<ZookeeperCluster> {
        let resource = self
            .context
            .client
            .merge_patch_status(
                &self.context.resource,
                &json!({ "currentVersion": version }),
            )
            .await?;

        Ok(resource)
    }

    async fn set_target_version(
        &self,
        version: Option<&ZookeeperVersion>,
    ) -> OperatorResult<ZookeeperCluster> {
        let resource = self
            .context
            .client
            .merge_patch_status(&self.context.resource, &json!({ "targetVersion": version }))
            .await?;

        Ok(resource)
    }

    /// For each existing role (server), return a tuple consisting of eligible nodes for a given
    /// selector. Required to delete excess pods that do not match any node or selector description.
    /// TODO: move to operator-rs
    pub fn get_full_pod_node_map(&self) -> Vec<(Vec<Node>, LabelOptionalValueMap)> {
        let mut eligible_nodes_map = vec![];

        for zookeeper_role in ZookeeperRole::iter() {
            if let Some(eligible_nodes_for_role) = self.eligible_nodes.get(&zookeeper_role) {
                for (group_name, eligible_nodes) in eligible_nodes_for_role {
                    // Create labels to identify eligible nodes
                    trace!(
                        "Adding [{}] nodes to eligible node list for role [{}] and group [{}].",
                        eligible_nodes.len(),
                        zookeeper_role,
                        group_name
                    );
                    eligible_nodes_map.push((
                        eligible_nodes.clone(),
                        get_role_and_group_labels(&zookeeper_role, group_name),
                    ))
                }
            }
        }
        eligible_nodes_map
    }

    /// Required labels for pods. Pods without any of these will deleted and/or replaced.
    /// TODO: Now we create this every reconcile run, should be created once and reused.
    pub fn get_deletion_labels(&self) -> BTreeMap<String, Option<Vec<String>>> {
        let roles = ZookeeperRole::iter()
            .map(|role| role.to_string())
            .collect::<Vec<_>>();
        let mut mandatory_labels = BTreeMap::new();

        mandatory_labels.insert(labels::APP_COMPONENT_LABEL.to_string(), Some(roles));
        mandatory_labels.insert(
            labels::APP_INSTANCE_LABEL.to_string(),
            Some(vec![self.context.name()]),
        );
        mandatory_labels.insert(
            labels::APP_VERSION_LABEL.to_string(),
            Some(vec![self.context.resource.spec.version.to_string()]),
        );
        mandatory_labels.insert(ID_LABEL.to_string(), None);

        mandatory_labels
    }

    /// Will initialize the status object if it's never been set.
    async fn init_status(&mut self) -> ZookeeperReconcileResult {
        // We'll begin by setting an empty status here because later in this method we might
        // update its conditions. To avoid any issues we'll just create it once here.
        if self.zk_status.is_none() {
            let status = ZookeeperClusterStatus::default();
            self.context
                .client
                .merge_patch_status(&self.context.resource, &status)
                .await?;
            self.zk_status = Some(status);
        }

        // This should always return either the existing one or the one we just created above.
        let status = self.zk_status.take().unwrap_or_default();
        let spec_version = self.zk_spec.version.clone();

        match (&status.current_version, &status.target_version) {
            (None, None) => {
                // No current_version and no target_version must be initial installation.
                // We'll set the Upgrading condition and the target_version to the version from spec.
                info!(
                    "Initial installation, now moving towards version [{}]",
                    self.zk_spec.version
                );
                self.zk_status = self
                    .set_upgrading_condition(
                        &status.conditions,
                        &format!("Initial installation to version [{:?}]", spec_version),
                        "InitialInstallation",
                        ConditionStatus::True,
                    )
                    .await?
                    .status;
                self.zk_status = self.set_target_version(Some(&spec_version)).await?.status;
            }
            (None, Some(target_version)) => {
                // No current_version but a target_version means we're still doing the initial
                // installation. Will continue working towards that goal even if another version
                // was set in the meantime.
                debug!(
                    "Initial installation, still moving towards version [{}]",
                    target_version
                );
                if &spec_version != target_version {
                    info!("A new target version ([{}]) was requested while we still do the initial installation to [{}], finishing running upgrade first", spec_version, target_version)
                }
                // We do this here to update the observedGeneration if needed
                self.zk_status = self
                    .set_upgrading_condition(
                        &status.conditions,
                        &format!("Initial installation to version [{:?}]", target_version),
                        "InitialInstallation",
                        ConditionStatus::True,
                    )
                    .await?
                    .status;
            }
            (Some(current_version), None) => {
                // We are at a stable version but have no target_version set.
                // This will be the normal state.
                // We'll check if there is a different version in spec and if it is will
                // set it in target_version, but only if it's actually a compatible upgrade.
                if current_version != &spec_version {
                    if current_version.is_valid_upgrade(&spec_version).unwrap() {
                        let new_version = spec_version;
                        let message = format!(
                            "Upgrading from [{:?}] to [{:?}]",
                            current_version, &new_version
                        );
                        info!("{}", message);
                        self.zk_status = self.set_target_version(Some(&new_version)).await?.status;
                        self.zk_status = self
                            .set_upgrading_condition(
                                &status.conditions,
                                &message,
                                "Upgrading",
                                ConditionStatus::True,
                            )
                            .await?
                            .status;
                    } else {
                        // TODO: This should be caught by an validating admission webhook
                        warn!("Upgrade from [{}] to [{}] not possible but requested in spec: Ignoring, will continue reconcile as if the invalid version weren't set", current_version, spec_version);
                    }
                } else {
                    let message = format!(
                        "No upgrade required [{:?}] is still the current_version",
                        current_version
                    );
                    trace!("{}", message);
                    self.zk_status = self
                        .set_upgrading_condition(
                            &status.conditions,
                            &message,
                            "",
                            ConditionStatus::False,
                        )
                        .await?
                        .status;
                }
            }
            (Some(current_version), Some(target_version)) => {
                // current_version and target_version are set means we're still in the process
                // of upgrading. We'll only do some logging and checks and will update
                // the condition so observedGeneration can be updated.
                debug!(
                    "Still upgrading from [{}] to [{}]",
                    current_version, target_version
                );
                if &self.zk_spec.version != target_version {
                    info!("A new target version was requested while we still upgrade from [{}] to [{}], finishing running upgrade first", current_version, target_version)
                }
                let message = format!(
                    "Upgrading from [{:?}] to [{:?}]",
                    current_version, target_version
                );

                self.zk_status = self
                    .set_upgrading_condition(
                        &status.conditions,
                        &message,
                        "",
                        ConditionStatus::False,
                    )
                    .await?
                    .status;
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    // This looks at all currently existing Pods for the current ZookeeperCluster object.
    // It checks if all the pods are valid (i.e. contain required labels) and then builds an `IdInformation`
    // object and sets it on the current state.
    async fn read_existing_pod_information(&mut self) -> ZookeeperReconcileResult {
        trace!(
            "Reading existing pod information for {}",
            self.context.log_name()
        );

        // We first create a list of all used ids (`myid`) so we know which we can reuse
        // At the same time we create a map of id to pod for all pods which already exist
        // Later we fill those up.
        // It depends on the ZooKeeper version on whether this requires a full restart of the cluster
        // or whether this can be done dynamically.
        // This list also includes all ids currently in use by terminating or otherwise not-ready pods.
        // We never want to use those as long as there's a chance that some process might be actively
        // using it.
        // There can be a maximum of 255 (I believe) ids.
        let mut used_ids = Vec::with_capacity(self.existing_pods.len());
        let mut node_name_to_pod = HashMap::new(); // This is going to own the pods
        let mut node_name_to_id = HashMap::new();

        // Iterate over all existing pods and read the label which contains the `myid`
        for pod in &self.existing_pods {
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
                        error!("ZookeeperCluster {}: Pod [{:?}] does not have the `id` label, this is illegal, deleting it.",
                               self.context.log_name(),
                               pod);
                        self.context.client.delete(pod).await?;
                    }
                    Some(label) => {
                        let id = match label.parse::<usize>() {
                            Ok(id) => id,
                            Err(_) => {
                                error!("ZookeeperCluster {}: Pod [{:?}] does have the `id` label but the label ([{}]) cannot be parsed, this is illegal, deleting the pod.",
                                       self.context.log_name(), pod, label);
                                self.context.client.delete(pod).await?;
                                continue;
                            }
                        };

                        // Check if we have seen the same id before
                        // This should never happen and would currently require manual cleanup
                        if used_ids.contains(&id) {
                            // TODO: Update status
                            error!(
                                "Found a duplicate `myid` [{}] in Pod [{}], we can't recover \
                                 from this error and you need to clean up manually",
                                id,
                                Resource::name(pod)
                            );
                            return Err(Error::ReconcileError("Found duplicate id".to_string()));
                        }

                        used_ids.push(id);
                        node_name_to_id.insert(node_name.clone(), id);
                        node_name_to_pod.insert(node_name.clone(), pod.clone());
                    }
                };
            } else {
                error!("ZookeeperCluster {}: Pod [{:?}] does not have any spec or labels, this is illegal, deleting it.",
                       self.context.log_name(),
                       pod);
                self.context.client.delete(pod).await?;
            }
        }

        debug!(
            "ZookeeperCluster {}: Found these myids in use [{:?}]",
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
    async fn assign_ids(&mut self) -> ZookeeperReconcileResult {
        trace!("Assigning ids to new servers from the spec",);

        let id_information = self.id_information.as_mut().ok_or_else(|| error::Error::ReconcileError(
                        "id_information missing, this is a programming error and should never happen. Please report in our issue tracker.".to_string(),
                    ))?;

        id_information.used_ids.sort_unstable();

        for role in ZookeeperRole::iter() {
            if let Some(eligible_nodes_for_role) = self.eligible_nodes.get(&role) {
                for eligible_nodes in eligible_nodes_for_role.values() {
                    for node in eligible_nodes {
                        let node_name = match &node.metadata.name {
                            Some(name) => name,
                            None => continue,
                        };

                        match id_information.node_name_to_pod.get(node_name) {
                            None => {
                                // TODO: Need to check whether the topology has changed. If it has we need to restart all servers depending on the ZK version
                                let new_id = find_first_missing(&id_information.used_ids);

                                id_information.used_ids.push(new_id);
                                id_information.used_ids.sort_unstable();
                                id_information
                                    .node_name_to_id
                                    .insert(node_name.clone(), new_id);

                                info!(
                                    "Assigning new id [{}] to server/node [{}]",
                                    new_id, node_name
                                )
                            }
                            Some(_) => {
                                trace!(
                                    "Pod for node [{}] already exists and is assigned id [{:?}]",
                                    node_name,
                                    id_information.node_name_to_id.get(node_name)
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    pub async fn create_missing_pods(&mut self) -> ZookeeperReconcileResult {
        trace!("Starting `create_missing_pods`");

        let id_information = self.id_information.as_mut().ok_or_else(|| error::Error::ReconcileError(
                        "id_information missing, this is a programming error and should never happen. Please report in our issue tracker.".to_string(),
                    ))?;

        // The iteration happens in two stages here, to accommodate the way our operators think
        // about nodes and roles.
        // The hierarchy is:
        // - Roles (for example Datanode, Namenode, Server)
        //   - Role groups for this role (user defined)
        for zookeeper_role in ZookeeperRole::iter() {
            if let Some(nodes_for_role) = self.eligible_nodes.get(&zookeeper_role) {
                for (role_group, nodes) in nodes_for_role {
                    debug!(
                        "Identify missing pods for [{}] role and group [{}]",
                        zookeeper_role, role_group
                    );
                    trace!(
                        "candidate_nodes[{}]: [{:?}]",
                        nodes.len(),
                        nodes
                            .iter()
                            .map(|node| node.metadata.name.as_ref().unwrap())
                            .collect::<Vec<_>>()
                    );
                    trace!(
                        "existing_pods[{}]: [{:?}]",
                        &self.existing_pods.len(),
                        &self
                            .existing_pods
                            .iter()
                            .map(|pod| pod.metadata.name.as_ref().unwrap())
                            .collect::<Vec<_>>()
                    );
                    trace!(
                        "labels: [{:?}]",
                        get_role_and_group_labels(&zookeeper_role, role_group)
                    );
                    let nodes_that_need_pods = k8s_utils::find_nodes_that_need_pods(
                        nodes,
                        &self.existing_pods,
                        &get_role_and_group_labels(&zookeeper_role, role_group),
                    );

                    for node in nodes_that_need_pods {
                        let node_name = if let Some(node_name) = &node.metadata.name {
                            node_name
                        } else {
                            warn!("No name found in metadata, this should not happen! Skipping node: [{:?}]", node);
                            continue;
                        };
                        debug!(
                            "Creating pod on node [{}] for [{}] role and group [{}]",
                            node.metadata
                                .name
                                .as_deref()
                                .unwrap_or("<no node name found>"),
                            zookeeper_role,
                            role_group
                        );

                        if id_information.node_name_to_pod.get(node_name).is_none() {
                            info!("Pod for server [{}] missing, creating now...", node_name);

                            let id = *id_information
                                .node_name_to_id
                                .get(node_name)
                                .ok_or_else(|| Error::ReconcileError(format!("We didn't find a `myid` for [{}] but it should have been assigned, this is a bug, please report it", node_name)))?;

                            let pod_name = format!(
                                "{}-{}-{}-{}",
                                APP_NAME,
                                self.context.name(),
                                role_group,
                                zookeeper_role
                            )
                            .to_lowercase();

                            let pod_labels = build_pod_labels(
                                &zookeeper_role.to_string(),
                                role_group,
                                &self.context.name(),
                                &self.context.resource.spec.version.to_string(),
                                &id.to_string(),
                            );

                            self.create_pod(&node_name, &pod_name, pod_labels).await?;
                            self.create_config_maps(&pod_name, id).await?;

                            return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
                        } else {
                            debug!(
                                "Pod for server [{}] already created, skipping ...",
                                node_name
                            );
                        }
                    }
                }
            }
        }

        let status = self.zk_status.clone().ok_or_else(|| error::Error::ReconcileError(
            "`zk_status missing, this is a programming error and should never happen. Please report in our issue tracker.".to_string(),
        ))?;

        // If we reach here it means all pods must be running on target_version.
        // We can now set current_version to target_version (if target_version was set) and
        // target_version to None
        if let Some(target_version) = &status.target_version {
            self.zk_status = self.set_target_version(None).await?.status;
            self.zk_status = self
                .set_current_version(Some(&target_version))
                .await?
                .status;
            self.zk_status = self
                .set_upgrading_condition(
                    &status.conditions,
                    &format!(
                        "No upgrade required [{:?}] is still the current_version",
                        target_version
                    ),
                    "",
                    ConditionStatus::False,
                )
                .await?
                .status;
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    async fn delete_all_pods(&self) -> OperatorResult<ReconcileFunctionAction> {
        let existing_pods = self.context.list_pods().await?;
        for pod in existing_pods {
            self.context.client.delete(&pod).await?;
        }
        Ok(ReconcileFunctionAction::Done)
    }

    async fn create_config_maps(&self, pod_name: &str, id: usize) -> Result<(), Error> {
        let mut options = HashMap::new();
        options.insert("tickTime".to_string(), "2000".to_string());
        options.insert("dataDir".to_string(), "/tmp/zookeeper".to_string());
        options.insert("initLimit".to_string(), "5".to_string());
        options.insert("syncLimit".to_string(), "2".to_string());
        options.insert("clientPort".to_string(), "2181".to_string());

        let id_information = self.id_information.as_ref().ok_or_else(|| error::Error::ReconcileError(
                        "id_information missing, this is a programming error and should never happen. Please report in our issue tracker.".to_string(),
                    ))?;

        for (node_name, id) in &id_information.node_name_to_id {
            options.insert(format!("server.{}", id), format!("{}:2888:3888", node_name));
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

        let cm_name = format!("{}-config", pod_name);
        let cm = config_map::create_config_map(&self.context.resource, &cm_name, data)?;
        self.context.client.apply_patch(&cm, &cm).await?;

        // ...and one for the data directory (which only contains the myid file)
        let mut data = BTreeMap::new();
        data.insert("myid".to_string(), id.to_string());
        let cm_name = format!("{}-data", pod_name);
        let cm = config_map::create_config_map(&self.context.resource, &cm_name, data)?;
        self.context.client.apply_patch(&cm, &cm).await?;
        Ok(())
    }

    async fn create_pod(
        &self,
        node_name: &str,
        pod_name: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<Pod, Error> {
        let pod = self.build_pod(node_name, pod_name, labels)?;
        Ok(self.context.client.create(&pod).await?)
    }

    fn build_pod(
        &self,
        node_name: &str,
        pod_name: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<Pod, Error> {
        let (containers, volumes) = self.build_containers(pod_name);

        Ok(Pod {
            metadata: metadata::build_metadata(
                pod_name.to_string(),
                Some(labels),
                &self.context.resource,
                true,
            )?,
            spec: Some(PodSpec {
                node_name: Some(node_name.to_string()),
                tolerations: Some(krustlet::create_tolerations()),
                containers,
                volumes: Some(volumes),
                ..PodSpec::default()
            }),

            ..Pod::default()
        })
    }

    fn build_containers(&self, pod_name: &str) -> (Vec<Container>, Vec<Volume>) {
        let version = &self.context.resource.spec.version;

        let image_name = format!("stackable/zookeeper:{}", version.to_string());

        let containers = vec![Container {
            image: Some(image_name),
            name: "zookeeper".to_string(),
            command: Some(vec![
                format!("{}/bin/zkServer.sh", version.package_name()),
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

        let volumes = vec![
            Volume {
                name: "config-volume".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(format!("{}-config", pod_name)),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            },
            Volume {
                name: "data-volume".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(format!("{}-data", pod_name)),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            },
        ];

        (containers, volumes)
    }
}

impl ReconciliationState for ZookeeperState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        info!("========================= Starting reconciliation =========================");
        debug!("Deletion Labels: [{:?}]", &self.get_deletion_labels());

        Box::pin(async move {
            self.init_status()
                .await?
                .then(self.context.handle_deletion(
                    Box::pin(self.delete_all_pods()),
                    FINALIZER_NAME,
                    true,
                ))
                .await?
                .then(self.context.delete_illegal_pods(
                    self.existing_pods.as_slice(),
                    &self.get_deletion_labels(),
                    ContinuationStrategy::OneRequeue,
                ))
                .await?
                .then(
                    self.context
                        .wait_for_terminating_pods(self.existing_pods.as_slice()),
                )
                .await?
                .then(
                    self.context
                        .wait_for_running_and_ready_pods(&self.existing_pods),
                )
                .await?
                .then(self.context.delete_excess_pods(
                    self.get_full_pod_node_map().as_slice(),
                    &self.existing_pods,
                    ContinuationStrategy::OneRequeue,
                ))
                .await?
                .then(self.read_existing_pod_information())
                .await?
                .then(self.assign_ids())
                .await?
                .then(self.create_missing_pods())
                .await
        })
    }
}

#[derive(Debug)]
struct ZookeeperStrategy {}

impl ZookeeperStrategy {
    pub fn new() -> ZookeeperStrategy {
        ZookeeperStrategy {}
    }
}

#[async_trait]
impl ControllerStrategy for ZookeeperStrategy {
    type Item = ZookeeperCluster;
    type State = ZookeeperState;
    type Error = Error;

    /// Init the ZooKeeper state. Store all available pods owned by this cluster for later processing.
    /// Retrieve nodes that fit selectors and store them for later processing:
    /// ZookeeperRole (we only have 'server') -> role group -> list of nodes.
    async fn init_reconcile_state(
        &self,
        context: ReconciliationContext<Self::Item>,
    ) -> Result<Self::State, Self::Error> {
        let existing_pods = context.list_pods().await?;
        trace!(
            "{}: Found [{}] pods",
            context.log_name(),
            existing_pods.len()
        );

        let zk_spec: ZookeeperClusterSpec = context.resource.spec.clone();

        let mut eligible_nodes = HashMap::new();

        let role_groups: Vec<RoleGroup> = zk_spec
            .servers
            .selectors
            .iter()
            .map(|(group_name, selector_config)| RoleGroup {
                name: group_name.to_string(),
                selector: selector_config.clone().selector.unwrap(),
            })
            .collect();

        eligible_nodes.insert(
            ZookeeperRole::Server,
            role_utils::find_nodes_that_fit_selectors(
                &context.client,
                None,
                role_groups.as_slice(),
            )
            .await?,
        );

        Ok(ZookeeperState {
            zk_spec: context.resource.spec.clone(),
            zk_status: context.resource.status.clone(),
            context,
            id_information: None,
            existing_pods,
            eligible_nodes,
        })
    }
}

/// This creates an instance of a [`Controller`] which waits for incoming events and reconciles them.
///
/// This is an async method and the returned future needs to be consumed to make progress.
pub async fn create_controller(client: Client) {
    let zk_api: Api<ZookeeperCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();

    let controller = Controller::new(zk_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default());

    let strategy = ZookeeperStrategy::new();

    controller
        .run(client, strategy, Duration::from_secs(10))
        .await;
}

/// Return a map with labels and values for role (component) and group (role_group).
/// Required to find nodes that are a possible match for pods.
/// TODO: move to operator-rs
fn get_role_and_group_labels(role: &ZookeeperRole, group_name: &str) -> LabelOptionalValueMap {
    let mut labels = BTreeMap::new();
    labels.insert(
        labels::APP_COMPONENT_LABEL.to_string(),
        Some(role.to_string()),
    );
    labels.insert(
        labels::APP_ROLE_GROUP_LABEL.to_string(),
        Some(group_name.to_string()),
    );
    labels
}

fn build_pod_labels(
    role: &str,
    role_group: &str,
    name: &str,
    version: &str,
    id: &str,
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(labels::APP_NAME_LABEL.to_string(), APP_NAME.to_string());
    labels.insert(
        labels::APP_MANAGED_BY_LABEL.to_string(),
        MANAGED_BY.to_string(),
    );
    labels.insert(labels::APP_COMPONENT_LABEL.to_string(), role.to_string());
    labels.insert(
        labels::APP_ROLE_GROUP_LABEL.to_string(),
        role_group.to_string(),
    );
    labels.insert(labels::APP_INSTANCE_LABEL.to_string(), name.to_string());
    labels.insert(labels::APP_VERSION_LABEL.to_string(), version.to_string());
    labels.insert(ID_LABEL.to_string(), id.to_string());

    labels
}

#[cfg(test)]
mod tests {

    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(vec![1, 2, 3], 4)]
    #[case(vec![1, 3, 5], 2)]
    #[case(vec![], 1)]
    #[case(vec![3, 4, 6], 1)]
    #[case(vec![1], 2)]
    #[case(vec![2], 1)]
    fn test_first_missing(#[case] input: Vec<usize>, #[case] expected: usize) {
        let first = find_first_missing(&input);
        assert_eq!(first, expected);
    }
}
