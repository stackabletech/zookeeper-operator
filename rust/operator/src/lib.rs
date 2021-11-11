mod error;
use crate::error::Error;
use stackable_zookeeper_crd::commands::{Restart, Start, Stop};

use async_trait::async_trait;
use stackable_operator::builder::{ContainerBuilder, ObjectMetaBuilder, PodBuilder, VolumeBuilder};
use stackable_operator::client::Client;
use stackable_operator::command::materialize_command;
use stackable_operator::controller::Controller;
use stackable_operator::controller::{ControllerStrategy, ReconciliationState};
use stackable_operator::error::OperatorResult;
use stackable_operator::identity::{LabeledPodIdentityFactory, PodIdentity, PodToNodeMapping};
use stackable_operator::k8s_openapi::api::core::v1::{ConfigMap, EnvVar, Pod};
use stackable_operator::kube::api::{ListParams, ResourceExt};
use stackable_operator::kube::Api;
use stackable_operator::labels;
use stackable_operator::labels::{
    build_common_labels_for_all_managed_resources, get_recommended_labels,
};
use stackable_operator::name_utils;
use stackable_operator::product_config::types::PropertyNameKind;
use stackable_operator::product_config::ProductConfigManager;
use stackable_operator::product_config_utils::{
    config_for_role_and_group, transform_all_roles_to_config, validate_all_roles_and_groups_config,
    ValidatedRoleConfigByPropertyKind,
};
use stackable_operator::reconcile::{
    ContinuationStrategy, ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::role_utils;
use stackable_operator::role_utils::{
    get_role_and_group_labels, list_eligible_nodes_for_role_and_group, EligibleNodesForRoleAndGroup,
};
use stackable_operator::scheduler::{
    K8SUnboundedHistory, RoleGroupEligibleNodes, ScheduleStrategy, Scheduler, StickyScheduler,
};
use stackable_operator::status::HasClusterExecutionStatus;
use stackable_operator::status::{init_status, ClusterExecutionStatus};
use stackable_operator::versioning::{finalize_versioning, init_versioning};
use stackable_operator::{configmap, product_config};
use stackable_zookeeper_crd::{
    ZookeeperCluster, ZookeeperClusterSpec, ZookeeperRole, ZookeeperVersion, ADMIN_PORT,
    ADMIN_PORT_PROPERTY, APP_NAME, CLIENT_PORT, CLIENT_PORT_PROPERTY, CONFIG_MAP_TYPE_DATA,
    DATA_DIR, METRICS_PORT, METRICS_PORT_PROPERTY,
};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use strum::IntoEnumIterator;
use tracing::error;
use tracing::{debug, info, trace, warn};

/// The docker image we default to. This needs to be adapted if the operator does not work
/// with images 0.0.1, 0.1.0 etc. anymore and requires e.g. a new major version like 1(.0.0).
const DEFAULT_IMAGE_VERSION: &str = "0";

const FINALIZER_NAME: &str = "zookeeper.stackable.tech/cleanup";
const ID_LABEL: &str = "zookeeper.stackable.tech/id";
const SHOULD_BE_SCRAPED: &str = "monitoring.stackable.tech/should_be_scraped";
const PROPERTIES_FILE: &str = "zoo.cfg";

type ZookeeperReconcileResult = ReconcileResult<error::Error>;

struct ZookeeperState {
    context: ReconciliationContext<ZookeeperCluster>,
    existing_pods: Vec<Pod>,
    eligible_nodes: EligibleNodesForRoleAndGroup,
    validated_role_config: ValidatedRoleConfigByPropertyKind,
}

impl ZookeeperState {
    /// Required labels for pods. Pods without any of these will deleted and/or replaced.
    pub fn get_required_labels(&self) -> BTreeMap<String, Option<Vec<String>>> {
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
        // init status with default values if not available yet.
        self.context.resource = init_status(&self.context.client, &self.context.resource).await?;

        let spec_version = self.context.resource.spec.version.clone();

        self.context.resource =
            init_versioning(&self.context.client, &self.context.resource, spec_version).await?;

        // set the cluster status to running
        if self.context.resource.cluster_execution_status().is_none() {
            self.context
                .client
                .merge_patch_status(
                    &self.context.resource,
                    &self
                        .context
                        .resource
                        .cluster_execution_status_patch(&ClusterExecutionStatus::Running),
                )
                .await?;
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    pub async fn create_missing_pods(&mut self) -> ZookeeperReconcileResult {
        trace!("Starting `create_missing_pods`");
        // The iteration happens in two stages here, to accommodate the way our operators think
        // about roles and role groups.
        // The hierarchy is:
        // - Roles (for ZooKeeper there - currently - is only a single role)
        //   - Role groups for this role (user defined)
        for zookeeper_role in ZookeeperRole::iter() {
            if let Some(nodes_for_role) = self.eligible_nodes.get(&zookeeper_role.to_string()) {
                for (role_group, eligible_nodes) in nodes_for_role {
                    debug!(
                        "Identify missing pods for [{}] role and group [{}]",
                        zookeeper_role, role_group
                    );
                    trace!(
                        "candidate_nodes[{}]: [{:?}]",
                        eligible_nodes.nodes.len(),
                        eligible_nodes
                            .nodes
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
                        get_role_and_group_labels(&zookeeper_role.to_string(), role_group)
                    );

                    let mut history = match self
                        .context
                        .resource
                        .status
                        .as_ref()
                        .and_then(|status| status.history.as_ref())
                    {
                        Some(simple_history) => {
                            // we clone here because we cannot access mut self because we need it later
                            // to create config maps and pods. The `status` history will be out of sync
                            // with the cloned `simple_history` until the next reconcile.
                            // The `status` history should not be used after this method to avoid side
                            // effects.
                            K8SUnboundedHistory::new(&self.context.client, simple_history.clone())
                        }
                        None => K8SUnboundedHistory::new(
                            &self.context.client,
                            PodToNodeMapping::default(),
                        ),
                    };

                    let mut scheduler =
                        StickyScheduler::new(&mut history, ScheduleStrategy::GroupAntiAffinity);

                    let pod_id_factory = LabeledPodIdentityFactory::new(
                        APP_NAME,
                        &self.context.name(),
                        &self.eligible_nodes,
                        ID_LABEL,
                        1,
                    );

                    let state = scheduler.schedule(
                        &pod_id_factory,
                        &RoleGroupEligibleNodes::from(&self.eligible_nodes),
                        &self.existing_pods,
                    )?;

                    let mapping = state.remaining_mapping().filter(
                        APP_NAME,
                        &self.context.name(),
                        &zookeeper_role.to_string(),
                        role_group,
                    );

                    if let Some((pod_id, node_id)) = mapping.iter().next() {
                        // now we have a node that needs a pod -> get validated config
                        let validated_config = config_for_role_and_group(
                            pod_id.role(),
                            pod_id.group(),
                            &self.validated_role_config,
                        )?;

                        let config_maps = self
                            .create_config_maps(pod_id, validated_config, &state.mapping())
                            .await?;

                        self.create_pod(pod_id, &node_id.name, &config_maps, validated_config)
                            .await?;

                        history.save(&self.context.resource).await?;

                        return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
                    }
                }
            }
        }

        // If we reach here it means all pods must be running on target_version.
        // We can now set current_version to target_version (if target_version was set) and
        // target_version to None
        finalize_versioning(&self.context.client, &self.context.resource).await?;
        Ok(ReconcileFunctionAction::Continue)
    }

    /// Creates the config maps required for a zookeeper instance (or role, role_group combination):
    /// * The 'zoo.cfg' properties file
    /// * The 'myid' file
    ///
    /// The 'zoo.cfg' properties are read from the product_config and/or merged with the cluster
    /// custom resource.
    ///
    /// Labels are automatically adapted from the `recommended_labels` with a type (data for
    /// 'zoo.cfg' and id for 'myid'). Names are generated via `name_utils::build_resource_name`.
    ///
    /// Returns a map with a 'type' identifier (e.g. data, id) as key and the corresponding
    /// ConfigMap as value. This is required to set the volume mounts in the pod later on.
    ///
    /// # Arguments
    ///
    /// - `pod_id` - The `PodIdentity` containing app, instance, role, group names and the id.
    /// - `validated_config` - The validated product config.
    /// - `id_mapping` - All id to node mappings required to create config maps
    ///
    async fn create_config_maps(
        &self,
        pod_id: &PodIdentity,
        validated_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
        id_mapping: &PodToNodeMapping,
    ) -> Result<HashMap<&'static str, ConfigMap>, Error> {
        let mut config_maps = HashMap::new();

        let recommended_labels = get_recommended_labels(
            &self.context.resource,
            pod_id.app(),
            &self.context.resource.spec.version.to_string(),
            pod_id.role(),
            pod_id.group(),
        );

        // Get config from product-config for the zookeeper properties file (zoo.cfg)
        if let Some(config) =
            validated_config.get(&PropertyNameKind::File(PROPERTIES_FILE.to_string()))
        {
            // We need to convert from <String, String> to <String, Option<String>> to deal with
            // CLI flags or the java properties writer etc. We can not currently represent that
            // via operator-rs / product-config. This is a preparation for that.
            let mut transformed_config: BTreeMap<String, Option<String>> = config
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone())))
                .collect();

            // add dynamic config map requirement for server ids
            for (pod_id, node_id) in id_mapping.mapping.iter() {
                transformed_config.insert(
                    format!("server.{}", pod_id.id()),
                    Some(format!("{}:2888:3888", node_id.name)),
                );
            }

            let zoo_cfg =
                product_config::writer::to_java_properties_string(transformed_config.iter())?;

            // enhance with config map type label
            let mut cm_config_data_labels = recommended_labels.clone();
            cm_config_data_labels.insert(
                configmap::CONFIGMAP_TYPE_LABEL.to_string(),
                CONFIG_MAP_TYPE_DATA.to_string(),
            );

            let cm_data_name = name_utils::build_resource_name(
                pod_id.app(),
                pod_id.instance(),
                pod_id.role(),
                Some(pod_id.group()),
                None,
                Some(CONFIG_MAP_TYPE_DATA),
            )?;

            let mut cm_config_data = BTreeMap::new();
            cm_config_data.insert(PROPERTIES_FILE.to_string(), zoo_cfg);

            let cm_data = configmap::build_config_map(
                &self.context.resource,
                &cm_data_name,
                &self.context.namespace(),
                cm_config_data_labels,
                cm_config_data,
            )?;

            config_maps.insert(
                CONFIG_MAP_TYPE_DATA,
                configmap::create_config_map(&self.context.client, cm_data).await?,
            );
        }

        Ok(config_maps)
    }

    /// Creates the pod required for the zookeeper instance.
    ///
    /// # Arguments
    ///
    /// - `pod_id` - The `PodIdentity` containing app, instance, role, group names and the id.
    /// - `node_name` - The node_name for this pod.
    /// - `config_maps` - The config maps and respective types required for this pod.
    /// - `validated_config` - The validated product config.
    ///
    async fn create_pod(
        &self,
        pod_id: &PodIdentity,
        node_name: &str,
        config_maps: &HashMap<&'static str, ConfigMap>,
        validated_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    ) -> Result<Pod, Error> {
        let mut env_vars = vec![];
        let mut metrics_port: Option<String> = None;
        let mut client_port: Option<String> = None;
        let mut admin_port: Option<String> = None;
        let mut data_dir: Option<String> = None;

        let version: &ZookeeperVersion = &self.context.resource.spec.version;

        for (property_name_kind, config) in validated_config {
            match property_name_kind {
                PropertyNameKind::File(_) => {
                    // we need to extract the client port here to add to container ports later
                    client_port = config.get(CLIENT_PORT_PROPERTY).cloned();
                    // we need to extract the admin port here to add to container ports later
                    admin_port = config.get(ADMIN_PORT_PROPERTY).cloned();
                    // we need to extract the data dir for the volume mounts later
                    data_dir = config.get(DATA_DIR).cloned();
                }
                PropertyNameKind::Env => {
                    for (property_name, property_value) in config {
                        if property_name.is_empty() {
                            warn!("Received empty property_name for ENV... skipping");
                            continue;
                        }

                        // if a metrics port is provided (for now by user, it is not required in
                        // product config to be able to not configure any monitoring / metrics)
                        if property_name == METRICS_PORT_PROPERTY {
                            metrics_port = Some(property_value.to_string());
                            env_vars.push(EnvVar {
                                name: "SERVER_JVMFLAGS".to_string(),
                                value: Some(format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/server.yaml",
                                                    property_value)),
                                ..EnvVar::default()
                            });
                            continue;
                        }

                        env_vars.push(EnvVar {
                            name: property_name.clone(),
                            value: Some(property_value.clone()),
                            value_from: None,
                        });
                    }
                }
                _ => {}
            }
        }

        let pod_name = name_utils::build_resource_name(
            pod_id.app(),
            pod_id.instance(),
            pod_id.role(),
            Some(pod_id.group()),
            Some(node_name),
            None,
        )?;

        let data_folder = data_dir.unwrap_or_else(|| "/stackable/data".to_string());

        let mut pod_builder = PodBuilder::new();

        let mut container_builder = ContainerBuilder::new(APP_NAME);
        container_builder
            .image(format!(
                // For now we hardcode the stackable image version via DEFAULT_IMAGE_VERSION
                // which represents the major image version and will fallback to the newest
                // available image e.g. if DEFAULT_IMAGE_VERSION = 0 and versions 0.0.1 and
                // 0.0.2 are available, the latter one will be selected. This may change the
                // image during restarts depending on the imagePullPolicy.
                // TODO: should be made configurable
                "docker.stackable.tech/stackable/zookeeper:{}-stackable{}",
                version.to_string(),
                DEFAULT_IMAGE_VERSION
            ))
            .add_env_vars(env_vars)
            .command(vec!["/bin/bash".to_string(), "-c".to_string()])
            // first we execute the myid script and then start zookeeper
            .args(vec![format!(
                "{} {} {}; {} {} {}",
                "/stackable/bin/write-myid",
                data_folder,
                pod_id.id(),
                "bin/zkServer.sh",
                "start-foreground",
                "/stackable/conf/zoo.cfg"
            )]);

        // One mount for the config directory
        if let Some(config_map_data) = config_maps.get(CONFIG_MAP_TYPE_DATA) {
            if let Some(name) = config_map_data.metadata.name.as_ref() {
                container_builder.add_volume_mount("config", "/stackable/conf");
                pod_builder.add_volume(VolumeBuilder::new("config").with_config_map(name).build());
            } else {
                return Err(error::Error::MissingConfigMapNameError {
                    cm_type: CONFIG_MAP_TYPE_DATA,
                });
            }
        } else {
            return Err(error::Error::MissingConfigMapError {
                cm_type: CONFIG_MAP_TYPE_DATA,
                pod_name,
            });
        }

        container_builder.add_volume_mount("data", &data_folder);
        pod_builder.add_volume(
            VolumeBuilder::new("data")
                .with_empty_dir(Some(""), None)
                .build(),
        );

        let mut annotations = BTreeMap::new();
        // only add metrics container port and annotation if available
        if let Some(metrics_port) = metrics_port {
            annotations.insert(SHOULD_BE_SCRAPED.to_string(), "true".to_string());
            container_builder.add_container_port(METRICS_PORT, metrics_port.parse()?);
        }
        // add client port if available
        if let Some(client_port) = client_port {
            container_builder.add_container_port(CLIENT_PORT, client_port.parse()?);
        }

        // add admin port if available
        if let Some(admin_port) = admin_port {
            container_builder.add_container_port(ADMIN_PORT, admin_port.parse()?);
        }

        let mut pod_labels = get_recommended_labels(
            &self.context.resource,
            pod_id.app(),
            &self.context.resource.spec.version.to_string(),
            pod_id.role(),
            pod_id.group(),
        );
        // we need to add the zookeeper id to the labels
        pod_labels.insert(ID_LABEL.to_string(), pod_id.id().to_string());

        let pod = pod_builder
            .metadata(
                ObjectMetaBuilder::new()
                    .generate_name(pod_name)
                    .namespace(&self.context.client.default_namespace)
                    .with_labels(pod_labels)
                    .with_annotations(annotations)
                    .ownerreference_from_resource(&self.context.resource, Some(true), Some(true))?
                    .build()?,
            )
            .add_container(container_builder.build())
            .node_name(node_name)
            // TODO: first iteration we are using host network
            .host_network(true)
            .build()?;

        Ok(self.context.client.create(&pod).await?)
    }

    async fn delete_all_pods(&self) -> OperatorResult<ReconcileFunctionAction> {
        for pod in &self.existing_pods {
            self.context.client.delete(pod).await?;
        }
        Ok(ReconcileFunctionAction::Done)
    }

    pub async fn process_command(&mut self) -> ZookeeperReconcileResult {
        match self.context.retrieve_current_command().await? {
            // if there is no new command and the execution status is stopped we stop the
            // reconcile loop here.
            None => match self.context.resource.cluster_execution_status() {
                Some(execution_status) if execution_status == ClusterExecutionStatus::Stopped => {
                    Ok(ReconcileFunctionAction::Done)
                }
                _ => Ok(ReconcileFunctionAction::Continue),
            },
            Some(command_ref) => match command_ref.kind.as_str() {
                "Restart" => {
                    info!("Restarting cluster [{:?}]", command_ref);
                    let mut restart_command: Restart =
                        materialize_command(&self.context.client, &command_ref).await?;
                    Ok(self.context.default_restart(&mut restart_command).await?)
                }
                "Start" => {
                    info!("Starting cluster [{:?}]", command_ref);
                    let mut start_command: Start =
                        materialize_command(&self.context.client, &command_ref).await?;
                    Ok(self.context.default_start(&mut start_command).await?)
                }
                "Stop" => {
                    info!("Stopping cluster [{:?}]", command_ref);
                    let mut stop_command: Stop =
                        materialize_command(&self.context.client, &command_ref).await?;

                    Ok(self.context.default_stop(&mut stop_command).await?)
                }
                _ => {
                    error!("Got unknown type of command: [{:?}]", command_ref);
                    Ok(ReconcileFunctionAction::Done)
                }
            },
        }
    }
}

impl ReconciliationState for ZookeeperState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        info!("========================= Starting reconciliation =========================");

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
                    &self.get_required_labels(),
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
                .then(self.process_command())
                .await?
                .then(self.context.delete_excess_pods(
                    list_eligible_nodes_for_role_and_group(&self.eligible_nodes).as_slice(),
                    &self.existing_pods,
                    ContinuationStrategy::OneRequeue,
                ))
                .await?
                .then(self.create_missing_pods())
                .await
        })
    }
}

struct ZookeeperStrategy {
    config: Arc<ProductConfigManager>,
}

impl ZookeeperStrategy {
    pub fn new(config: ProductConfigManager) -> ZookeeperStrategy {
        ZookeeperStrategy {
            config: Arc::new(config),
        }
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
        let existing_pods = context
            .list_owned(build_common_labels_for_all_managed_resources(
                APP_NAME,
                &context.resource.name(),
            ))
            .await?;
        trace!(
            "{}: Found [{}] pods",
            context.log_name(),
            existing_pods.len()
        );

        let zk_spec: ZookeeperClusterSpec = context.resource.spec.clone();

        let mut eligible_nodes = HashMap::new();

        eligible_nodes.insert(
            ZookeeperRole::Server.to_string(),
            role_utils::find_nodes_that_fit_selectors(&context.client, None, &zk_spec.servers)
                .await?,
        );

        let mut roles = HashMap::new();
        roles.insert(
            ZookeeperRole::Server.to_string(),
            (
                vec![
                    PropertyNameKind::File(PROPERTIES_FILE.to_string()),
                    PropertyNameKind::Env,
                ],
                context.resource.spec.servers.clone().into(),
            ),
        );

        let role_config = transform_all_roles_to_config(&context.resource, roles);
        let validated_role_config = validate_all_roles_and_groups_config(
            &context.resource.spec.version.to_string(),
            &role_config,
            &self.config,
            false,
            false,
        )?;

        Ok(ZookeeperState {
            context,
            existing_pods,
            eligible_nodes,
            validated_role_config,
        })
    }
}

/// This creates an instance of a [`Controller`] which waits for incoming events and reconciles them.
///
/// This is an async method and the returned future needs to be consumed to make progress.
pub async fn create_controller(client: Client, product_config_path: &str) -> OperatorResult<()> {
    let zk_api: Api<ZookeeperCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();
    let cmd_restart_api: Api<Restart> = client.get_all_api();
    let cmd_start_api: Api<Start> = client.get_all_api();
    let cmd_stop_api: Api<Stop> = client.get_all_api();

    let controller = Controller::new(zk_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default())
        .owns(cmd_restart_api, ListParams::default())
        .owns(cmd_start_api, ListParams::default())
        .owns(cmd_stop_api, ListParams::default());

    let product_config = ProductConfigManager::from_yaml_file(product_config_path).unwrap();

    let strategy = ZookeeperStrategy::new(product_config);

    controller
        .run(client, strategy, Duration::from_secs(10))
        .await;

    Ok(())
}
