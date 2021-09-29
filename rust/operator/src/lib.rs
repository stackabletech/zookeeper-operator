mod error;
use crate::error::Error;
use stackable_zookeeper_crd::commands::{Restart, Start, Stop};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{ConfigMap, EnvVar, Pod};
use kube::api::{ListParams, ResourceExt};
use kube::Api;
use kube::CustomResourceExt;
use product_config::types::PropertyNameKind;
use product_config::ProductConfigManager;
use stackable_operator::builder::{
    ContainerBuilder, ContainerPortBuilder, ObjectMetaBuilder, PodBuilder,
};
use stackable_operator::client::Client;
use stackable_operator::command::{clear_current_command, materialize_command, CommandRef};
use stackable_operator::command_controller::Command;
use stackable_operator::configmap;
use stackable_operator::controller::Controller;
use stackable_operator::controller::{ControllerStrategy, ReconciliationState};
use stackable_operator::error::Error::InvalidName;
use stackable_operator::error::OperatorResult;
use stackable_operator::identity::{LabeledPodIdentityFactory, PodIdentity, PodToNodeMapping};
use stackable_operator::labels;
use stackable_operator::labels::{
    build_common_labels_for_all_managed_resources, get_recommended_labels,
};
use stackable_operator::name_utils;
use stackable_operator::product_config_utils::{
    config_for_role_and_group, transform_all_roles_to_config, validate_all_roles_and_groups_config,
    ValidatedRoleConfigByPropertyKind,
};
use stackable_operator::reconcile::{
    ContinuationStrategy, ProvidesPod, ReconcileFunctionAction, ReconcileResult,
    ReconciliationContext,
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
use stackable_zookeeper_crd::{
    ZookeeperCluster, ZookeeperClusterSpec, ZookeeperRole, ZookeeperVersion, ADMIN_PORT, APP_NAME,
    CLIENT_PORT, CONFIG_MAP_TYPE_DATA, CONFIG_MAP_TYPE_ID, DATA_DIR, METRICS_PORT,
};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use strum::IntoEnumIterator;
use tracing::error;
use tracing::{debug, info, trace, warn};

const FINALIZER_NAME: &str = "zookeeper.stackable.tech/cleanup";
const ID_LABEL: &str = "zookeeper.stackable.tech/id";
const SHOULD_BE_SCRAPED: &str = "monitoring.stackable.tech/should_be_scraped";
const PROPERTIES_FILE: &str = "zoo.cfg";
const CONFIG_DIR_NAME: &str = "conf";

type ZookeeperReconcileResult = ReconcileResult<error::Error>;

struct ZookeeperState {
    context: ReconciliationContext<ZookeeperCluster>,
    existing_pods: Vec<Pod>,
    eligible_nodes: EligibleNodesForRoleAndGroup,
    validated_role_config: ValidatedRoleConfigByPropertyKind,
}

#[async_trait]
impl ProvidesPod for ZookeeperState {
    async fn get_pod_and_context(
        &self,
        _role: &str,
        _role_group: &str,
        _node: &str,
    ) -> OperatorResult<(Pod, Vec<ConfigMap>)> {
        // self.create_pod_and_config_maps(
        //     &ZookeeperRole::from_str(role).unwrap(),
        //     role_group,
        //     node,
        //     config_for_role_and_group(role, role_group, &self.validated_role_config)?,
        // )
        // .await
        // .map_err(|_| InvalidName {
        //     errors: vec!["Stuff went wrong!".to_string()],
        // })

        Err(InvalidName {
            errors: vec!["Stuff went wrong!".to_string()],
        })
    }
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

        // config map for the data directory (which only contains the 'myid' file)
        let cm_config_id_name = name_utils::build_resource_name(
            pod_id.app(),
            pod_id.instance(),
            pod_id.role(),
            Some(pod_id.group()),
            None,
            Some(CONFIG_MAP_TYPE_ID),
        )?;

        // enhance with config map type label and the id for differentiation
        let mut cm_config_id_labels = recommended_labels.clone();
        cm_config_id_labels.insert(
            configmap::CONFIGMAP_TYPE_LABEL.to_string(),
            CONFIG_MAP_TYPE_ID.to_string(),
        );
        cm_config_id_labels.insert(ID_LABEL.to_string(), pod_id.id().to_string());

        let mut cm_id_data = BTreeMap::new();
        cm_id_data.insert("myid".to_string(), pod_id.id().to_string());

        let cm_id = configmap::build_config_map(
            &self.context.resource,
            &cm_config_id_name,
            &self.context.namespace(),
            cm_config_id_labels,
            cm_id_data,
        )?;

        config_maps.insert(
            CONFIG_MAP_TYPE_ID,
            configmap::create_config_map(&self.context.client, cm_id).await?,
        );

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
                    client_port = config.get(CLIENT_PORT).cloned();
                    // we need to extract the admin port here to add to container ports later
                    admin_port = config.get(ADMIN_PORT).cloned();
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
                        if property_name == METRICS_PORT {
                            metrics_port = Some(property_value.to_string());
                            env_vars.push(EnvVar {
                                name: "SERVER_JVMFLAGS".to_string(),
                                value: Some(format!("-javaagent:{{{{packageroot}}}}/{}/stackable/lib/jmx_prometheus_javaagent-0.16.1.jar={}:{{{{packageroot}}}}/{}/stackable/conf/jmx_exporter.yaml",
                                                    version.package_name(), property_value, version.package_name())),
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

        let mut container_builder = ContainerBuilder::new(APP_NAME);
        container_builder.image(format!("stackable/zookeeper:{}", version.to_string()));
        container_builder.command(vec![
            format!(
                "{}/bin/zkServer.sh",
                self.context.resource.spec.version.package_name()
            ),
            "start-foreground".to_string(),
            // "--config".to_string(), TODO: Version 3.4 does not support --config but later versions do
            format!("{{{{configroot}}}}/{}/zoo.cfg", CONFIG_DIR_NAME),
        ]);

        // One mount for the config directory
        if let Some(config_map_data) = config_maps.get(CONFIG_MAP_TYPE_DATA) {
            if let Some(name) = config_map_data.metadata.name.as_ref() {
                container_builder.add_configmapvolume(name, CONFIG_DIR_NAME.to_string());
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

        // We need a second mount for the data directory
        // because we need to write the 'myid' file into the data directory
        if let Some(config_map_data) = config_maps.get(CONFIG_MAP_TYPE_ID) {
            if let Some(name) = config_map_data.metadata.name.as_ref() {
                container_builder.add_configmapvolume(
                    name,
                    data_dir.unwrap_or_else(|| "/tmp/zookeeper".to_string()),
                );
            } else {
                return Err(error::Error::MissingConfigMapNameError {
                    cm_type: CONFIG_MAP_TYPE_ID,
                });
            }
        } else {
            return Err(error::Error::MissingConfigMapError {
                cm_type: CONFIG_MAP_TYPE_ID,
                pod_name,
            });
        }

        container_builder.add_env_vars(env_vars);

        let mut annotations = BTreeMap::new();
        // only add metrics container port and annotation if available
        if let Some(metrics_port) = metrics_port {
            annotations.insert(SHOULD_BE_SCRAPED.to_string(), "true".to_string());
            container_builder.add_container_port(
                ContainerPortBuilder::new(metrics_port.parse()?)
                    .name("metrics")
                    .build(),
            );
        }
        // add client port if available
        if let Some(client_port) = client_port {
            container_builder.add_container_port(
                ContainerPortBuilder::new(client_port.parse()?)
                    .name("client")
                    .build(),
            );
        }

        // add admin port if available
        if let Some(admin_port) = admin_port {
            container_builder.add_container_port(
                ContainerPortBuilder::new(admin_port.parse()?)
                    .name("admin")
                    .build(),
            );
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

        let pod = PodBuilder::new()
            .metadata(
                ObjectMetaBuilder::new()
                    .generate_name(pod_name)
                    .namespace(&self.context.client.default_namespace)
                    .with_labels(pod_labels)
                    .with_annotations(annotations)
                    .ownerreference_from_resource(&self.context.resource, Some(true), Some(true))?
                    .build()?,
            )
            .add_stackable_agent_tolerations()
            .add_container(container_builder.build())
            .node_name(node_name)
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
                "Restart" => self.handle_restart(command_ref).await,
                "Start" => self.handle_start(command_ref).await,
                "Stop" => self.handle_stop(command_ref).await,
                _ => {
                    error!("Got unknown type of command: [{:?}]", command_ref);
                    Ok(ReconcileFunctionAction::Done)
                }
            },
        }
    }

    pub async fn handle_restart(&mut self, command: CommandRef) -> ZookeeperReconcileResult {
        info!("Restarting cluster [{:?}]", command);
        let mut restart_command: Restart =
            materialize_command(&self.context.client, &command).await?;

        let patch = Command::start_patch(&mut restart_command);
        self.context
            .client
            .merge_patch_status(&restart_command, &patch)
            .await?;

        // The returned value from this indicates if all eligible pods have been restarted already
        // if this returns ::Done the command has been finished, we can remove it from the status
        // and continue
        // TODO: Should we continue or requeue to check for a new command?
        match self.context.default_restart(&restart_command, self).await {
            Ok(ReconcileFunctionAction::Done) => {
                clear_current_command(&self.context.client, &mut self.context.resource).await?;

                let patch = Command::finish_patch(&mut restart_command);
                self.context
                    .client
                    .merge_patch_status(&restart_command, &patch)
                    .await?;

                Ok(ReconcileFunctionAction::Continue)
            }
            Ok(_) => Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(5))),
            Err(e) => Err(Error::OperatorError { source: e }),
        }
    }

    pub async fn handle_start(&mut self, command: CommandRef) -> ZookeeperReconcileResult {
        info!("Starting cluster [{:?}]", command);
        let mut start_command: Start = materialize_command(&self.context.client, &command).await?;

        let patch = Command::start_patch(&mut start_command);
        self.context
            .client
            .merge_patch_status(&start_command, &patch)
            .await?;

        self.context.default_start().await?;

        // TODO: We should probably only clear the command after it has finished starting
        //  but that will require some thought about the code structure
        clear_current_command(&self.context.client, &mut self.context.resource).await?;

        let patch = Command::finish_patch(&mut start_command);
        self.context
            .client
            .merge_patch_status(&start_command, &patch)
            .await?;

        // Clearing the stopped condition and requeing will cause the operator to add all missing
        // pods (should be all of them) which effectively causes the service to start again
        // TODO: By handling this in the normal pod start functionality we lose control over
        //   whether to start all pods at once one by one, ...
        //   might be worthwhile adding starting behavior to the command handling instead
        //   As discussed with Lars, the way forward might be to create a "ProvidesPods" trait
        //   which the Operator needs to implement, which would enable the framework to ask for
        //   the pod that should be created for this "node, role, rolegroup" combination (all of
        //   which are provided by the labels, so should be available)
        Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(5)))
    }

    pub async fn handle_stop(&mut self, command: CommandRef) -> ZookeeperReconcileResult {
        info!("Stopping cluster [{:?}]", command);
        let mut stop_command: Stop = materialize_command(&self.context.client, &command).await?;

        let patch = Command::start_patch(&mut stop_command);
        self.context
            .client
            .merge_patch_status(&stop_command, &patch)
            .await?;

        // The returned value from this indicates if all eligible pods have been stopped already
        // if this returns ::Done the command has been finished, we can remove it from the status.
        match self.context.default_stop(&stop_command).await {
            Ok(ReconcileFunctionAction::Done) => {
                clear_current_command(&self.context.client, &mut self.context.resource).await?;

                let patch = Command::finish_patch(&mut stop_command);
                self.context
                    .client
                    .merge_patch_status(&stop_command, &patch)
                    .await?;

                Ok(ReconcileFunctionAction::Done)
            }
            Ok(_) => Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(5))),
            Err(e) => Err(Error::OperatorError { source: e }),
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
    if let Err(error) = stackable_operator::crd::wait_until_crds_present(
        &client,
        vec![ZookeeperCluster::crd_name()],
        None,
    )
    .await
    {
        error!("Required CRDs missing, aborting: {:?}", error);
        return Err(error);
    };

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
