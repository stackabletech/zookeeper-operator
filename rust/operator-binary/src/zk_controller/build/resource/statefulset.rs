//! Builds the rolegroup [`StatefulSet`] that runs the ZooKeeper servers.

use std::str::FromStr;

use indoc::formatdoc;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            volume::{ListenerOperatorVolumeSourceBuilder, ListenerReference},
        },
    },
    commons::product_image_selection::ResolvedProductImage,
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMapVolumeSource, EmptyDirVolumeSource, EnvVar, EnvVarSource, ExecAction,
                ObjectFieldSelector, PersistentVolumeClaim, PodSecurityContext, Probe,
                ResourceRequirements, ServiceAccount, Volume,
            },
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::ResourceExt,
    kvp::Labels,
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{
        self,
        framework::{
            LoggingError, create_vector_shutdown_file_command, remove_vector_shutdown_file_command,
        },
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    utils::COMMON_BASH_TRAP_FUNCTIONS,
    v2::{
        builder::{
            meta::ownerreference_from_resource,
            pod::container::{EnvVarName, EnvVarSet},
        },
        types::operator::{ProductVersion, RoleGroupName},
    },
};

use crate::{
    APP_NAME,
    crd::{
        JMX_METRICS_PORT_NAME, METRICS_PROVIDER_HTTP_PORT_NAME, STACKABLE_CONFIG_DIR,
        STACKABLE_DATA_DIR, STACKABLE_LOG_CONFIG_DIR, STACKABLE_LOG_DIR, STACKABLE_RW_CONFIG_DIR,
        ZOOKEEPER_ELECTION_PORT, ZOOKEEPER_ELECTION_PORT_NAME, ZOOKEEPER_LEADER_PORT,
        ZOOKEEPER_LEADER_PORT_NAME, ZOOKEEPER_SERVER_PORT_NAME, ZookeeperRole, role_listener_name,
        security::{self, ZookeeperSecurity},
        v1alpha1,
    },
    zk_controller::{
        LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME,
        build::{
            command::create_init_container_command_args,
            graceful_shutdown::add_graceful_shutdown_config,
            jvm::{construct_non_heap_jvm_args, construct_zk_server_heap_env},
            properties::{self, ConfigFileName},
        },
        validate::{ValidatedCluster, ZookeeperRoleGroupConfig},
    },
};

type Result<T, E = Error> = std::result::Result<T, E>;

/// Maximum size of the `prepare` init container log file (before rotation).
const MAX_PREPARE_LOG_FILE_SIZE: MemoryQuantity = MemoryQuantity {
    value: 1.0,
    unit: BinaryMultiple::Mebi,
};

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("failed to add TLS volume mounts"))]
    AddTlsVolumeMounts { source: security::Error },

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging { source: LoggingError },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArguments {
        source: crate::zk_controller::build::jvm::Error,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::zk_controller::build::graceful_shutdown::Error,
    },

    #[snafu(display("failed to build listener volume"))]
    BuildListenerPersistentVolume {
        source: stackable_operator::builder::pod::volume::ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,
}

fn build_role_listener_pvc(
    group_listener_name: &str,
    unversioned_recommended_labels: &Labels,
) -> Result<PersistentVolumeClaim> {
    ListenerOperatorVolumeSourceBuilder::new(
        &ListenerReference::ListenerName(group_listener_name.to_string()),
        unversioned_recommended_labels,
    )
    .build_pvc(LISTENER_VOLUME_NAME.to_string())
    .context(BuildListenerPersistentVolumeSnafu)
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the
/// corresponding headless
/// [`Service`](`stackable_operator::k8s_openapi::api::core::v1::Service`) (from
/// [`build_server_rolegroup_headless_service`](super::service::build_server_rolegroup_headless_service)).
#[allow(clippy::too_many_arguments)]
pub fn build_server_rolegroup_statefulset(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    rolegroup_config: &ZookeeperRoleGroupConfig,
    zookeeper_security: &ZookeeperSecurity,
    resolved_product_image: &ResolvedProductImage,
    metrics_port: u16,
    service_account: &ServiceAccount,
) -> Result<StatefulSet> {
    let merged_config = &rolegroup_config.config;
    let logging = &merged_config.logging;
    let resource_names = cluster.resource_names(role_group_name);

    // The operator-injected environment variables (formerly produced by the
    // product-config `Configuration::compute_env` implementation) plus the
    // user-provided `envOverrides` (which win on conflict).
    let env_vars = EnvVarSet::new()
        .with_value(
            &EnvVarName::from_str_unsafe(v1alpha1::ZookeeperConfig::MYID_OFFSET),
            merged_config.myid_offset.to_string(),
        )
        // Used by zkEnv.sh and the shell scripts in bin/. If unset it tries to find the
        // conf directory automatically and that fails.
        .with_value(
            &EnvVarName::from_str_unsafe("ZOOCFGDIR"),
            STACKABLE_RW_CONFIG_DIR,
        )
        .merge(rolegroup_config.env_overrides.clone());

    // Build the `data` PVC and the container resource requirements from the merged config.
    // The precedence (role group > role > default) is already resolved in the validate step.
    let resources_config = merged_config.resources.clone();
    let data_pvc = resources_config
        .storage
        .data
        .build_pvc("data", Some(vec!["ReadWriteOnce"]));
    let original_pvcs = vec![data_pvc];
    let resources: ResourceRequirements = resources_config.into();

    let mut cb_prepare =
        ContainerBuilder::new("prepare").expect("invalid hard-coded container name");
    let mut cb_zookeeper =
        ContainerBuilder::new(APP_NAME).expect("invalid hard-coded container name");
    let mut pod_builder = PodBuilder::new();

    // Used for PVC templates that cannot be modified once they are deployed. A version value is
    // required, so a constant "none" is used to keep the labels stable across version upgrades.
    let unversioned_recommended_labels = cluster.recommended_labels_for(
        &ProductVersion::from_str("none").expect("'none' is a valid product version"),
        role_group_name,
    );

    let listener_pvc = build_role_listener_pvc(
        &role_listener_name(cluster.name.as_ref(), &ZookeeperRole::Server),
        &unversioned_recommended_labels,
    )?;

    let mut pvcs = original_pvcs;
    pvcs.extend([listener_pvc]);

    cb_zookeeper
        .add_volume_mount(LISTENER_VOLUME_NAME, LISTENER_VOLUME_DIR)
        .context(AddVolumeMountSnafu)?;

    let requested_secret_lifetime = merged_config
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    // add volumes and mounts depending on tls / auth settings
    zookeeper_security
        .add_volume_mounts(
            &mut pod_builder,
            &mut cb_zookeeper,
            &requested_secret_lifetime,
        )
        .context(AddTlsVolumeMountsSnafu)?;

    let mut args = Vec::new();

    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&v1alpha1::Container::Prepare)
    {
        args.push(product_logging::framework::capture_shell_output(
            STACKABLE_LOG_DIR,
            "prepare",
            log_config,
        ));
    }
    args.extend(create_init_container_command_args());

    let container_prepare = cb_prepare
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![args.join("\n")])
        .add_env_vars(env_vars.clone())
        .add_env_vars(vec![EnvVar {
            name: "POD_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_string()),
                    field_path: "metadata.name".to_string(),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        }])
        .add_volume_mount("data", STACKABLE_DATA_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("config", STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("rwconfig", STACKABLE_RW_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("200m")
                .with_cpu_limit("800m")
                .with_memory_request("512Mi")
                .with_memory_limit("512Mi")
                .build(),
        )
        .build();

    let container_zk = cb_zookeeper
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![formatdoc! {"
            {COMMON_BASH_TRAP_FUNCTIONS}
            {remove_vector_shutdown_file_command}
            prepare_signal_handlers
            containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &
            bin/zkServer.sh start-foreground {STACKABLE_RW_CONFIG_DIR}/{zoo_cfg} &
            wait_for_termination $!
            {create_vector_shutdown_file_command}
            ",
            zoo_cfg = ConfigFileName::ZooCfg,
            remove_vector_shutdown_file_command =
                remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
            create_vector_shutdown_file_command =
                create_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        }])
        .add_env_vars(env_vars)
        .add_env_var(
            "ZK_SERVER_HEAP",
            construct_zk_server_heap_env(merged_config).context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            "SERVER_JVMFLAGS",
            construct_non_heap_jvm_args(rolegroup_config, &resolved_product_image.product_version),
        )
        .add_env_var(
            "CONTAINERDEBUG_LOG_DIRECTORY",
            format!("{STACKABLE_LOG_DIR}/containerdebug"),
        )
        // Only allow the global load balancing service to send traffic to pods that are members of the quorum
        // This also acts as a hint to the StatefulSet controller to wait for each pod to enter quorum before taking down the next
        .readiness_probe(Probe {
            exec: Some(ExecAction {
                command: Some(vec![
                    "bash".to_string(),
                    "-c".to_string(),
                    // We don't have telnet or netcat in the container images, but
                    // we can use Bash's virtual /dev/tcp filesystem to accomplish the same thing
                    format!(
                        "exec 3<>/dev/tcp/127.0.0.1/{} && echo srvr >&3 && grep '^Mode: ' <&3",
                        zookeeper_security.client_port()
                    ),
                ]),
            }),
            period_seconds: Some(1),
            ..Probe::default()
        })
        .add_container_port(
            ZOOKEEPER_SERVER_PORT_NAME,
            zookeeper_security.client_port() as i32,
        )
        .add_container_port(ZOOKEEPER_LEADER_PORT_NAME, ZOOKEEPER_LEADER_PORT as i32)
        .add_container_port(ZOOKEEPER_ELECTION_PORT_NAME, ZOOKEEPER_ELECTION_PORT as i32)
        .add_container_port(JMX_METRICS_PORT_NAME, 9505)
        .add_container_port(METRICS_PROVIDER_HTTP_PORT_NAME, metrics_port.into())
        .add_volume_mount("data", STACKABLE_DATA_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("config", STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log-config", STACKABLE_LOG_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("rwconfig", STACKABLE_RW_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
        .resources(resources)
        .build();

    let pb_metadata = ObjectMetaBuilder::new()
        .with_labels(cluster.recommended_labels(role_group_name))
        .build();

    pod_builder
        .metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_init_container(container_prepare)
        .add_container(container_zk)
        .affinity(&merged_config.affinity)
        .add_volume(Volume {
            name: "config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: resource_names.role_group_config_map().to_string(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        .add_volume(Volume {
            empty_dir: Some(EmptyDirVolumeSource {
                medium: None,
                size_limit: None,
            }),
            name: "rwconfig".to_string(),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        .add_empty_dir_volume(
            "log",
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[
                    properties::logging::MAX_ZK_LOG_FILES_SIZE,
                    MAX_PREPARE_LOG_FILE_SIZE,
                ],
            )),
        )
        .context(AddVolumeSnafu)?
        .security_context(PodSecurityContext {
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        })
        .service_account_name(service_account.name_any());

    // Use the user-provided custom log ConfigMap if one is configured, otherwise fall back to the
    // rolegroup's own ConfigMap.
    let log_config_map = if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = logging.containers.get(&v1alpha1::Container::Zookeeper)
    {
        config_map.into()
    } else {
        resource_names.role_group_config_map().to_string()
    };
    pod_builder
        .add_volume(Volume {
            name: "log-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: log_config_map,
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?;

    if logging.enable_vector_agent {
        match &cluster.cluster_config.vector_aggregator_config_map_name {
            Some(vector_aggregator_config_map_name) => {
                pod_builder.add_container(
                    product_logging::framework::vector_container(
                        resolved_product_image,
                        "config",
                        "log",
                        logging.containers.get(&v1alpha1::Container::Vector),
                        ResourceRequirementsBuilder::new()
                            .with_cpu_request("250m")
                            .with_cpu_limit("500m")
                            .with_memory_request("128Mi")
                            .with_memory_limit("128Mi")
                            .build(),
                        vector_aggregator_config_map_name,
                    )
                    .context(ConfigureLoggingSnafu)?,
                );
            }
            None => {
                VectorAggregatorConfigMapMissingSnafu.fail()?;
            }
        }
    }

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;

    let mut pod_template = pod_builder.build_template();
    pod_template.merge_from(rolegroup_config.pod_overrides.clone());

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(cluster)
        .name(resource_names.stateful_set_name().to_string())
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(cluster.recommended_labels(role_group_name))
        .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
        .build();

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("Parallel".to_string()),
        replicas: Some(i32::from(rolegroup_config.replicas)),
        selector: LabelSelector {
            match_labels: Some(cluster.role_group_selector(role_group_name).into()),
            ..LabelSelector::default()
        },
        service_name: Some(resource_names.headless_service_name().to_string()),
        template: pod_template,
        volume_claim_templates: Some(pvcs),
        ..StatefulSetSpec::default()
    };

    Ok(StatefulSet {
        metadata,
        spec: Some(statefulset_spec),
        status: None,
    })
}
