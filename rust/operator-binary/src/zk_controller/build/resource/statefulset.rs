//! Builds the rolegroup [`StatefulSet`] that runs the ZooKeeper servers.

use std::str::FromStr;

use indoc::formatdoc;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder, container::FieldPathEnvVar, resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder,
        },
    },
    constant,
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMapVolumeSource, EmptyDirVolumeSource, ExecAction, PersistentVolumeClaim,
                Probe, ResourceRequirements, Volume,
            },
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kvp::Labels,
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{
        self,
        framework::{create_vector_shutdown_file_command, remove_vector_shutdown_file_command},
    },
    utils::COMMON_BASH_TRAP_FUNCTIONS,
    v2::{
        builder::pod::{
            container::{EnvVarName, EnvVarSet, new_container_builder},
            volume::{ListenerReference, listener_operator_volume_source_builder_build_pvc},
        },
        product_logging::framework::{ValidatedContainerLogConfigChoice, vector_container},
        types::{
            kubernetes::{ContainerName, ListenerName, PersistentVolumeClaimName, VolumeName},
            operator::RoleGroupName,
        },
    },
};

use crate::{
    APP_NAME,
    crd::{
        JMX_METRICS_PORT, JMX_METRICS_PORT_NAME, METRICS_PROVIDER_HTTP_PORT_NAME,
        STACKABLE_CONFIG_DIR, STACKABLE_DATA_DIR, STACKABLE_LOG_CONFIG_DIR, STACKABLE_LOG_DIR,
        STACKABLE_RW_CONFIG_DIR, ZOOKEEPER_ELECTION_PORT, ZOOKEEPER_ELECTION_PORT_NAME,
        ZOOKEEPER_LEADER_PORT, ZOOKEEPER_LEADER_PORT_NAME, ZOOKEEPER_SERVER_PORT_NAME,
        ZookeeperRole, role_listener_name, security, v1alpha1,
    },
    zk_controller::{
        LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME,
        build::{
            command::create_init_container_command_args,
            graceful_shutdown::add_graceful_shutdown_config,
            jvm::{construct_non_heap_jvm_args, construct_zk_server_heap_env},
            object_meta,
            properties::{self, ConfigFileName},
            recommended_labels_for_role_group_resources,
            recommended_labels_for_unversioned_role_group_resources, role_group_selector,
        },
        validate::{ValidatedCluster, ValidatedZookeeperConfig, ZookeeperRoleGroupConfig},
    },
};

type Result<T, E = Error> = std::result::Result<T, E>;

/// Maximum size of the `prepare` init container log file (before rotation).
const MAX_PREPARE_LOG_FILE_SIZE: MemoryQuantity = MemoryQuantity {
    value: 1.0,
    unit: BinaryMultiple::Mebi,
};

// Volume names. Each is shared between a `Volume`/PVC definition and one or more volume mounts; the
// strings must match, so they are defined once here rather than repeated at every call site.
constant!(DATA_VOLUME_NAME: VolumeName = "data");
constant!(CONFIG_VOLUME_NAME: VolumeName = "config");
constant!(RW_CONFIG_VOLUME_NAME: VolumeName = "rwconfig");
constant!(LOG_VOLUME_NAME: VolumeName = "log");
constant!(LOG_CONFIG_VOLUME_NAME: VolumeName = "log-config");

// The listener volume is provisioned as a PVC by the listener-operator; this is its typed name.
// It must match `LISTENER_VOLUME_NAME`, by which the volume mount and the secret-operator volume
// scope reference it.
constant!(LISTENER_PVC_NAME: PersistentVolumeClaimName = "listener");

// Container names. These must match the corresponding (kebab-cased) `crate::crd::Container`
// variants, which key the per-container logging config. The prepare container name is also used
// as its log subdirectory.
constant!(PREPARE_CONTAINER_NAME: ContainerName = "prepare");
constant!(ZOOKEEPER_CONTAINER_NAME: ContainerName = APP_NAME);
constant!(VECTOR_CONTAINER_NAME: ContainerName = "vector");

// Env vars the operator sets on the containers.
constant!(POD_NAME: EnvVarName = "POD_NAME");
constant!(MYID_OFFSET: EnvVarName = v1alpha1::ZookeeperConfig::MYID_OFFSET);
// Used by zkEnv.sh and the shell scripts in bin/. If unset the scripts try to find the conf
// directory automatically and that fails.
constant!(ZOOCFGDIR: EnvVarName = "ZOOCFGDIR");
constant!(ZK_SERVER_HEAP: EnvVarName = "ZK_SERVER_HEAP");
constant!(SERVER_JVMFLAGS: EnvVarName = "SERVER_JVMFLAGS");
// Needed for the `containerdebug` process to log its tracing information to.
constant!(CONTAINERDEBUG_LOG_DIRECTORY: EnvVarName = "CONTAINERDEBUG_LOG_DIRECTORY");

/// The shell invocation shared by the `prepare` init container and the main ZooKeeper container.
fn container_command() -> Vec<String> {
    vec![
        "/bin/bash".to_string(),
        "-x".to_string(),
        "-euo".to_string(),
        "pipefail".to_string(),
        "-c".to_string(),
    ]
}

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("failed to add TLS volume mounts"))]
    AddTlsVolumeMounts { source: security::Error },

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
}

fn build_role_listener_pvc(
    role_listener_name: ListenerName,
    unversioned_recommended_labels: &Labels,
) -> PersistentVolumeClaim {
    listener_operator_volume_source_builder_build_pvc(
        &ListenerReference::Listener(role_listener_name),
        unversioned_recommended_labels,
        &LISTENER_PVC_NAME,
    )
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the
/// corresponding headless
/// [`Service`](`stackable_operator::k8s_openapi::api::core::v1::Service`) (from
/// [`build_server_rolegroup_headless_service`](super::service::build_server_rolegroup_headless_service)).
pub fn build_server_rolegroup_statefulset(
    cluster: &ValidatedCluster,
    zk_role: &ZookeeperRole,
    role_group_name: &RoleGroupName,
    rolegroup_config: &ZookeeperRoleGroupConfig,
) -> Result<StatefulSet> {
    let merged_config = &rolegroup_config.config;
    let resource_names = cluster.role_group_resource_names(role_group_name);
    let resolved_product_image = &cluster.image;
    let zookeeper_security = &cluster.cluster_config.zookeeper_security;
    let metrics_port = cluster.metrics_http_port(rolegroup_config);

    // Operator-set env vars first; the user's `envOverrides` are merged on top last and win.
    let prepare_env_vars = common_env_vars(merged_config)
        .with_field_path(&POD_NAME, &FieldPathEnvVar::Name)
        .merge(rolegroup_config.env_overrides.clone());

    let zookeeper_env_vars = common_env_vars(merged_config)
        .with_value(
            &ZK_SERVER_HEAP,
            construct_zk_server_heap_env(merged_config).context(ConstructJvmArgumentsSnafu)?,
        )
        .with_value(
            &SERVER_JVMFLAGS,
            construct_non_heap_jvm_args(rolegroup_config),
        )
        .with_value(
            &CONTAINERDEBUG_LOG_DIRECTORY,
            format!("{STACKABLE_LOG_DIR}/containerdebug"),
        )
        .merge(rolegroup_config.env_overrides.clone());

    // Build the `data` PVC and the container resource requirements from the merged config.
    // The precedence (role group > role > default) is already resolved in the validate step.
    let resources_config = merged_config.resources.clone();
    let data_pvc = resources_config
        .storage
        .data
        .build_pvc(DATA_VOLUME_NAME.as_ref(), Some(vec!["ReadWriteOnce"]));
    let original_pvcs = vec![data_pvc];
    let resources: ResourceRequirements = resources_config.into();

    let mut cb_prepare = new_container_builder(&PREPARE_CONTAINER_NAME);
    let mut cb_zookeeper = new_container_builder(&ZOOKEEPER_CONTAINER_NAME);
    let mut pod_builder = PodBuilder::new();

    // Used for PVC templates, which cannot be modified once they are deployed. The version label
    // is omitted so the labels stay stable across version upgrades.
    let unversioned_recommended_labels =
        recommended_labels_for_unversioned_role_group_resources(cluster, zk_role, role_group_name);

    let listener_pvc = build_role_listener_pvc(
        role_listener_name(cluster.name.as_ref(), zk_role),
        &unversioned_recommended_labels,
    );

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

    if let ValidatedContainerLogConfigChoice::Automatic(log_config) =
        &rolegroup_config.config.logging.prepare_container
    {
        args.push(product_logging::framework::capture_shell_output(
            STACKABLE_LOG_DIR,
            PREPARE_CONTAINER_NAME.as_ref(),
            log_config,
        ));
    }
    args.extend(create_init_container_command_args());

    let container_prepare = cb_prepare
        .image_from_product_image(resolved_product_image)
        .command(container_command())
        .args(vec![args.join("\n")])
        .add_env_vars(prepare_env_vars)
        .add_volume_mount(&*DATA_VOLUME_NAME, STACKABLE_DATA_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(&*CONFIG_VOLUME_NAME, STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(&*RW_CONFIG_VOLUME_NAME, STACKABLE_RW_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(&*LOG_VOLUME_NAME, STACKABLE_LOG_DIR)
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
        .command(container_command())
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
        .add_env_vars(zookeeper_env_vars)
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
            i32::from(zookeeper_security.client_port()),
        )
        .add_container_port(ZOOKEEPER_LEADER_PORT_NAME, i32::from(ZOOKEEPER_LEADER_PORT))
        .add_container_port(
            ZOOKEEPER_ELECTION_PORT_NAME,
            i32::from(ZOOKEEPER_ELECTION_PORT),
        )
        .add_container_port(JMX_METRICS_PORT_NAME, i32::from(JMX_METRICS_PORT))
        .add_container_port(METRICS_PROVIDER_HTTP_PORT_NAME, metrics_port.into())
        .add_volume_mount(&*DATA_VOLUME_NAME, STACKABLE_DATA_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(&*CONFIG_VOLUME_NAME, STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(&*LOG_CONFIG_VOLUME_NAME, STACKABLE_LOG_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(&*RW_CONFIG_VOLUME_NAME, STACKABLE_RW_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(&*LOG_VOLUME_NAME, STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
        .resources(resources)
        .build();

    let pb_metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels_for_role_group_resources(
            cluster,
            zk_role,
            role_group_name,
        ))
        .build();

    pod_builder
        .metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_init_container(container_prepare)
        .add_container(container_zk)
        .affinity(&merged_config.affinity)
        .add_volume(Volume {
            name: CONFIG_VOLUME_NAME.to_string(),
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
            name: RW_CONFIG_VOLUME_NAME.to_string(),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        .add_empty_dir_volume(
            &*LOG_VOLUME_NAME,
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[
                    properties::product_logging::MAX_ZK_LOG_FILES_SIZE,
                    MAX_PREPARE_LOG_FILE_SIZE,
                ],
            )),
        )
        .context(AddVolumeSnafu)?
        .security_context(
            PodSecurityContextBuilder::with_stackable_defaults()
                .fs_group(1000)
                .build(),
        )
        .service_account_name(cluster.cluster_resource_names().service_account_name());

    // Use the user-provided custom log ConfigMap if one is configured, otherwise fall back to the
    // rolegroup's own ConfigMap. This branches on the *validated* logging choice.
    let log_config_map = match &rolegroup_config.config.logging.zookeeper_container {
        ValidatedContainerLogConfigChoice::Custom(config_map) => config_map.to_string(),
        ValidatedContainerLogConfigChoice::Automatic(_) => {
            resource_names.role_group_config_map().to_string()
        }
    };
    pod_builder
        .add_volume(Volume {
            name: LOG_CONFIG_VOLUME_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: log_config_map,
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?;

    // The static `vector.yaml` (in the rolegroup ConfigMap, mounted as the `config` volume) is
    // parameterised at runtime via env vars that the `vector_container` injects. The validated
    // Vector log config is built up-front in the validate step.
    if let Some(vector_log_config) = &rolegroup_config.config.logging.vector_container {
        pod_builder.add_container(vector_container(
            &VECTOR_CONTAINER_NAME,
            resolved_product_image,
            vector_log_config,
            &resource_names,
            &CONFIG_VOLUME_NAME,
            &LOG_VOLUME_NAME,
            EnvVarSet::new(),
        ));
    }

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;

    let mut pod_template = pod_builder.build_template();
    pod_template.merge_from(rolegroup_config.pod_overrides.clone());

    let metadata = object_meta(
        cluster,
        resource_names.stateful_set_name().to_string(),
        recommended_labels_for_role_group_resources(cluster, zk_role, role_group_name),
    )
    .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
    .build();

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("Parallel".to_string()),
        // `None` (no replica count specified) leaves `.spec.replicas` unset so a
        // HorizontalPodAutoscaler can manage the replica count.
        replicas: rolegroup_config.replicas.map(i32::from),
        selector: LabelSelector {
            match_labels: Some(role_group_selector(cluster, zk_role, role_group_name).into()),
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

/// Environment variables the operator sets on both the `prepare` and the ZooKeeper container.
///
/// Returned as an [`EnvVarSet`] so the callers can merge the user's `envOverrides` on top,
/// letting an override win on a name collision.
fn common_env_vars(merged_config: &ValidatedZookeeperConfig) -> EnvVarSet {
    EnvVarSet::new()
        .with_value(&MYID_OFFSET, merged_config.myid_offset.to_string())
        .with_value(&ZOOCFGDIR, STACKABLE_RW_CONFIG_DIR)
}

#[cfg(test)]
mod tests {
    use stackable_operator::k8s_openapi::api::core::v1::{Container, EnvVar};

    use super::*;
    use crate::zk_controller::test_support::{minimal_zk, validated_cluster};

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *DATA_VOLUME_NAME;
        let _ = *CONFIG_VOLUME_NAME;
        let _ = *RW_CONFIG_VOLUME_NAME;
        let _ = *LOG_VOLUME_NAME;
        let _ = *LOG_CONFIG_VOLUME_NAME;
        let _ = *LISTENER_PVC_NAME;
        let _ = *PREPARE_CONTAINER_NAME;
        let _ = *ZOOKEEPER_CONTAINER_NAME;
        let _ = *VECTOR_CONTAINER_NAME;
        let _ = *POD_NAME;
        let _ = *MYID_OFFSET;
        let _ = *ZOOCFGDIR;
        let _ = *ZK_SERVER_HEAP;
        let _ = *SERVER_JVMFLAGS;
        let _ = *CONTAINERDEBUG_LOG_DIRECTORY;
    }

    /// Builds the `default` server StatefulSet for `yaml` and returns the ConfigMap name mounted by
    /// its `log-config` volume.
    fn log_config_map_name(yaml: &str) -> String {
        let validated = validated_cluster(&minimal_zk(yaml));
        let rg_name = RoleGroupName::from_str("default").expect("valid role group name");
        let rg = validated.role_group_configs[&ZookeeperRole::Server][&rg_name].clone();
        build_server_rolegroup_statefulset(&validated, &ZookeeperRole::Server, &rg_name, &rg)
            .expect("statefulset builds")
            .spec
            .and_then(|spec| spec.template.spec)
            .and_then(|pod| pod.volumes)
            .expect("pod volumes")
            .into_iter()
            .find(|volume| volume.name == "log-config")
            .expect("log-config volume")
            .config_map
            .expect("log-config is a ConfigMap volume")
            .name
    }

    #[test]
    fn custom_log_config_mounts_user_config_map() {
        // A custom log ConfigMap is mounted directly for the `log-config` volume.
        let name = log_config_map_name(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: simple-zookeeper
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleGroups:
                  default:
                    replicas: 1
                    config:
                      logging:
                        containers:
                          zookeeper:
                            custom:
                              configMap: my-log-config
            "#,
        );
        assert_eq!(name, "my-log-config");
    }

    #[test]
    fn automatic_log_config_mounts_rolegroup_config_map() {
        // Automatic logging mounts the role group's own ConfigMap, not a user-provided one.
        let name = log_config_map_name(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: simple-zookeeper
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleGroups:
                  default:
                    replicas: 1
            "#,
        );
        assert_ne!(name, "my-log-config");
        assert!(name.contains("simple-zookeeper"), "{name}");
    }

    /// Builds the `default` server StatefulSet with the given env override applied and returns the
    /// env vars of the container named `container_name`.
    fn env_with_override(container_name: &str, name: &EnvVarName, value: &str) -> Vec<EnvVar> {
        let validated = validated_cluster(&minimal_zk(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: simple-zookeeper
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleGroups:
                  default:
                    replicas: 1
            "#,
        ));
        let rg_name = RoleGroupName::from_str("default").expect("valid role group name");
        let mut rg = validated.role_group_configs[&ZookeeperRole::Server][&rg_name].clone();
        rg.env_overrides = rg.env_overrides.with_value(name, value);

        let stateful_set =
            build_server_rolegroup_statefulset(&validated, &ZookeeperRole::Server, &rg_name, &rg)
                .expect("statefulset builds");

        let pod_spec = stateful_set
            .spec
            .expect("the StatefulSet has a spec")
            .template
            .spec
            .expect("the pod template has a spec");
        pod_spec
            .containers
            .into_iter()
            .chain(pod_spec.init_containers.into_iter().flatten())
            .find(|container: &Container| container.name == container_name)
            .unwrap_or_else(|| panic!("the {container_name} container exists"))
            .env
            .unwrap_or_else(|| panic!("the {container_name} container has env vars"))
    }

    /// The user-supplied `envOverrides` must be merged in after all operator-set environment
    /// variables, so that they can override any of them. `CONTAINERDEBUG_LOG_DIRECTORY` is used
    /// as the example here because it is set unconditionally by the operator.
    #[test]
    fn env_overrides_override_operator_set_env_vars() {
        let env = env_with_override(
            ZOOKEEPER_CONTAINER_NAME.as_ref(),
            &CONTAINERDEBUG_LOG_DIRECTORY,
            "/custom/log/dir",
        );

        let containerdebug: Vec<_> = env
            .iter()
            .filter(|env_var| env_var.name == "CONTAINERDEBUG_LOG_DIRECTORY")
            .collect();
        assert_eq!(
            containerdebug.len(),
            1,
            "the override must replace the operator-set value, not duplicate it"
        );
        assert_eq!(containerdebug[0].value.as_deref(), Some("/custom/log/dir"));
    }

    /// Same guarantee for the `prepare` init container, whose env vars are assembled separately.
    #[test]
    fn prepare_env_overrides_override_operator_set_env_vars() {
        let env = env_with_override(
            PREPARE_CONTAINER_NAME.as_ref(),
            &ZOOCFGDIR,
            "/custom/conf/dir",
        );

        let zoocfgdir: Vec<_> = env
            .iter()
            .filter(|env_var| env_var.name == "ZOOCFGDIR")
            .collect();
        assert_eq!(
            zoocfgdir.len(),
            1,
            "the override must replace the operator-set value, not duplicate it"
        );
        assert_eq!(zoocfgdir[0].value.as_deref(), Some("/custom/conf/dir"));
    }
}
