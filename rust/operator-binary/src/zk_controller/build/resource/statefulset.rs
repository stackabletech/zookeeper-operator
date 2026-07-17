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
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMapVolumeSource, EmptyDirVolumeSource, EnvVar, EnvVarSource, ExecAction,
                ObjectFieldSelector, PersistentVolumeClaim, PodSecurityContext, Probe,
                ResourceRequirements, Volume,
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
        builder::pod::container::{EnvVarName, EnvVarSet},
        product_logging::framework::{ValidatedContainerLogConfigChoice, vector_container},
        types::{
            kubernetes::{ContainerName, VolumeName},
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
            UNVERSIONED_PRODUCT_VERSION,
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

// Volume names. Each is shared between a `Volume`/PVC definition and one or more volume mounts; the
// strings must match, so they are defined once here rather than repeated at every call site.
stackable_operator::constant!(DATA_VOLUME_NAME: VolumeName = "data");
stackable_operator::constant!(CONFIG_VOLUME_NAME: VolumeName = "config");
stackable_operator::constant!(RW_CONFIG_VOLUME_NAME: VolumeName = "rwconfig");
stackable_operator::constant!(LOG_VOLUME_NAME: VolumeName = "log");
stackable_operator::constant!(LOG_CONFIG_VOLUME_NAME: VolumeName = "log-config");

/// Name of the `prepare` init container (also used as its log subdirectory).
const PREPARE_CONTAINER_NAME: &str = "prepare";

stackable_operator::constant!(VECTOR_CONTAINER_NAME: ContainerName = "vector");

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

    #[snafu(display("failed to build listener volume"))]
    BuildListenerPersistentVolume {
        source: stackable_operator::builder::pod::volume::ListenerOperatorVolumeSourceBuilderError,
    },
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
pub fn build_server_rolegroup_statefulset(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    rolegroup_config: &ZookeeperRoleGroupConfig,
) -> Result<StatefulSet> {
    let merged_config = &rolegroup_config.config;
    let resource_names = cluster.resource_names(role_group_name);
    let resolved_product_image = &cluster.image;
    let zookeeper_security = &cluster.cluster_config.zookeeper_security;
    let metrics_port = cluster.metrics_http_port(rolegroup_config);

    // The operator-injected environment variables plus the user-provided `envOverrides`
    // (which win on conflict).
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
        .build_pvc(DATA_VOLUME_NAME.as_ref(), Some(vec!["ReadWriteOnce"]));
    let original_pvcs = vec![data_pvc];
    let resources: ResourceRequirements = resources_config.into();

    let mut cb_prepare =
        ContainerBuilder::new(PREPARE_CONTAINER_NAME).expect("invalid hard-coded container name");
    let mut cb_zookeeper =
        ContainerBuilder::new(APP_NAME).expect("invalid hard-coded container name");
    let mut pod_builder = PodBuilder::new();

    // Used for PVC templates that cannot be modified once they are deployed. A constant version
    // keeps the labels stable across version upgrades.
    let unversioned_recommended_labels =
        cluster.recommended_labels_for(&UNVERSIONED_PRODUCT_VERSION, role_group_name);

    let listener_pvc = build_role_listener_pvc(
        role_listener_name(cluster.name.as_ref(), &ZookeeperRole::Server).as_ref(),
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

    if let ValidatedContainerLogConfigChoice::Automatic(log_config) =
        &rolegroup_config.config.logging.prepare_container
    {
        args.push(product_logging::framework::capture_shell_output(
            STACKABLE_LOG_DIR,
            PREPARE_CONTAINER_NAME,
            log_config,
        ));
    }
    args.extend(create_init_container_command_args());

    let container_prepare = cb_prepare
        .image_from_product_image(resolved_product_image)
        .command(container_command())
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
        .add_env_vars(env_vars)
        .add_env_var(
            "ZK_SERVER_HEAP",
            construct_zk_server_heap_env(merged_config).context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            "SERVER_JVMFLAGS",
            construct_non_heap_jvm_args(rolegroup_config),
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
        .with_labels(cluster.recommended_labels(role_group_name))
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
        .security_context(PodSecurityContext {
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        })
        .service_account_name(cluster.rbac_service_account_name());

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

    let metadata = cluster
        .object_meta(
            resource_names.stateful_set_name().to_string(),
            role_group_name,
        )
        .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
        .build();

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("Parallel".to_string()),
        // `None` (no replica count specified) leaves `.spec.replicas` unset so a
        // HorizontalPodAutoscaler can manage the replica count.
        replicas: rolegroup_config.replicas.map(i32::from),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        minimal_zk, minimal_zk_yaml, server_rolegroup_config, validated_cluster,
    };

    /// Builds the `default` server StatefulSet for `yaml` and returns the ConfigMap name mounted by
    /// its `log-config` volume.
    fn log_config_map_name(yaml: &str) -> String {
        let validated = validated_cluster(&minimal_zk(yaml));
        let (rg_name, rg) = server_rolegroup_config(&validated, "default");
        build_server_rolegroup_statefulset(&validated, &rg_name, rg)
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
        let name = log_config_map_name(&minimal_zk_yaml(1));
        assert_ne!(name, "my-log-config");
        assert!(name.contains("simple-zookeeper"), "{name}");
    }

    // ---------------------------------------------------------------------------------------------
    // StatefulSet resource shapes.
    //
    // These own what the kuttl smoke `13-assert` checked on the StatefulSet spec/metadata: replicas,
    // `podManagementPolicy`, `serviceName`, graceful-shutdown period, container resources (incl. the
    // `podOverrides` merge), the `data`/`listener` PVC templates and the heap env var. The `status:`
    // stanza (`readyReplicas`) is a runtime signal and stays in kuttl.
    // ---------------------------------------------------------------------------------------------

    fn build_sts(yaml: &str, role_group: &str) -> StatefulSet {
        let validated = validated_cluster(&minimal_zk(yaml));
        let (rg_name, rg) = server_rolegroup_config(&validated, role_group);
        build_server_rolegroup_statefulset(&validated, &rg_name, rg).expect("statefulset builds")
    }

    fn zookeeper_container(
        sts: &StatefulSet,
    ) -> &stackable_operator::k8s_openapi::api::core::v1::Container {
        sts.spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers
            .iter()
            .find(|c| c.name == APP_NAME)
            .expect("zookeeper container")
    }

    #[test]
    fn statefulset_shape_defaults() {
        let sts = build_sts(
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
                    replicas: 2
            "#,
            "default",
        );

        let spec = sts.spec.as_ref().unwrap();
        assert_eq!(spec.pod_management_policy.as_deref(), Some("Parallel"));
        assert_eq!(spec.replicas, Some(2));
        assert_eq!(
            spec.service_name.as_deref(),
            Some("simple-zookeeper-server-default-headless")
        );

        let pod_spec = spec.template.spec.as_ref().unwrap();
        // Default graceful-shutdown timeout -> terminationGracePeriodSeconds.
        assert_eq!(pod_spec.termination_grace_period_seconds, Some(120));
        assert_eq!(
            pod_spec.service_account_name.as_deref(),
            Some("simple-zookeeper-serviceaccount")
        );

        let labels = sts.metadata.labels.as_ref().unwrap();
        assert_eq!(
            labels
                .get("app.kubernetes.io/component")
                .map(String::as_str),
            Some("server")
        );
        assert_eq!(
            labels
                .get("app.kubernetes.io/role-group")
                .map(String::as_str),
            Some("default")
        );
        // The restarter label lets the restart controller pick up config changes.
        assert_eq!(
            labels
                .get("restarter.stackable.tech/enabled")
                .map(String::as_str),
            Some("true")
        );

        let owner = &sts.metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(owner.kind, "ZookeeperCluster");
        assert_eq!(owner.name, "simple-zookeeper");
        assert_eq!(owner.controller, Some(true));
    }

    #[test]
    fn statefulset_unset_replicas_leaves_spec_replicas_none() {
        // An unset replica count leaves `.spec.replicas` unset so an HPA can manage it.
        let sts = build_sts(
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
                  default: {}
            "#,
            "default",
        );
        assert_eq!(sts.spec.as_ref().unwrap().replicas, None);
    }

    #[test]
    fn pod_overrides_merge_into_container_resources() {
        // The role sets the base resources; a rolegroup `podOverride` overrides only cpu, and the
        // k8s deep-merge must keep the un-overridden memory. Mirrors the kuttl `secondary` group.
        let sts = build_sts(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: simple-zookeeper
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                config:
                  resources:
                    cpu:
                      min: 250m
                      max: 500m
                    memory:
                      limit: 512Mi
                roleGroups:
                  default:
                    replicas: 1
                    podOverrides:
                      spec:
                        containers:
                          - name: zookeeper
                            resources:
                              requests:
                                cpu: 300m
                              limits:
                                cpu: 600m
            "#,
            "default",
        );

        let resources = zookeeper_container(&sts).resources.as_ref().unwrap();
        let requests = resources.requests.as_ref().unwrap();
        let limits = resources.limits.as_ref().unwrap();
        // cpu comes from the podOverride ...
        assert_eq!(requests.get("cpu").unwrap().0, "300m");
        assert_eq!(limits.get("cpu").unwrap().0, "600m");
        // ... memory is untouched by the merge (Stackable sets memory request == limit).
        assert_eq!(requests.get("memory").unwrap().0, "512Mi");
        assert_eq!(limits.get("memory").unwrap().0, "512Mi");
    }

    #[test]
    fn volume_claim_templates_carry_data_and_listener_pvcs() {
        let sts = build_sts(
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
                      resources:
                        storage:
                          data:
                            capacity: 2Gi
            "#,
            "default",
        );

        let pvcs = sts
            .spec
            .as_ref()
            .unwrap()
            .volume_claim_templates
            .as_ref()
            .unwrap();

        let data = pvcs
            .iter()
            .find(|p| p.metadata.name.as_deref() == Some("data"))
            .expect("data PVC template");
        let storage = data
            .spec
            .as_ref()
            .unwrap()
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap()
            .get("storage")
            .unwrap();
        assert_eq!(storage.0, "2Gi");

        // The listener PVC template is always appended alongside the data PVC.
        assert!(
            pvcs.iter()
                .any(|p| p.metadata.name.as_deref() == Some(LISTENER_VOLUME_NAME)),
            "missing listener PVC template"
        );
    }

    #[test]
    fn statefulset_sets_zk_server_heap_env() {
        // The heap value computed in jvm.rs lands on the STS as `ZK_SERVER_HEAP`:
        // 512Mi default memory limit x 0.8 -> 409 MiB (calculation pinned by the jvm.rs tests).
        let sts = build_sts(&minimal_zk_yaml(1), "default");

        let heap = zookeeper_container(&sts)
            .env
            .as_ref()
            .unwrap()
            .iter()
            .find(|e| e.name == "ZK_SERVER_HEAP")
            .expect("ZK_SERVER_HEAP env var");
        assert_eq!(heap.value.as_deref(), Some("409"));
    }

    #[test]
    fn vector_agent_adds_vector_container_to_statefulset() {
        // Enabling the Vector agent wires a `vector` sidecar onto the StatefulSet, mounting the
        // `config` (for `vector.yaml`) and `log` volumes. The env var format comes from the
        // upstream `vector_container` helper, but the identity values (cluster/role/role-group) and
        // the aggregator ConfigMap reference are wiring this operator supplies, so those are pinned
        // here (they were previously only checked by the kuttl smoke `14-assert` heredoc).
        let sts = build_sts(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: simple-zookeeper
            spec:
              image:
                productVersion: "3.9.5"
              clusterConfig:
                vectorAggregatorConfigMapName: vector-aggregator-discovery
              servers:
                roleGroups:
                  default:
                    replicas: 1
                    config:
                      logging:
                        enableVectorAgent: true
            "#,
            "default",
        );

        let vector = sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers
            .iter()
            .find(|c| c.name == VECTOR_CONTAINER_NAME.as_ref())
            .expect("vector container");

        let mount_names: Vec<&str> = vector
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .map(|m| m.name.as_str())
            .collect();
        assert!(
            mount_names.contains(&CONFIG_VOLUME_NAME.as_ref()),
            "vector container missing `config` volume mount: {mount_names:?}"
        );
        assert!(
            mount_names.contains(&LOG_VOLUME_NAME.as_ref()),
            "vector container missing `log` volume mount: {mount_names:?}"
        );

        // Identity wiring this operator passes into the upstream `vector_container` helper.
        let env = vector.env.as_ref().expect("vector container env");
        let env_value = |name: &str| {
            env.iter()
                .find(|e| e.name == name)
                .unwrap_or_else(|| panic!("vector env {name} missing"))
                .value
                .as_deref()
        };
        assert_eq!(env_value("CLUSTER_NAME"), Some("simple-zookeeper"));
        assert_eq!(env_value("ROLE_NAME"), Some("server"));
        assert_eq!(env_value("ROLE_GROUP_NAME"), Some("default"));

        // The aggregator address resolves from the discovery ConfigMap named in `clusterConfig`.
        let aggregator = env
            .iter()
            .find(|e| e.name == "VECTOR_AGGREGATOR_ADDRESS")
            .expect("VECTOR_AGGREGATOR_ADDRESS env var")
            .value_from
            .as_ref()
            .and_then(|source| source.config_map_key_ref.as_ref())
            .expect("VECTOR_AGGREGATOR_ADDRESS resolved from a ConfigMap key");
        assert_eq!(aggregator.name, "vector-aggregator-discovery");
        assert_eq!(aggregator.key, "ADDRESS");
    }

    #[test]
    fn no_vector_container_without_agent() {
        // Sanity counterpart: with the agent disabled (the default), the StatefulSet carries no
        // `vector` sidecar.
        let sts = build_sts(&minimal_zk_yaml(1), "default");

        let has_vector = sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers
            .iter()
            .any(|c| c.name == VECTOR_CONTAINER_NAME.as_ref());
        assert!(!has_vector, "unexpected vector container without the agent");
    }

    #[test]
    fn env_overrides_apply_with_role_group_precedence() {
        // `envOverrides` land on the zookeeper container, with the rolegroup value winning over the
        // role value on conflict: COMMON_VAR is set at both levels (group wins), ROLE_VAR only at
        // the role, GROUP_VAR only at the group.
        let sts = build_sts(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: simple-zookeeper
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                envOverrides:
                  COMMON_VAR: role-value
                  ROLE_VAR: role-value
                roleGroups:
                  primary:
                    replicas: 1
                    envOverrides:
                      COMMON_VAR: group-value
                      GROUP_VAR: group-value
            "#,
            "primary",
        );

        let env = zookeeper_container(&sts).env.as_ref().unwrap();
        let env_value = |name: &str| {
            env.iter()
                .find(|e| e.name == name)
                .unwrap_or_else(|| panic!("missing env var {name}"))
                .value
                .as_deref()
        };

        // Rolegroup value overrides the role value on conflict.
        assert_eq!(env_value("COMMON_VAR"), Some("group-value"));
        // Role-only and group-only overrides are both present.
        assert_eq!(env_value("ROLE_VAR"), Some("role-value"));
        assert_eq!(env_value("GROUP_VAR"), Some("group-value"));
    }
}
