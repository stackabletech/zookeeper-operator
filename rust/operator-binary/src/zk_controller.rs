//! Ensures that `Pod`s are configured and running for each [`v1alpha1::ZookeeperCluster`]
use std::{hash::Hasher, str::FromStr, sync::Arc};

use const_format::concatcp;
use fnv::FnvHasher;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::ClusterResourceApplyStrategy,
    commons::rbac::build_rbac_resources,
    kube::{
        api::DynamicObject,
        core::{DeserializeGuard, error_boundary},
        runtime::controller,
    },
    kvp::LabelError,
    logging::controller::ReconcilerError,
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    v2::{
        cluster_resources::cluster_resources_new,
        types::operator::{ControllerName, RoleGroupName},
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    APP_NAME, OPERATOR_NAME, ObjectRef,
    crd::{ZookeeperRole, v1alpha1},
    zk_controller::{
        build::resource::{
            config_map, discovery,
            listener::build_role_listener,
            pdb::build_pdb,
            service::{
                build_server_rolegroup_headless_service, build_server_rolegroup_metrics_service,
            },
            statefulset::build_server_rolegroup_statefulset,
        },
        validate::{operator_name, product_name},
    },
};

pub(crate) mod build;
pub(crate) mod dereference;
pub(crate) mod validate;

pub const ZK_CONTROLLER_NAME: &str = "zookeepercluster";
pub const ZK_FULL_CONTROLLER_NAME: &str = concatcp!(ZK_CONTROLLER_NAME, '.', OPERATOR_NAME);
pub const LISTENER_VOLUME_NAME: &str = "listener";
pub const LISTENER_VOLUME_DIR: &str = "/stackable/listener";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub operator_environment: OperatorEnvironmentOptions,
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("ZookeeperCluster object is invalid"))]
    InvalidZookeeperCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to dereference resources"))]
    Dereference { source: dereference::Error },

    #[snafu(display("failed to validate cluster"))]
    ValidateCluster { source: validate::Error },

    #[snafu(display("crd validation failure"))]
    CrdValidationFailure { source: crate::crd::Error },

    #[snafu(display("internal operator failure"))]
    InternalOperatorFailure { source: crate::crd::Error },

    #[snafu(display("failed to apply Service for role group {rolegroup}"))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupName,
    },

    #[snafu(display("failed to build ConfigMap for role group {rolegroup}"))]
    BuildRoleGroupConfigMap {
        source: config_map::Error,
        rolegroup: RoleGroupName,
    },

    #[snafu(display("failed to apply ConfigMap for role group {rolegroup}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupName,
    },

    #[snafu(display("failed to build StatefulSet for role group {rolegroup}"))]
    BuildRoleGroupStatefulSet {
        source: build::resource::statefulset::Error,
        rolegroup: RoleGroupName,
    },

    #[snafu(display("failed to apply StatefulSet for role group {rolegroup}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupName,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig { source: discovery::Error },

    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to create RBAC service account"))]
    ApplyServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to create RBAC role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphans {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply PodDisruptionBudget"))]
    ApplyPdb {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build label"))]
    BuildLabel { source: LabelError },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to apply group listener"))]
    ApplyGroupListener {
        source: stackable_operator::cluster_resources::Error,
    },
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::InvalidZookeeperCluster { .. } => None,
            Error::Dereference { .. } => None,
            Error::ValidateCluster { .. } => None,
            Error::CrdValidationFailure { .. } => None,
            Error::InternalOperatorFailure { .. } => None,
            Error::ApplyRoleGroupService { .. } => None,
            Error::BuildRoleGroupConfigMap { .. } => None,
            Error::ApplyRoleGroupConfig { .. } => None,
            Error::BuildRoleGroupStatefulSet { .. } => None,
            Error::ApplyRoleGroupStatefulSet { .. } => None,
            Error::ObjectMissingMetadataForOwnerRef { .. } => None,
            Error::BuildDiscoveryConfig { .. } => None,
            Error::ApplyDiscoveryConfig { .. } => None,
            Error::ApplyStatus { .. } => None,
            Error::ApplyServiceAccount { .. } => None,
            Error::ApplyRoleBinding { .. } => None,
            Error::BuildRbacResources { .. } => None,
            Error::DeleteOrphans { .. } => None,
            Error::ApplyPdb { .. } => None,
            Error::BuildLabel { .. } => None,
            Error::ObjectMeta { .. } => None,
            Error::ApplyGroupListener { .. } => None,
        }
    }
}

pub async fn reconcile_zk(
    zk: Arc<DeserializeGuard<v1alpha1::ZookeeperCluster>>,
    ctx: Arc<Ctx>,
) -> Result<controller::Action> {
    tracing::info!("Starting reconcile");
    let zk =
        zk.0.as_ref()
            .map_err(error_boundary::InvalidObject::clone)
            .context(InvalidZookeeperClusterSnafu)?;
    let client = &ctx.client;

    // dereference (client required)
    let dereferenced_objects = dereference::dereference(client, zk)
        .await
        .context(DereferenceSnafu)?;

    // validate (no client required)
    let validated_cluster =
        validate::validate(zk, &dereferenced_objects, &ctx.operator_environment)
            .context(ValidateClusterSnafu)?;

    // Names are derived from compile-time constants.
    let mut cluster_resources = cluster_resources_new(
        &product_name(),
        &operator_name(),
        &ControllerName::from_str(ZK_CONTROLLER_NAME)
            .expect("ZK_CONTROLLER_NAME should be a valid controller name"),
        &validated_cluster.name,
        &validated_cluster.namespace,
        &validated_cluster.uid,
        ClusterResourceApplyStrategy::from(&validated_cluster.cluster_operation),
        &validated_cluster.object_overrides,
    );

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        zk,
        APP_NAME,
        cluster_resources
            .get_required_labels()
            .context(BuildLabelSnafu)?,
    )
    .context(BuildRbacResourcesSnafu)?;

    cluster_resources
        .add(client, rbac_sa.clone())
        .await
        .context(ApplyServiceAccountSnafu)?;

    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    let zk_role = ZookeeperRole::Server;
    let server_role_group_configs = validated_cluster
        .role_group_configs
        .get(&zk_role)
        .into_iter()
        .flatten();
    for (rolegroup_name, rolegroup_config) in server_role_group_configs {
        // Resource naming, labels and owner references are derived from the `ValidatedCluster` and
        // the type-safe `RoleGroupName`.
        let rg_headless_service =
            build_server_rolegroup_headless_service(&validated_cluster, rolegroup_name);
        let rg_metrics_service = build_server_rolegroup_metrics_service(
            &validated_cluster,
            rolegroup_name,
            rolegroup_config,
        );
        let rg_configmap = config_map::build_server_rolegroup_config_map(
            &validated_cluster,
            &client.kubernetes_cluster_info,
            rolegroup_name,
            rolegroup_config,
        )
        .context(BuildRoleGroupConfigMapSnafu {
            rolegroup: rolegroup_name.clone(),
        })?;
        let rg_statefulset = build_server_rolegroup_statefulset(
            &validated_cluster,
            rolegroup_name,
            rolegroup_config,
        )
        .with_context(|_| BuildRoleGroupStatefulSetSnafu {
            rolegroup: rolegroup_name.clone(),
        })?;

        cluster_resources
            .add(client, rg_headless_service)
            .await
            .with_context(|_| ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup_name.clone(),
            })?;
        cluster_resources
            .add(client, rg_metrics_service)
            .await
            .with_context(|_| ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup_name.clone(),
            })?;
        cluster_resources
            .add(client, rg_configmap)
            .await
            .with_context(|_| ApplyRoleGroupConfigSnafu {
                rolegroup: rolegroup_name.clone(),
            })?;

        // Note: The StatefulSet needs to be applied after all ConfigMaps and Secrets it mounts
        // to prevent unnecessary Pod restarts.
        // See https://github.com/stackabletech/commons-operator/issues/111 for details.
        ss_cond_builder.add(
            cluster_resources
                .add(client, rg_statefulset)
                .await
                .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                    rolegroup: rolegroup_name.clone(),
                })?,
        );
    }

    if let Some(role_config) = &validated_cluster.role_config
        && let Some(pdb) = build_pdb(&role_config.pdb, &validated_cluster, &zk_role)
    {
        cluster_resources
            .add(client, pdb)
            .await
            .context(ApplyPdbSnafu)?;
    }

    let listener = build_role_listener(&validated_cluster, &zk_role);
    let applied_listener = cluster_resources
        .add(client, listener)
        .await
        .context(ApplyGroupListenerSnafu)?;

    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    let mut discovery_hash = FnvHasher::with_key(0);
    let discovery_cm = discovery::build_discovery_configmap(
        &validated_cluster,
        ZK_CONTROLLER_NAME,
        applied_listener,
    )
    .context(BuildDiscoveryConfigSnafu)?;

    let discovery_cm = cluster_resources
        .add(client, discovery_cm)
        .await
        .context(ApplyDiscoveryConfigSnafu)?;
    if let Some(generation) = discovery_cm.metadata.resource_version {
        discovery_hash.write(generation.as_bytes())
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&zk.spec.cluster_operation);

    let status = v1alpha1::ZookeeperClusterStatus {
        // Serialize as a string to discourage users from trying to parse the value,
        // and to keep things flexible if we end up changing the hasher at some point.
        discovery_hash: Some(discovery_hash.finish().to_string()),
        conditions: compute_conditions(zk, &[&ss_cond_builder, &cluster_operation_cond_builder]),
    };

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphansSnafu)?;
    client
        .apply_patch_status(OPERATOR_NAME, zk, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(controller::Action::await_change())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::ZookeeperCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> controller::Action {
    match error {
        // root object is invalid, will be requeued when modified anyway
        Error::InvalidZookeeperCluster { .. } => controller::Action::await_change(),

        _ => controller::Action::requeue(*Duration::from_secs(5)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use stackable_operator::k8s_openapi::api::core::v1::ConfigMap;

    use super::*;
    use crate::{
        test_support::{
            cluster_info, minimal_zk, minimal_zk_yaml, server_rolegroup_config, validated_cluster,
            validated_cluster_with_client_auth,
        },
        zk_controller::{build::properties::zoo_cfg, validate::ValidatedCluster},
    };

    #[test]
    fn test_default_config() {
        let cm = build_config_map(&minimal_zk_yaml(3)).data.unwrap();
        let config = cm.get("zoo.cfg").unwrap();
        assert!(config.contains(
            "authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider"
        ));
        assert!(config.contains("ssl.hostnameVerification=true"));
        // Default value
        assert!(config.contains("ssl.quorum.hostnameVerification=true"));

        assert!(cm.contains_key("security.properties"));
    }

    #[test]
    fn test_config_overrides() {
        let zookeeper_yaml = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.5"
          servers:
            configOverrides:
              zoo.cfg:
                foo: bar
                level: role
                hello-from-role: "true"
            roleGroups:
              default:
                configOverrides:
                  zoo.cfg:
                    foo: bar
                    level: role-group
                    ssl.quorum.hostnameVerification: "false"
                    hello-from-role-group: "true"
                replicas: 3
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();
        let config = cm.get("zoo.cfg").unwrap();
        assert!(config.contains("foo=bar"));
        assert!(config.contains("level=role-group"));
        assert!(config.contains("hello-from-role=true"));
        assert!(config.contains("hello-from-role-group=true"));
        assert!(config.contains(
            "authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider"
        ));
        assert!(config.contains("ssl.hostnameVerification=true"));
        // Overwritten by configOverride
        assert!(config.contains("ssl.quorum.hostnameVerification=false"));

        assert!(cm.contains_key("security.properties"));
    }

    #[test]
    fn test_seeded_operator_defaults() {
        // These values are seeded directly by the ConfigMap builder and must stay byte-identical.
        // This test (together with the TLS x client-auth matrix tests below) is the source of truth
        // for the rendered `zoo.cfg` / `security.properties`; the kuttl smoke keeps only the
        // live-cluster readiness/status checks.
        let cm = build_config_map(&minimal_zk_yaml(3)).data.unwrap();

        // `security.properties` is fully operator-injected; assert it byte-for-byte.
        assert_eq!(
            cm.get("security.properties").unwrap(),
            "networkaddress.cache.negative.ttl=0\nnetworkaddress.cache.ttl=5\n"
        );

        let zoo_cfg = cm.get("zoo.cfg").unwrap();
        for expected in [
            "admin.serverPort=8080",
            // new_for_tests() enables server TLS, so the secure client port is used.
            "clientPort=2282",
            "dataDir=/stackable/data",
            "initLimit=5",
            "syncLimit=2",
            "tickTime=3000",
            "metricsProvider.className=org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider",
            "metricsProvider.httpPort=7000",
        ] {
            assert!(
                zoo_cfg.contains(expected),
                "missing {expected:?} in:\n{zoo_cfg}"
            );
        }
    }

    #[test]
    fn test_user_config_overrides_seeded_default() {
        // A value set on the typed config must win over the seeded default.
        let zookeeper_yaml = r#"
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
                replicas: 3
                config:
                  tickTime: 4000
                  initLimit: 7
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();
        let zoo_cfg = cm.get("zoo.cfg").unwrap();
        assert!(zoo_cfg.contains("tickTime=4000"), "{zoo_cfg}");
        assert!(zoo_cfg.contains("initLimit=7"), "{zoo_cfg}");
        // Untouched default stays.
        assert!(zoo_cfg.contains("syncLimit=2"), "{zoo_cfg}");
    }

    #[test]
    fn test_non_tls_uses_insecure_client_port() {
        // With server TLS disabled (`serverSecretClass: null`) the insecure client port is used and
        // none of the server-TLS settings (keystore, port unification) are emitted. This exercises
        // the non-TLS branch of `ZookeeperSecurity::{client_port, config_settings}`.
        let zookeeper_yaml = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.5"
          clusterConfig:
            tls:
              serverSecretClass: null
          servers:
            roleGroups:
              default:
                replicas: 3
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();
        let zoo_cfg = cm.get("zoo.cfg").unwrap();
        assert!(zoo_cfg.contains("clientPort=2181"), "{zoo_cfg}");
        assert!(!zoo_cfg.contains("client.portUnification"), "{zoo_cfg}");
        // The server-TLS keystore line (distinct from the always-present quorum keystore).
        assert!(!zoo_cfg.contains("ssl.keyStore.location"), "{zoo_cfg}");
    }

    #[test]
    fn test_config_override_wins_over_typed_config() {
        // The typed `config` sets `syncLimit`, but a `configOverride` for the same key must win
        // (configOverrides are layered last in `zoo.cfg` precedence).
        let zookeeper_yaml = r#"
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
                replicas: 3
                config:
                  syncLimit: 9
                configOverrides:
                  zoo.cfg:
                    syncLimit: "15"
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();
        let zoo_cfg = cm.get("zoo.cfg").unwrap();
        assert!(zoo_cfg.contains("syncLimit=15"), "{zoo_cfg}");
    }

    #[test]
    fn test_custom_log_config_omits_logback() {
        // Automatic logging renders `logback.xml` into the ConfigMap; a custom log ConfigMap
        // suppresses it (the `Custom` arm of `build_logback_config`).
        let automatic = build_config_map(&minimal_zk_yaml(3)).data.unwrap();
        assert!(automatic.contains_key("logback.xml"));

        let custom_yaml = r#"
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
                replicas: 3
                config:
                  logging:
                    containers:
                      zookeeper:
                        custom:
                          configMap: my-log-config
        "#;
        let custom = build_config_map(custom_yaml).data.unwrap();
        assert!(!custom.contains_key("logback.xml"));
    }

    #[test]
    fn test_vector_agent_adds_vector_config() {
        // Enabling the Vector agent (with the required aggregator discovery ConfigMap) adds
        // `vector.yaml` to the rolegroup ConfigMap.
        let zookeeper_yaml = r#"
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
                replicas: 3
                config:
                  logging:
                    enableVectorAgent: true
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();
        assert!(cm.contains_key("vector.yaml"));
    }

    #[test]
    fn test_vector_config_absent_when_agent_disabled() {
        // Default logging has the Vector agent disabled, so no `vector.yaml` is added to the
        // ConfigMap. Pins the negative branch alongside `test_vector_agent_adds_vector_config`.
        let cm = build_config_map(SERVER_TLS_YAML).data.unwrap();
        assert!(!cm.contains_key("vector.yaml"));
    }

    #[test]
    fn test_logback_default_structure() {
        // Pins the structural parts of the rendered `logback.xml` that this operator controls (log
        // dir + file name, console conversion pattern, max file size, appender wiring). The levels
        // are covered separately by `test_logback_renders_zookeeper_container_log_levels`.
        let cm = build_config_map(SERVER_TLS_YAML).data.unwrap();
        let logback = cm.get("logback.xml").unwrap();

        // Console appender + the operator's conversion pattern.
        assert!(
            logback.contains(
                r#"<appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">"#
            ),
            "{logback}"
        );
        assert!(
            logback.contains(
                "<pattern>%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n</pattern>"
            ),
            "{logback}"
        );

        // Rolling file appender writing to the operator's log dir + file name.
        assert!(
            logback.contains(
                r#"<appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">"#
            ),
            "{logback}"
        );
        assert!(
            logback.contains("<File>/stackable/log/zookeeper/zookeeper.log4j.xml</File>"),
            "{logback}"
        );
        assert!(
            logback.contains(
                "<FileNamePattern>/stackable/log/zookeeper/zookeeper.log4j.xml.%i</FileNamePattern>"
            ),
            "{logback}"
        );
        // 10 MiB total across 2 files -> 5MB per file (derived from MAX_ZK_LOG_FILES_SIZE).
        assert!(
            logback.contains("<MaxFileSize>5MB</MaxFileSize>"),
            "{logback}"
        );

        // Root wires both appenders and defaults to INFO.
        assert!(logback.contains(r#"<root level="INFO">"#), "{logback}");
        assert!(
            logback.contains(r#"<appender-ref ref="CONSOLE" />"#)
                && logback.contains(r#"<appender-ref ref="FILE" />"#),
            "{logback}"
        );
    }

    #[test]
    fn test_logback_renders_zookeeper_container_log_levels() {
        // The zookeeper container's automatic log config drives `logback.xml`: the console and file
        // appender ThresholdFilter levels, per-logger levels, and the root level. (The `prepare` and
        // `vector` container levels do NOT affect `logback.xml` — they flow into those containers'
        // own config via upstream behavior — so only the zookeeper container is set here.)
        let zookeeper_yaml = r#"
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
                        console:
                          level: DEBUG
                        file:
                          level: WARN
                        loggers:
                          ROOT:
                            level: ERROR
                          org.apache.zookeeper:
                            level: TRACE
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();
        let logback = cm.get("logback.xml").unwrap();

        // The console and file appenders share the same ThresholdFilter element, so split on the
        // FILE appender to tie each level to its appender: console -> DEBUG, file -> WARN.
        let (console_part, file_part) = logback
            .split_once(r#"name="FILE""#)
            .unwrap_or_else(|| panic!("FILE appender present: {logback}"));
        assert!(
            console_part.contains("<level>DEBUG</level>"),
            "console appender level should be DEBUG: {logback}"
        );
        assert!(
            !console_part.contains("<level>WARN</level>"),
            "console appender level should not be WARN: {logback}"
        );
        assert!(
            file_part.contains("<level>WARN</level>"),
            "file appender level should be WARN: {logback}"
        );

        // The ROOT logger level maps to the `<root>` element; a named logger renders its own entry.
        assert!(
            logback.contains(r#"<root level="ERROR">"#),
            "root level should be ERROR: {logback}"
        );
        assert!(
            logback.contains(r#"<logger name="org.apache.zookeeper" level="TRACE" />"#),
            "named logger should render at TRACE: {logback}"
        );
    }

    // ---------------------------------------------------------------------------------------------
    // TLS x client-auth matrix.
    //
    // These four tests own the `zoo.cfg` TLS/client-mTLS matrix the kuttl smoke `14-assert` heredoc
    // used to check: the exact property key set plus the ports/`ssl.*` lines that vary with server
    // TLS and client mTLS. They assert on the structured map from the pure `zoo_cfg::build` seam
    // (via `zoo_cfg_map`), so the full key set compares exactly: a missing or extra property fails
    // set equality, with no brittle `!contains` checks. Server TLS is toggled by the two fixtures;
    // client mTLS is layered on via `validated_cluster_with_client_auth`.
    // ---------------------------------------------------------------------------------------------

    /// Server role group with default TLS (server `AuthenticationClass` off, server TLS on).
    const SERVER_TLS_YAML: &str = r#"
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
                replicas: 3
        "#;

    /// As [`SERVER_TLS_YAML`] but with server TLS explicitly disabled (`serverSecretClass: null`).
    const NO_SERVER_TLS_YAML: &str = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.5"
          clusterConfig:
            tls:
              serverSecretClass: null
          servers:
            roleGroups:
              default:
                replicas: 3
        "#;

    /// Builds the structured `zoo.cfg` key/value map for a server role group via the pure
    /// `zoo_cfg::build` seam (before Java-properties serialization). Asserting on this map gives
    /// exact key-set and value comparisons without a hand-rolled parser; the serialization itself is
    /// pinned separately (`test_server_lines_use_myid_offset_across_rolegroups`,
    /// `test_seeded_operator_defaults`).
    fn zoo_cfg_map(validated: &ValidatedCluster, role_group: &str) -> BTreeMap<String, String> {
        let (_, rolegroup_config) = server_rolegroup_config(validated, role_group);
        let server_addresses = zoo_cfg::server_addresses(validated, &cluster_info());
        zoo_cfg::build(validated, rolegroup_config, &server_addresses)
    }

    /// Builds a `BTreeSet<String>` from string slices, for comparing against `zoo.cfg` key sets.
    fn key_set<'a>(keys: impl IntoIterator<Item = &'a str>) -> BTreeSet<String> {
        keys.into_iter().map(str::to_owned).collect()
    }

    /// The non-`server.<myid>` property keys of a built `zoo.cfg` map. The quorum `server.<myid>`
    /// entries are replica-dependent and asserted separately in
    /// `test_server_lines_use_myid_offset_across_rolegroups`.
    fn property_keys(zoo_cfg: &BTreeMap<String, String>) -> BTreeSet<String> {
        zoo_cfg
            .keys()
            .filter(|k| !k.starts_with("server."))
            .cloned()
            .collect()
    }

    /// The `zoo.cfg` keys that are always present regardless of the TLS/client-auth matrix
    /// (operator-injected defaults + quorum TLS, which is always on).
    fn base_zoo_cfg_keys() -> BTreeSet<String> {
        key_set([
            "admin.serverPort",
            "authProvider.x509",
            "clientPort",
            "dataDir",
            "initLimit",
            "metricsProvider.className",
            "metricsProvider.httpPort",
            "serverCnxnFactory",
            "ssl.quorum.clientAuth",
            "ssl.quorum.hostnameVerification",
            "ssl.quorum.keyStore.location",
            "ssl.quorum.trustStore.location",
            "sslQuorum",
            "syncLimit",
            "tickTime",
        ])
    }

    /// TLS off, client-auth off: insecure client port, no server-`ssl.*` lines, no
    /// `client.portUnification`, no `ssl.clientAuth`.
    #[test]
    fn test_matrix_no_tls_no_client_auth() {
        let zoo_cfg = zoo_cfg_map(
            &validated_cluster(&minimal_zk(NO_SERVER_TLS_YAML)),
            "default",
        );

        // The exact base key set proves the absence of every server-TLS / client-auth property.
        assert_eq!(property_keys(&zoo_cfg), base_zoo_cfg_keys());
        assert_eq!(zoo_cfg["clientPort"], "2181");
    }

    /// TLS on (default), client-auth off: secure client port, server-`ssl.*` lines and
    /// `client.portUnification`, but no `ssl.clientAuth` (proven by the key set).
    #[test]
    fn test_matrix_server_tls_no_client_auth() {
        let zoo_cfg = zoo_cfg_map(&validated_cluster(&minimal_zk(SERVER_TLS_YAML)), "default");

        assert_eq!(
            property_keys(&zoo_cfg),
            &base_zoo_cfg_keys()
                | &key_set([
                    "client.portUnification",
                    "ssl.hostnameVerification",
                    "ssl.keyStore.location",
                    "ssl.trustStore.location",
                ]),
        );
        assert_eq!(zoo_cfg["clientPort"], "2282");
        assert_eq!(zoo_cfg["client.portUnification"], "true");
        assert_eq!(
            zoo_cfg["ssl.keyStore.location"],
            "/stackable/server_tls/keystore.p12"
        );
    }

    /// TLS on, client-auth on: as above plus `ssl.clientAuth=need`.
    #[test]
    fn test_matrix_server_tls_and_client_auth() {
        let zoo_cfg = zoo_cfg_map(
            &validated_cluster_with_client_auth(&minimal_zk(SERVER_TLS_YAML)),
            "default",
        );

        assert_eq!(
            property_keys(&zoo_cfg),
            &base_zoo_cfg_keys()
                | &key_set([
                    "client.portUnification",
                    "ssl.clientAuth",
                    "ssl.hostnameVerification",
                    "ssl.keyStore.location",
                    "ssl.trustStore.location",
                ]),
        );
        assert_eq!(zoo_cfg["clientPort"], "2282");
        assert_eq!(zoo_cfg["ssl.clientAuth"], "need");
    }

    /// Client-auth on while server TLS is explicitly disabled: the client `AuthenticationClass`
    /// alone turns TLS on (secure port, `ssl.*` lines, `client.portUnification`) and adds
    /// `ssl.clientAuth=need`. This cross term was previously unreachable in unit tests.
    #[test]
    fn test_matrix_client_auth_forces_tls_without_server_secret_class() {
        let zoo_cfg = zoo_cfg_map(
            &validated_cluster_with_client_auth(&minimal_zk(NO_SERVER_TLS_YAML)),
            "default",
        );

        assert_eq!(
            property_keys(&zoo_cfg),
            &base_zoo_cfg_keys()
                | &key_set([
                    "client.portUnification",
                    "ssl.clientAuth",
                    "ssl.hostnameVerification",
                    "ssl.keyStore.location",
                    "ssl.trustStore.location",
                ]),
        );
        assert_eq!(zoo_cfg["clientPort"], "2282");
        assert_eq!(zoo_cfg["client.portUnification"], "true");
        assert_eq!(zoo_cfg["ssl.clientAuth"], "need");
    }

    /// `server.<myid>` quorum lines are rendered into the per-rolegroup `zoo.cfg` with
    /// `myidOffset` applied and colons escaped, aggregated across *all* server role groups
    /// (primary offset 10, secondary offset 20).
    #[test]
    fn test_server_lines_use_myid_offset_across_rolegroups() {
        let zk = minimal_zk(
            r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: test-zk
        spec:
          image:
            productVersion: "3.9.5"
          clusterConfig:
            tls:
              serverSecretClass: null
          servers:
            roleGroups:
              primary:
                replicas: 2
                config:
                  myidOffset: 10
              secondary:
                replicas: 1
                config:
                  myidOffset: 20
        "#,
        );
        let validated = validated_cluster(&zk);

        // The `server.N` entries are identical in every rolegroup's `zoo.cfg`; inspect the primary's.
        let zoo_cfg = zoo_cfg_map(&validated, "primary");
        let server_keys: BTreeSet<String> = zoo_cfg
            .keys()
            .filter(|k| k.starts_with("server."))
            .cloned()
            .collect();
        // Exactly the offset-shifted ids and nothing else: primary (offset 10) -> 10, 11;
        // secondary (offset 20) -> 20.
        assert_eq!(
            server_keys,
            key_set(["server.10", "server.11", "server.20"])
        );
        // Composed value (pre-serialization, colons unescaped): FQDN + leader/election ports +
        // non-TLS client port 2181.
        assert_eq!(
            zoo_cfg["server.10"],
            "test-zk-server-primary-0.test-zk-server-primary-headless.default.svc.cluster.local:2888:3888;2181"
        );

        // The Java-properties serializer escapes the colons (`\:`); pin that on the rendered file.
        let rendered = config_map_for(&validated, "primary");
        let zoo_cfg_file = rendered.data.as_ref().unwrap().get("zoo.cfg").unwrap();
        assert!(
            zoo_cfg_file.contains(
                "server.10=test-zk-server-primary-0.test-zk-server-primary-headless.default.svc.cluster.local\\:2888\\:3888;2181"
            ),
            "{zoo_cfg_file}"
        );
    }

    fn build_config_map(zookeeper_yaml: &str) -> ConfigMap {
        config_map_for(&validated_cluster(&minimal_zk(zookeeper_yaml)), "default")
    }

    /// Builds the rolegroup `ConfigMap` for the named server role group of a validated cluster.
    fn config_map_for(validated_cluster: &ValidatedCluster, role_group: &str) -> ConfigMap {
        let (role_group_name, rolegroup_config) =
            server_rolegroup_config(validated_cluster, role_group);

        config_map::build_server_rolegroup_config_map(
            validated_cluster,
            &cluster_info(),
            &role_group_name,
            rolegroup_config,
        )
        .unwrap()
    }
}
