//! Builder for `zoo.cfg` (the main ZooKeeper properties file).

use std::collections::BTreeMap;

use stackable_operator::{utils::cluster_info::KubernetesClusterInfo, v2::types::common::Port};

use crate::{
    crd::{
        METRICS_PROVIDER_HTTP_PORT, METRICS_PROVIDER_HTTP_PORT_KEY, STACKABLE_DATA_DIR,
        ZOOKEEPER_ELECTION_PORT, ZOOKEEPER_LEADER_PORT, ZookeeperPodRef, ZookeeperRole,
        security::ZookeeperSecurity, v1alpha1::ZookeeperConfig,
    },
    zk_controller::validate::{ValidatedCluster, ZookeeperRoleGroupConfig},
};

const ADMIN_SERVER_PORT_KEY: &str = "admin.serverPort";
const DEFAULT_ADMIN_SERVER_PORT: &str = "8080";
const METRICS_PROVIDER_CLASS_NAME_KEY: &str = "metricsProvider.className";
const PROMETHEUS_METRICS_PROVIDER: &str =
    "org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider";
const DEFAULT_INIT_LIMIT: &str = "5";
const DEFAULT_SYNC_LIMIT: &str = "2";
const DEFAULT_TICK_TIME: &str = "3000";

/// Builds the `server.<myid>` quorum entries for `zoo.cfg` from the expected pods.
///
/// The pods are predicted from the validated role-group configs (`replicas` + `myidOffset`)
/// rather than from the live cluster state, to avoid instance churn.
pub(crate) fn server_addresses(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> BTreeMap<String, String> {
    let security = &cluster.cluster_config.zookeeper_security;
    let mut server_addresses = BTreeMap::new();
    for (rg_name, rg_config) in cluster
        .role_group_configs
        .get(&ZookeeperRole::Server)
        .into_iter()
        .flatten()
    {
        let resource_names = cluster.role_group_resource_names(rg_name);
        let headless_service_name = resource_names.headless_service_name();
        let stateful_set_name = resource_names.stateful_set_name().to_string();
        // An unset replica count (HPA-managed) predicts a single-server quorum entry, matching
        // the historical default.
        for i in 0..rg_config.replicas.unwrap_or(1) {
            let pod_ref = ZookeeperPodRef {
                namespace: cluster.namespace.clone(),
                role_group_headless_service_name: headless_service_name.clone(),
                pod_name: format!("{stateful_set_name}-{i}"),
                zookeeper_myid: i + rg_config.config.myid_offset,
            };
            server_addresses.insert(
                format!("server.{id}", id = pod_ref.zookeeper_myid),
                format!(
                    "{internal_fqdn}:{ZOOKEEPER_LEADER_PORT}:{ZOOKEEPER_ELECTION_PORT};{client_port}",
                    internal_fqdn = pod_ref.internal_fqdn(cluster_info),
                    client_port = security.client_port()
                ),
            );
        }
    }
    server_addresses
}

/// Builds the `zoo.cfg` key/value pairs for a role group, excluding the
/// `server.<myid>` quorum entries (which depend on `cluster_info`).
///
/// Precedence (lowest to highest):
/// 1. operator-injected defaults
/// 2. TLS / quorum settings from [`ZookeeperSecurity`]
/// 3. user-set merged config (`initLimit` / `syncLimit` / `tickTime`)
/// 4. `configOverrides` for `zoo.cfg`
fn build_base(
    cluster: &ValidatedCluster,
    rolegroup_config: &ZookeeperRoleGroupConfig,
) -> BTreeMap<String, String> {
    let security = &cluster.cluster_config.zookeeper_security;
    let config = &rolegroup_config.config;

    let mut zoo_cfg = BTreeMap::new();

    // 1. Operator-injected defaults (former properties.yaml recommended/default
    //    values and the `Configuration::compute_files` output).
    zoo_cfg.insert(
        ADMIN_SERVER_PORT_KEY.to_string(),
        DEFAULT_ADMIN_SERVER_PORT.to_string(),
    );
    zoo_cfg.insert(
        ZookeeperSecurity::CLIENT_PORT_NAME.to_string(),
        security.client_port().to_string(),
    );
    zoo_cfg.insert(
        ZookeeperConfig::DATA_DIR.to_string(),
        STACKABLE_DATA_DIR.to_string(),
    );
    zoo_cfg.insert(
        ZookeeperConfig::INIT_LIMIT.to_string(),
        DEFAULT_INIT_LIMIT.to_string(),
    );
    zoo_cfg.insert(
        ZookeeperConfig::SYNC_LIMIT.to_string(),
        DEFAULT_SYNC_LIMIT.to_string(),
    );
    zoo_cfg.insert(
        ZookeeperConfig::TICK_TIME.to_string(),
        DEFAULT_TICK_TIME.to_string(),
    );
    zoo_cfg.insert(
        METRICS_PROVIDER_CLASS_NAME_KEY.to_string(),
        PROMETHEUS_METRICS_PROVIDER.to_string(),
    );
    zoo_cfg.insert(
        METRICS_PROVIDER_HTTP_PORT_KEY.to_string(),
        METRICS_PROVIDER_HTTP_PORT.to_string(),
    );

    // 2. TLS / quorum settings.
    zoo_cfg.extend(security.config_settings());

    // 3. User-set merged config overrides the seeded defaults above.
    if let Some(init_limit) = config.init_limit {
        zoo_cfg.insert(
            ZookeeperConfig::INIT_LIMIT.to_string(),
            init_limit.to_string(),
        );
    }
    if let Some(sync_limit) = config.sync_limit {
        zoo_cfg.insert(
            ZookeeperConfig::SYNC_LIMIT.to_string(),
            sync_limit.to_string(),
        );
    }
    if let Some(tick_time) = config.tick_time {
        zoo_cfg.insert(
            ZookeeperConfig::TICK_TIME.to_string(),
            tick_time.to_string(),
        );
    }

    // 4. configOverrides go last so they win.
    zoo_cfg.extend(rolegroup_config.config_overrides.zoo_cfg.clone());

    zoo_cfg
}

/// Builds the full `zoo.cfg` key/value pairs for a role group.
///
/// The `server.<myid>` quorum entries take lowest precedence; everything from
/// [`build_base`] is layered on top.
pub fn build(
    cluster: &ValidatedCluster,
    rolegroup_config: &ZookeeperRoleGroupConfig,
    server_addresses: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut zoo_cfg = server_addresses.clone();
    zoo_cfg.extend(build_base(cluster, rolegroup_config));
    zoo_cfg
}

impl ValidatedCluster {
    /// Resolves the metrics HTTP port for the given role group, honoring a
    /// `metricsProvider.httpPort` `configOverride` if present.
    pub fn metrics_http_port(&self, rolegroup_config: &ZookeeperRoleGroupConfig) -> Port {
        // Only a `configOverride` can change the metrics port; the seeded default and the
        // TLS/quorum settings never touch `metricsProvider.httpPort`.
        rolegroup_config
            .config_overrides
            .zoo_cfg
            .overrides
            .get(METRICS_PROVIDER_HTTP_PORT_KEY)
            .and_then(|port| port.parse::<u16>().ok())
            .map(Port::from)
            .unwrap_or(METRICS_PROVIDER_HTTP_PORT)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use stackable_operator::v2::types::operator::RoleGroupName;

    use super::*;
    use crate::zk_controller::test_support::{cluster_info, minimal_zk, validated_cluster};

    /// Validates `yaml` into a [`ValidatedCluster`] and returns its `server` `default` role group.
    fn validated_with_default_rg(yaml: &str) -> (ValidatedCluster, ZookeeperRoleGroupConfig) {
        let validated = validated_cluster(&minimal_zk(yaml));
        let rg_name = RoleGroupName::from_str("default").expect("valid role group name");
        let rg = validated.role_group_configs[&ZookeeperRole::Server][&rg_name].clone();
        (validated, rg)
    }

    #[test]
    fn server_addresses_append_secure_client_port_when_tls_enabled() {
        // TLS is enabled by default, so the quorum entries advertise the secure client port.
        let (validated, _) = validated_with_default_rg(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: test-zk
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleGroups:
                  default:
                    replicas: 1
            "#,
        );
        let entry = server_addresses(&validated, &cluster_info())
            .remove("server.1")
            .expect("missing server.1");
        assert!(entry.ends_with(";2282"), "unexpected entry: {entry}");
    }

    #[test]
    fn server_addresses_append_insecure_client_port_when_tls_disabled() {
        // With server TLS disabled the quorum entries advertise the insecure client port.
        let (validated, _) = validated_with_default_rg(
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
                  default:
                    replicas: 1
            "#,
        );
        let entry = server_addresses(&validated, &cluster_info())
            .remove("server.1")
            .expect("missing server.1");
        assert!(entry.ends_with(";2181"), "unexpected entry: {entry}");
    }

    #[test]
    fn server_addresses_unset_replicas_predicts_single_entry() {
        // An unset replica count (HPA-managed) predicts a single-server quorum entry.
        let validated = validated_cluster(&minimal_zk(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: test-zk
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleGroups:
                  default: {}
            "#,
        ));
        let addresses = server_addresses(&validated, &cluster_info());
        assert_eq!(addresses.len(), 1);
        assert!(addresses.contains_key("server.1"), "{addresses:?}");
    }

    #[test]
    fn server_addresses_honor_myid_offset_across_role_groups() {
        // The quorum keys are `server.<i + myidOffset>`, aggregated across all server role groups.
        let validated = validated_cluster(&minimal_zk(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: test-zk
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleGroups:
                  primary:
                    replicas: 2
                  secondary:
                    replicas: 2
                    config:
                      myidOffset: 10
            "#,
        ));
        let addresses = server_addresses(&validated, &cluster_info());
        // primary: offset 1 -> server.1, server.2; secondary: offset 10 -> server.10, server.11.
        for expected in ["server.1", "server.2", "server.10", "server.11"] {
            assert!(
                addresses.contains_key(expected),
                "missing {expected} in {addresses:?}"
            );
        }
        assert_eq!(addresses.len(), 4);
    }

    #[test]
    fn metrics_http_port_defaults_and_honors_override() {
        // Default: the seeded `METRICS_PROVIDER_HTTP_PORT`.
        let (validated, rg) = validated_with_default_rg(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: test-zk
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleGroups:
                  default:
                    replicas: 1
            "#,
        );
        assert_eq!(validated.metrics_http_port(&rg), METRICS_PROVIDER_HTTP_PORT);

        // A numeric `configOverride` wins.
        let (validated, rg) = validated_with_default_rg(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: test-zk
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleGroups:
                  default:
                    replicas: 1
                    configOverrides:
                      zoo.cfg:
                        metricsProvider.httpPort: "9999"
            "#,
        );
        assert_eq!(validated.metrics_http_port(&rg), Port::from(9999));

        // A non-numeric `configOverride` falls back to the default.
        let (validated, rg) = validated_with_default_rg(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: test-zk
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleGroups:
                  default:
                    replicas: 1
                    configOverrides:
                      zoo.cfg:
                        metricsProvider.httpPort: "not-a-port"
            "#,
        );
        assert_eq!(validated.metrics_http_port(&rg), METRICS_PROVIDER_HTTP_PORT);
    }

    #[test]
    fn server_addresses_predicts_one_entry_per_replica() {
        let zk = minimal_zk(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: test-zk
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleGroups:
                  default:
                    replicas: 3
            "#,
        );
        let validated = validated_cluster(&zk);

        let addresses = server_addresses(&validated, &cluster_info());

        // One quorum entry per replica, keyed by `server.<myid>` (myidOffset default 1).
        assert_eq!(addresses.len(), 3);
        for myid in 1..=3 {
            let entry = addresses
                .get(&format!("server.{myid}"))
                .unwrap_or_else(|| panic!("missing server.{myid}"));
            // host:leader:election;client_port — default (non-TLS) client port.
            assert!(entry.contains(":2888:3888;"), "unexpected entry: {entry}");
        }
    }
}
