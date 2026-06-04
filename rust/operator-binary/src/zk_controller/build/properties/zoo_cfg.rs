//! Builder for `zoo.cfg` (the main ZooKeeper properties file).
//!
//! The operator-injected defaults seeded here used to live in
//! `deploy/config-spec/properties.yaml` and were injected by the `product-config`
//! crate during validation. They are reproduced here so the rendered config is
//! byte-identical to the previous implementation.

use std::collections::BTreeMap;

use stackable_operator::config_overrides::KeyValueOverridesProvider;

use crate::{
    crd::{
        METRICS_PROVIDER_HTTP_PORT, METRICS_PROVIDER_HTTP_PORT_KEY, STACKABLE_DATA_DIR,
        ZOOKEEPER_PROPERTIES_FILE, security::ZookeeperSecurity, v1alpha1::ZookeeperConfig,
    },
    zk_controller::validate::{ValidatedCluster, ZookeeperRoleGroupConfig},
};

use super::apply_overrides;

const ADMIN_SERVER_PORT_KEY: &str = "admin.serverPort";
const DEFAULT_ADMIN_SERVER_PORT: &str = "8080";
const METRICS_PROVIDER_CLASS_NAME_KEY: &str = "metricsProvider.className";
const PROMETHEUS_METRICS_PROVIDER: &str =
    "org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider";
const DEFAULT_INIT_LIMIT: &str = "5";
const DEFAULT_SYNC_LIMIT: &str = "2";
const DEFAULT_TICK_TIME: &str = "3000";

/// Builds the `zoo.cfg` key/value pairs for a role group.
///
/// Precedence (lowest to highest):
/// 1. `server.<myid>` quorum entries (precomputed in validate)
/// 2. operator-injected defaults (formerly `product-config` `properties.yaml`)
/// 3. TLS / quorum settings from [`ZookeeperSecurity`]
/// 4. user-set merged config (`initLimit` / `syncLimit` / `tickTime`)
/// 5. `configOverrides` for `zoo.cfg`
pub fn build(
    cluster: &ValidatedCluster,
    rolegroup_config: &ZookeeperRoleGroupConfig,
) -> BTreeMap<String, String> {
    let security = &cluster.cluster_config.zookeeper_security;
    let config = &rolegroup_config.config;

    // 1. server.<myid> quorum entries.
    let mut zoo_cfg = cluster.cluster_config.server_addresses.clone();

    // 2. Operator-injected defaults (former properties.yaml recommended/default
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

    // 3. TLS / quorum settings.
    zoo_cfg.extend(security.config_settings());

    // 4. User-set merged config overrides the seeded defaults above.
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
        zoo_cfg.insert(ZookeeperConfig::TICK_TIME.to_string(), tick_time.to_string());
    }

    // 5. configOverrides go last so they win.
    apply_overrides(
        &mut zoo_cfg,
        rolegroup_config
            .config_overrides
            .get_key_value_overrides(ZOOKEEPER_PROPERTIES_FILE),
    );

    zoo_cfg
}

/// Resolves the metrics HTTP port for a role group, honoring a
/// `metricsProvider.httpPort` `configOverride` if present.
pub fn metrics_http_port(
    cluster: &ValidatedCluster,
    rolegroup_config: &ZookeeperRoleGroupConfig,
) -> u16 {
    build(cluster, rolegroup_config)
        .get(METRICS_PROVIDER_HTTP_PORT_KEY)
        .and_then(|port| port.parse().ok())
        .unwrap_or(METRICS_PROVIDER_HTTP_PORT)
}
