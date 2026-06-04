//! Builder for `security.properties` (ZooKeeper's JVM security properties file).

use std::collections::BTreeMap;

use crate::zk_controller::validate::ZookeeperRoleGroupConfig;

use super::resolved_overrides;

const NETWORKADDRESS_CACHE_TTL: &str = "networkaddress.cache.ttl";
const NETWORKADDRESS_CACHE_NEGATIVE_TTL: &str = "networkaddress.cache.negative.ttl";

const DEFAULT_NETWORKADDRESS_CACHE_TTL: &str = "5";
const DEFAULT_NETWORKADDRESS_CACHE_NEGATIVE_TTL: &str = "0";

/// Builds the `security.properties` key/value pairs for a role group.
///
/// The entire file is operator-injected (the values formerly came from
/// `product-config`'s `properties.yaml`), plus any user `configOverrides`.
pub fn build(rolegroup_config: &ZookeeperRoleGroupConfig) -> BTreeMap<String, String> {
    let mut security_properties = BTreeMap::new();

    // Operator-injected defaults (former properties.yaml recommended values).
    security_properties.insert(
        NETWORKADDRESS_CACHE_TTL.to_string(),
        DEFAULT_NETWORKADDRESS_CACHE_TTL.to_string(),
    );
    security_properties.insert(
        NETWORKADDRESS_CACHE_NEGATIVE_TTL.to_string(),
        DEFAULT_NETWORKADDRESS_CACHE_NEGATIVE_TTL.to_string(),
    );

    // configOverrides go last so they win.
    security_properties.extend(resolved_overrides(
        rolegroup_config.config_overrides.security_properties.clone(),
    ));

    security_properties
}
