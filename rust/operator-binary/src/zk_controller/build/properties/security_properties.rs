//! Builder for `security.properties` (ZooKeeper's JVM security properties file).

use std::collections::BTreeMap;

use stackable_operator::config_overrides::KeyValueOverridesProvider;

use crate::{crd::JVM_SECURITY_PROPERTIES_FILE, zk_controller::validate::ZookeeperRoleGroupConfig};

use super::apply_overrides;

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
    apply_overrides(
        &mut security_properties,
        rolegroup_config
            .config_overrides
            .get_key_value_overrides(JVM_SECURITY_PROPERTIES_FILE),
    );

    security_properties
}
