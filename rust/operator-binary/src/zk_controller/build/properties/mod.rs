//! Per-file builders for the ZooKeeper config files.
//!
//! Each submodule renders the key/value pairs (or file contents) for one file
//! that ends up in the rolegroup ConfigMap. The shared
//! [`writer`](crate::framework::writer) module serializes property maps to the
//! Java-properties on-wire format.

use std::collections::BTreeMap;

pub mod logging;
pub mod security_properties;
pub mod zoo_cfg;

/// The names of the config files assembled into the rolegroup ConfigMap.
#[derive(Clone, Copy, Debug, strum::Display)]
pub enum ConfigFileName {
    #[strum(serialize = "zoo.cfg")]
    ZooCfg,
    #[strum(serialize = "security.properties")]
    SecurityProperties,
}

/// Applies user-provided key-value overrides to a property map: `Some(value)`
/// sets the key, `None` removes it.
pub(crate) fn apply_overrides(
    target: &mut BTreeMap<String, String>,
    overrides: BTreeMap<String, Option<String>>,
) {
    for (key, value) in overrides {
        match value {
            Some(value) => {
                target.insert(key, value);
            }
            None => {
                target.remove(&key);
            }
        }
    }
}

/// Converts a `key -> value` map into the `key -> Some(value)` shape expected by
/// [`to_java_properties_string`](crate::framework::writer::to_java_properties_string).
pub(crate) fn into_optional_values(
    map: BTreeMap<String, String>,
) -> BTreeMap<String, Option<String>> {
    map.into_iter().map(|(k, v)| (k, Some(v))).collect()
}
