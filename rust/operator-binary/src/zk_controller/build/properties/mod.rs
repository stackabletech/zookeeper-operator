//! Per-file builders for the ZooKeeper config files.
//!
//! Each submodule renders the key/value pairs (or file contents) for one file
//! that ends up in the rolegroup ConfigMap. The shared
//! [`writer`](crate::framework::writer) module serializes property maps to the
//! Java-properties on-wire format.

use std::collections::BTreeMap;

use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

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

/// Resolves user-provided [`KeyValueConfigOverrides`] into the `(key, value)`
/// pairs to merge into a config file, dropping entries whose value is unset
/// (`None`).
pub(crate) fn resolved_overrides(
    overrides: KeyValueConfigOverrides,
) -> impl Iterator<Item = (String, String)> {
    overrides
        .overrides
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)))
}

/// Converts a `key -> value` map into the `key -> Some(value)` shape expected by
/// [`to_java_properties_string`](crate::framework::writer::to_java_properties_string).
pub(crate) fn into_optional_values(
    map: BTreeMap<String, String>,
) -> BTreeMap<String, Option<String>> {
    map.into_iter().map(|(k, v)| (k, Some(v))).collect()
}
