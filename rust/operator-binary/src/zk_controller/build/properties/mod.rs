//! Per-file builders for the ZooKeeper config files.
//!
//! Each submodule renders the key/value pairs (or file contents) for one file
//! that ends up in the rolegroup ConfigMap.

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
