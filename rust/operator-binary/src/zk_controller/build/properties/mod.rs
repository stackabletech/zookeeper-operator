//! Per-file builders for the ZooKeeper config files.
//!
//! Each submodule renders the key/value pairs (or file contents) for one file
//! that ends up in the rolegroup ConfigMap.

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
    #[strum(serialize = "log4j.properties")]
    Log4jProperties,
    #[strum(serialize = "logback.xml")]
    LogbackXml,
}
