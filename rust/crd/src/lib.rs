pub mod error;
pub mod util;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};
use stackable_operator::identity::PodToNodeMapping;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::Role;
use stackable_operator::status::{Conditions, Status, Versioned};
use stackable_operator::versioning::{ProductVersion, Versioning, VersioningState};
use std::cmp::Ordering;
use std::collections::BTreeMap;

pub const APP_NAME: &str = "zookeeper";
pub const MANAGED_BY: &str = "zookeeper-operator";

pub const CLIENT_PORT: &str = "clientPort";
pub const DATA_DIR: &str = "dataDir";
pub const INIT_LIMIT: &str = "initLimit";
pub const SYNC_LIMIT: &str = "syncLimit";
pub const TICK_TIME: &str = "tickTime";
pub const METRICS_PORT: &str = "metricsPort";
pub const ADMIN_PORT: &str = "admin.serverPort";

pub const CONFIG_MAP_TYPE_DATA: &str = "data";
pub const CONFIG_MAP_TYPE_ID: &str = "id";

// TODO: We need to validate the name of the cluster because it is used in pod and configmap names, it can't bee too long
// This probably also means we shouldn't use the node_names in the pod_name...
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "zookeeper.stackable.tech",
    version = "v1alpha1",
    kind = "ZookeeperCluster",
    plural = "zookeeperclusters",
    shortname = "zk",
    namespaced
)]
#[kube(status = "ZookeeperClusterStatus")]
pub struct ZookeeperClusterSpec {
    pub version: ZookeeperVersion,
    pub servers: Role<ZookeeperConfig>,
}

impl Status<ZookeeperClusterStatus> for ZookeeperCluster {
    fn status(&self) -> &Option<ZookeeperClusterStatus> {
        &self.status
    }

    fn status_mut(&mut self) -> &mut Option<ZookeeperClusterStatus> {
        &mut self.status
    }
}

// TODO: These all should be "Property" Enums that can be either simple or complex where complex allows forcing/ignoring errors and/or warnings
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperConfig {
    pub client_port: Option<u16>, // int in Java
    pub data_dir: Option<String>, // String in Java
    pub init_limit: Option<u32>,  // int in Java
    pub sync_limit: Option<u32>,  // int in Java
    pub tick_time: Option<u32>,   // int in Java
    pub metrics_port: Option<u16>,
    pub admin_port: Option<u16>,
}

impl Configuration for ZookeeperConfig {
    type Configurable = ZookeeperCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();
        if let Some(metrics_port) = self.metrics_port {
            result.insert(METRICS_PORT.to_string(), Some(metrics_port.to_string()));
        }
        Ok(result)
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();
        if let Some(client_port) = &self.client_port {
            result.insert(CLIENT_PORT.to_string(), Some(client_port.to_string()));
        }
        if let Some(data_dir) = &self.data_dir {
            result.insert(DATA_DIR.to_string(), Some(data_dir.clone()));
        }
        if let Some(init_limit) = self.init_limit {
            result.insert(INIT_LIMIT.to_string(), Some(init_limit.to_string()));
        }
        if let Some(sync_limit) = self.sync_limit {
            result.insert(SYNC_LIMIT.to_string(), Some(sync_limit.to_string()));
        }
        if let Some(tick_time) = self.tick_time {
            result.insert(TICK_TIME.to_string(), Some(tick_time.to_string()));
        }
        if let Some(admin_port) = self.admin_port {
            result.insert(ADMIN_PORT.to_string(), Some(admin_port.to_string()));
        }
        Ok(result)
    }
}

#[allow(non_camel_case_types)]
#[derive(
    Clone,
    Debug,
    Deserialize,
    Eq,
    JsonSchema,
    PartialEq,
    Serialize,
    strum_macros::Display,
    strum_macros::EnumString,
)]
pub enum ZookeeperVersion {
    #[serde(rename = "3.4.14")]
    #[strum(serialize = "3.4.14")]
    v3_4_14,

    #[serde(rename = "3.5.8")]
    #[strum(serialize = "3.5.8")]
    v3_5_8,
}

impl ZookeeperVersion {
    pub fn package_name(&self) -> String {
        match self {
            ZookeeperVersion::v3_4_14 => {
                format!("zookeeper-{}", self.to_string())
            }
            ZookeeperVersion::v3_5_8 => {
                format!("apache-zookeeper-{}-bin", self.to_string())
            }
        }
    }
}

impl Versioning for ZookeeperVersion {
    fn versioning_state(&self, other: &Self) -> VersioningState {
        let from_version = match Version::parse(&self.to_string()) {
            Ok(v) => v,
            Err(e) => {
                return VersioningState::Invalid(format!(
                    "Could not parse [{}] to SemVer: {}",
                    self.to_string(),
                    e.to_string()
                ))
            }
        };

        let to_version = match Version::parse(&other.to_string()) {
            Ok(v) => v,
            Err(e) => {
                return VersioningState::Invalid(format!(
                    "Could not parse [{}] to SemVer: {}",
                    other.to_string(),
                    e.to_string()
                ))
            }
        };

        match to_version.cmp(&from_version) {
            Ordering::Greater => VersioningState::ValidUpgrade,
            Ordering::Less => VersioningState::ValidDowngrade,
            Ordering::Equal => VersioningState::NoOp,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperClusterStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<ProductVersion<ZookeeperVersion>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<PodToNodeMapping>,
}

impl Versioned<ZookeeperVersion> for ZookeeperClusterStatus {
    fn version(&self) -> &Option<ProductVersion<ZookeeperVersion>> {
        &self.version
    }
    fn version_mut(&mut self) -> &mut Option<ProductVersion<ZookeeperVersion>> {
        &mut self.version
    }
}

impl Conditions for ZookeeperClusterStatus {
    fn conditions(&self) -> &[Condition] {
        self.conditions.as_slice()
    }
    fn conditions_mut(&mut self) -> &mut Vec<Condition> {
        &mut self.conditions
    }
}

#[cfg(test)]
mod tests {
    use crate::ZookeeperVersion;
    use stackable_operator::versioning::{Versioning, VersioningState};
    use std::str::FromStr;

    #[test]
    fn test_zookeeper_version_versioning() {
        assert_eq!(
            ZookeeperVersion::v3_4_14.versioning_state(&ZookeeperVersion::v3_5_8),
            VersioningState::ValidUpgrade
        );
        assert_eq!(
            ZookeeperVersion::v3_5_8.versioning_state(&ZookeeperVersion::v3_4_14),
            VersioningState::ValidDowngrade
        );
        assert_eq!(
            ZookeeperVersion::v3_4_14.versioning_state(&ZookeeperVersion::v3_4_14),
            VersioningState::NoOp
        );
    }

    #[test]
    fn test_version_conversion() {
        ZookeeperVersion::from_str("3.4.14").unwrap();
        ZookeeperVersion::from_str("3.5.8").unwrap();
        ZookeeperVersion::from_str("1.2.3").unwrap_err();
    }

    #[test]
    fn test_package_name() {
        assert_eq!(
            ZookeeperVersion::v3_4_14.package_name(),
            format!("zookeeper-{}", ZookeeperVersion::v3_4_14.to_string())
        );
        assert_eq!(
            ZookeeperVersion::v3_5_8.package_name(),
            format!(
                "apache-zookeeper-{}-bin",
                ZookeeperVersion::v3_5_8.to_string()
            )
        );
    }
}
