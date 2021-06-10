pub mod error;
pub mod util;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use product_config::types::PropertyNameKind;
use schemars::JsonSchema;
use semver::{SemVerError, Version};
use serde::{Deserialize, Serialize};
use stackable_operator::config::{ConfigError, Configuration};
use stackable_operator::role_utils::Role;
use stackable_operator::Crd;
use std::collections::HashMap;

pub const APP_NAME: &str = "zookeeper";
pub const MANAGED_BY: &str = "stackable-zookeeper";

// TODO: We need to validate the name of the cluster because it is used in pod and configmap names, it can't bee too long
// This probably also means we shouldn't use the node_names in the pod_name...
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "zookeeper.stackable.tech",
    version = "v1",
    kind = "ZookeeperCluster",
    shortname = "zk",
    namespaced
)]
#[kube(status = "ZookeeperClusterStatus")]
pub struct ZookeeperClusterSpec {
    pub version: ZookeeperVersion,
    pub servers: Role<ZookeeperConfig>,
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
}

impl Configuration for ZookeeperConfig {
    type Configurable = ZookeeperCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<HashMap<String, String>, ConfigError> {
        Ok(HashMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<HashMap<String, String>, ConfigError> {
        Ok(HashMap::new())
    }

    fn compute_properties(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<HashMap<String, String>, ConfigError> {
        Ok(product_config::ser::to_hash_map(self).unwrap())
    }

    fn config_information() -> HashMap<String, (PropertyNameKind, String)> {
        todo!()
    }
}

impl Crd for ZookeeperCluster {
    const RESOURCE_NAME: &'static str = "zookeeperclusters.zookeeper.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../../deploy/crd/zookeepercluster.crd.yaml");
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
    pub fn is_valid_upgrade(&self, to: &Self) -> Result<bool, SemVerError> {
        let from_version = Version::parse(&self.to_string())?;
        let to_version = Version::parse(&to.to_string())?;

        Ok(to_version > from_version)
    }

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

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperClusterStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_version: Option<ZookeeperVersion>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_version: Option<ZookeeperVersion>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(schema_with = "stackable_operator::conditions::schema")]
    pub conditions: Vec<Condition>,
}

impl ZookeeperClusterStatus {
    pub fn target_image_name(&self) -> Option<String> {
        self.target_version
            .as_ref()
            .map(|version| format!("stackable/zookeeper:{}", version.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use crate::ZookeeperVersion;
    use std::str::FromStr;

    #[test]
    fn test_version_upgrade() {
        assert!(ZookeeperVersion::v3_4_14
            .is_valid_upgrade(&ZookeeperVersion::v3_5_8)
            .unwrap());

        assert!(!ZookeeperVersion::v3_5_8
            .is_valid_upgrade(&ZookeeperVersion::v3_4_14)
            .unwrap());
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
