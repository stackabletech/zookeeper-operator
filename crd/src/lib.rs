use kube::CustomResource;
use schemars::JsonSchema;
use semver::{SemVerError, Version};
use serde::{Deserialize, Serialize};
use stackable_operator::Crd;
use strum_macros;

// TODO: We need to validate the name of the cluster because it is used in pod and configmap names, it can't bee too long
// This probably also means we shouldn't use the node_names in the pod_name...

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "zookeeper.stackable.de",
    version = "v1",
    kind = "ZooKeeperCluster",
    shortname = "zk",
    namespaced
)]
#[kube(status = "ZooKeeperClusterStatus")]
pub struct ZooKeeperClusterSpec {
    pub version: ZooKeeperVersion,
    pub servers: Vec<ZooKeeperServer>,
}

impl Crd for ZooKeeperCluster {
    const RESOURCE_NAME: &'static str = "zookeeperclusters.zookeeper.stackable.de";
    const CRD_DEFINITION: &'static str = include_str!("../zookeepercluster.crd.yaml");
}

impl ZooKeeperClusterSpec {
    pub fn image_name(&self) -> String {
        format!(
            "stackable/zookeeper:{}",
            serde_json::json!(self.version).as_str().unwrap()
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct ZooKeeperServer {
    pub node_name: String,
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
pub enum ZooKeeperVersion {
    #[serde(rename = "3.4.14")]
    #[strum(serialize = "3.4.14")]
    v3_4_14,

    #[serde(rename = "3.5.8")]
    #[strum(serialize = "3.5.8")]
    v3_5_8,
}

impl ZooKeeperVersion {
    pub fn is_valid_upgrade(from: Self, to: Self) -> Result<bool, SemVerError> {
        let from_version = Version::parse(&from.to_string())?;
        let to_version = Version::parse(&to.to_string())?;

        Ok(to_version > from_version)
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct ZooKeeperClusterStatus {}

#[cfg(test)]
mod tests {
    use crate::ZooKeeperVersion;
    use std::str::FromStr;

    #[test]
    fn test_version_upgrade() {
        assert!(ZooKeeperVersion::is_valid_upgrade(
            ZooKeeperVersion::v3_4_14,
            ZooKeeperVersion::v3_5_8
        )
        .unwrap());

        assert!(!ZooKeeperVersion::is_valid_upgrade(
            ZooKeeperVersion::v3_5_8,
            ZooKeeperVersion::v3_4_14,
        )
        .unwrap());
    }

    #[test]
    fn test_version_conversion() {
        ZooKeeperVersion::from_str("3.4.14").unwrap();
        ZooKeeperVersion::from_str("3.5.8").unwrap();
        ZooKeeperVersion::from_str("1.2.3").unwrap_err();
    }
}
