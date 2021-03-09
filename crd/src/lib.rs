use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube_derive::CustomResource;
use schemars::JsonSchema;
use semver::{SemVerError, Version};
use serde::{Deserialize, Serialize};
use stackable_operator::Crd;

// TODO: We need to validate the name of the cluster because it is used in pod and configmap names, it can't bee too long
// This probably also means we shouldn't use the node_names in the pod_name...
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
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
    pub fn is_valid_upgrade(&self, to: &Self) -> Result<bool, SemVerError> {
        let from_version = Version::parse(&self.to_string())?;
        let to_version = Version::parse(&to.to_string())?;

        Ok(to_version > from_version)
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZooKeeperClusterStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_version: Option<ZooKeeperVersion>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_version: Option<ZooKeeperVersion>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(schema_with = "stackable_operator::conditions::schema")]
    pub conditions: Vec<Condition>,
}

impl ZooKeeperClusterStatus {
    pub fn target_image_name(&self) -> Option<String> {
        match &self.target_version {
            None => None,
            Some(version) => Some(format!(
                "stackable/zookeeper:{}",
                serde_json::json!(version).as_str().unwrap()
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ZooKeeperVersion;
    use std::str::FromStr;

    #[test]
    fn test_version_upgrade() {
        assert!(ZooKeeperVersion::v3_4_14
            .is_valid_upgrade(&ZooKeeperVersion::v3_5_8)
            .unwrap());

        assert!(ZooKeeperVersion::v3_5_8
            .is_valid_upgrade(&ZooKeeperVersion::v3_4_14)
            .unwrap());
    }

    #[test]
    fn test_version_conversion() {
        ZooKeeperVersion::from_str("3.4.14").unwrap();
        ZooKeeperVersion::from_str("3.5.8").unwrap();
        ZooKeeperVersion::from_str("1.2.3").unwrap_err();
    }
}
