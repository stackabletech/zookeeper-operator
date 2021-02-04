use kube::CustomResource;
use serde::{Deserialize, Serialize};
use stackable_operator::CRD;

// TODO: We need to validate the name of the cluster because it is used in pod and configmap names, it can't bee too long
// This probably also means we shouldn't use the node_names in the pod_name...

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

impl CRD for ZooKeeperCluster {
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ZooKeeperServer {
    pub node_name: String,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ZooKeeperVersion {
    #[serde(rename = "3.4.14")]
    v3_4_14,

    #[serde(rename = "3.4.13")]
    v3_4_13,

    #[serde(rename = "3.5.8")]
    v3_5_8,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct ZooKeeperClusterStatus {}
