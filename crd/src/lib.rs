use kube::CustomResource;
use serde::{Deserialize, Serialize};
use stackable_operator::CRD;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

// TODO: We need to validate the name of the cluster because it is used in pod and configmap names, it can't bee too long
// This probably also means we shouldn't use the node_names in the pod_name...

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
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

// This only uses the hash of the spec
impl Hash for ZooKeeperCluster {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.spec.hash(state);
    }
}

impl CRD for ZooKeeperCluster {
    const RESOURCE_NAME: &'static str = "zookeeperclusters.zookeeper.stackable.de";
    const CRD_DEFINITION: &'static str = r#"
    apiVersion: apiextensions.k8s.io/v1
    kind: CustomResourceDefinition
    metadata:
      name: zookeeperclusters.zookeeper.stackable.de
    spec:
      group: zookeeper.stackable.de
      names:
        kind: ZooKeeperCluster
        plural: zookeeperclusters
        singular: zookeepercluster
        shortNames:
          - zk
      scope: Namespaced
      versions:
        - name: v1
          served: true
          storage: true
          schema:
            openAPIV3Schema:
              type: object
              properties:
                spec:
                  type: object
                  properties:
                    version:
                      type: string
                      enum: [ 3.4.14 ]
                    servers:
                      type: array
                      items:
                        type: object
                        properties:
                          node_name:
                            type: string
                  required: [ "version", "servers" ]
                status:
                   type: object
                   properties:
                     revision:
                       type: integer
          subresources:
             status: {}
    "#;
}

impl ZooKeeperClusterSpec {
    pub fn image_name(&self) -> String {
        format!(
            "stackable/zookeeper:{}",
            serde_json::json!(self.version).as_str().unwrap()
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ZooKeeperServer {
    pub node_name: String,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ZooKeeperVersion {
    #[serde(rename = "3.4.14")]
    v3_4_14,

    #[serde(rename = "3.4.13")]
    v3_4_13,

    #[serde(rename = "3.5.8")]
    v3_5_8,
}

#[derive(Clone, CustomResource, Debug, Deserialize, Serialize)]
#[kube(
    group = "zookeeper.stackable.de",
    version = "v1",
    kind = "ZooKeeperRestart",
    shortname = "zkrestart",
    namespaced
)]
pub struct ZooKeeperRestartSpec {
    pub zoo_keepr_cluster_ref: String,
}

// ConfigOption a map of version number to:
// * name
// * type
// ** int (min, max)
// ** float (min, max)
// ** string (min & max length, format: regex pattern, date, email, units....)
// ** boolean
// ** sub options?
// ** array
// * units? (e.g. "mb or "port" so the agent can check for conflicting ports before trying to start a process)
// * validator functions
// * optional/required/nullable
// * enum
// * deprecated (new name(s), no replacement)
// * as of version...
// * description/documentation
// * default value -> potentially a map of version number to default value
// * (recommended/default values (a calculator function?, which needs to be able to reference other options))
// * dynamic option/restart required
// * (override hierarchy?)
// * links to relevant documentation/code

pub struct VersionedConfigOption {
    pub version_map: HashMap<String, ConfigOption>,
}

pub struct ConfigOption {
    pub name: String,
}

pub struct ZooKeeperConfig {
    pub auto_purge_purge_interval: Option<i32>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct ZooKeeperClusterStatus {
    revision: i32,
}
