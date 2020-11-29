use kube::CustomResource;
use serde::{Deserialize, Serialize};
use stackable_operator::CRD;
use std::collections::HashMap;

#[derive(Clone, CustomResource, Debug, Deserialize, Serialize)]
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
                     is_bad:
                       type: boolean
          subresources:
             status: {}
    "#;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ZooKeeperServer {
    pub node_name: String,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ZooKeeperVersion {
    #[serde(rename = "3.4.14")]
    v3_4_14,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct ZooKeeperClusterStatus {
    is_bad: bool,
}
