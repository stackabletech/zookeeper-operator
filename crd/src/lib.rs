mod error;

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::PostParams;
use kube::{Api, Client, CustomResource};
use serde::{Deserialize, Serialize};
use tracing::info;

const ZOOKEEPER_CLUSTER_RESOURCE_NAME: &str = "zookeeperclusters.zookeeper.stackable.de";

// While we can also autogenerate a CRD from the generated class this does not currently include
// a schema (this is an open issue in kube-rs).
const ZOOKEEPER_CLUSTER_CRD: &str = r#"
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

/// Returns true if our CRD has been registered in Kubernetes, false otherwise.
pub async fn crd_exists(client: Client) -> bool {
    let api: Api<CustomResourceDefinition> = Api::all(client);
    return api.get(ZOOKEEPER_CLUSTER_RESOURCE_NAME).await.is_ok(); // TODO: This might also return a transient error (e.g. a timeout)
}

/// This makes sure the ZooKeeperCluster CRD is registered in the apiserver.
/// This will panic if there is an error.
// TODO: Make this generic so we can reuse it
// TODO: Make sure to wait until it's enabled in the apiserver
pub async fn ensure_crd_created(client: Client) {
    if crd_exists(client.clone()).await {
        info!("ZooKeeper CRD already exists in the cluster");
    } else {
        info!("ZooKeeper CRD not detected in the Kubernetes. Attempting to create it.");
        create(client)
            .await
            .expect("Creation of CRD should not fail");
        // TODO: Maybe retry?
    }
}

/// Creates the CRD in the Kubernetes cluster.
/// It will return an error if the CRD already exists.
/// If it returns successfully it does not mean that the CRD is fully established yet,
/// just that it has been accepted by the apiserver.
pub async fn create(client: Client) -> Result<(), error::Error> {
    let api: Api<CustomResourceDefinition> = Api::all(client);
    let zk_crd: CustomResourceDefinition = serde_yaml::from_str(ZOOKEEPER_CLUSTER_CRD)?;
    match api.create(&PostParams::default(), &zk_crd).await {
        Ok(_) => Result::Ok(()),
        Err(err) => Result::Err(error::Error::from(err)),
    }
}
