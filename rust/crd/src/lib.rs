use serde::{Deserialize, Serialize};
use stackable_operator::{
    kube::CustomResource,
    schemars::{self, JsonSchema},
};

/// A cluster of ZooKeeper nodes
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "zookeeper.stackable.tech",
    version = "v1alpha1",
    kind = "ZookeeperCluster",
    plural = "zookeeperclusters",
    shortname = "zk",
    namespaced,
    kube_core = "stackable_operator::kube::core",
    k8s_openapi = "stackable_operator::k8s_openapi",
    schemars = "stackable_operator::schemars"
)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperClusterSpec {
    /// The desired number of nodes in the cluster
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// Emergency stop button, if `true` then all pods are stopped without affecting configuration (as setting `replicas` to `0` would)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<bool>,
}

impl ZookeeperCluster {
    /// The name of the "global" load-balanced Kubernetes `Service`
    pub fn global_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }

    /// The fully-qualified domain name of the "global" load-balanced Kubernetes `Service`
    pub fn global_service_fqdn(&self) -> Option<String> {
        Some(format!(
            "{}.{}.svc.cluster.local",
            self.global_service_name()?,
            self.metadata.namespace.as_ref()?
        ))
    }

    /// Base name for Kubernetes objects used to fulfil the server role
    pub fn server_role_service_name(&self) -> Option<String> {
        Some(format!("{}-servers", self.metadata.name.as_ref()?))
    }

    /// References to all pods forming the cluster
    pub fn pods(&self) -> Option<impl Iterator<Item = ZookeeperPodRef>> {
        let ns = self.metadata.namespace.clone()?;
        let role_svc_name = self.server_role_service_name()?;
        Some(
            (0..self.spec.replicas.unwrap_or(0)).map(move |i| ZookeeperPodRef {
                namespace: ns.clone(),
                role_service_name: role_svc_name.clone(),
                pod_name: format!("{}-{}", role_svc_name, i),
                zookeeper_id: i + 1,
            }),
        )
    }
}

/// Reference to a single `Pod` that is a component of a [`ZookeeperCluster`]
///
/// Used for service discovery.
pub struct ZookeeperPodRef {
    pub namespace: String,
    pub role_service_name: String,
    pub pod_name: String,
    pub zookeeper_id: i32,
}

impl ZookeeperPodRef {
    pub fn fqdn(&self) -> String {
        format!(
            "{}.{}.{}.svc.cluster.local",
            self.pod_name, self.role_service_name, self.namespace
        )
    }
}

/// A claim for a single ZooKeeper ZNode tree (filesystem node)
///
/// A `ConfigMap` will automatically be created with the same name, containing the connection string in the field `ZOOKEEPER_BROKERS`.
/// Each `ZookeeperZnode` gets an isolated ZNode chroot, which the `ZOOKEEPER_BROKERS` automatically contains.
/// All data inside of this chroot will be deleted when the corresponding `ZookeeperZnode` is.
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "zookeeper.stackable.tech",
    version = "v1alpha1",
    kind = "ZookeeperZnode",
    plural = "zookeeperznodes",
    shortname = "zno",
    shortname = "znode",
    namespaced,
    kube_core = "stackable_operator::kube::core",
    k8s_openapi = "stackable_operator::k8s_openapi",
    schemars = "stackable_operator::schemars"
)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperZnodeSpec {
    #[serde(default)]
    pub cluster_ref: ZookeeperClusterRef,
}

/// A reference to a [`ZookeeperCluster`]
#[derive(Clone, Default, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperClusterRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}
