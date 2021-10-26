use stackable_operator::kube;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Illegal ZooKeeper path [{path}]: {errors:?}")]
    IllegalZookeeperPath { path: String, errors: Vec<String> },

    #[error("Illegal znode [{znode}]: {reason}")]
    IllegalZnode { znode: String, reason: String },

    #[error("No pods are found for ZooKeeper cluster [{namespace}/{name}]. Please check the ZooKeeper custom resource and ZooKeeper Operator for errors.")]
    NoZookeeperPodsAvailableForConnectionInfo { namespace: String, name: String },

    #[error("Pod [{pod_name}] has no container port [{port_name}] set. This is should not happen, please open a ticket for this with the reference: [{reference}].")]
    PodWithoutContainerPort {
        pod_name: String,
        port_name: String,
        reference: String,
    },

    #[error("Pod [{pod_name}] has no hostname assignment, this is most probably a transitive failure and should be retried.")]
    PodWithoutHostname { pod_name: String },

    #[error("Pod [{pod_name}] is missing the following required labels: [{labels:?}]")]
    PodMissingLabels {
        pod_name: String,
        labels: Vec<String>,
    },

    #[error("Got object with no name from Kubernetes, this should not happen, please open a ticket for this with the reference: [{reference}]")]
    ObjectWithoutName { reference: String },

    #[error("Kubernetes reported error: {source}")]
    KubeError {
        #[from]
        source: kube::Error,
    },

    #[error("Operator Framework reported error: {source}")]
    OperatorFrameworkError {
        #[from]
        source: stackable_operator::error::Error,
    },
}

pub type ZookeeperOperatorResult<T> = std::result::Result<T, Error>;
