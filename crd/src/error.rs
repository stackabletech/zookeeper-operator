#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to serialize YAML: {source}")]
    SerializationError {
        #[from]
        source: serde_yaml::Error,
    },

    #[error("Kubernetes reported error: {source}")]
    KubeError {
        #[from]
        source: kube::Error,
    },
}
