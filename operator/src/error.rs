use std::num::ParseIntError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Kubernetes reported error: {source}")]
    KubeError {
        #[from]
        source: kube::Error,
    },

    #[error("Error from Operator framework: {source}")]
    OperatorError {
        #[from]
        source: stackable_operator::error::Error,
    },

    #[error("Error from serde_json: {source}")]
    SerdeError {
        #[from]
        source: serde_json::Error,
    },

    #[error("Pod contains invalid id: {source}")]
    InvalidId {
        #[from]
        source: ParseIntError,
    },

    #[error("Error during reconciliation: {0}")]
    ReconcileError(String),
}
