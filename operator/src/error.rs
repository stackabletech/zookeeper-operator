use std::backtrace::Backtrace;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Kubernetes reported error: {source}")]
    KubeError {
        #[from]
        source: kube::Error,
        backtrace: Backtrace,
    },

    #[error("Error from Operator framework: {source}")]
    OperatorError {
        #[from]
        source: stackable_operator::error::Error,
        backtrace: Backtrace,
    },

    #[error("Error from serde_json: {source}")]
    SerdeError {
        #[from]
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[error("Object is missing key: {key}")]
    MissingObjectKey { key: &'static str },

    #[error("Pod [{}] missing", pod_name)]
    MissingPod { pod_name: String },

    #[error("ControllerRevision missing")]
    MissingControllerRevision,
}
