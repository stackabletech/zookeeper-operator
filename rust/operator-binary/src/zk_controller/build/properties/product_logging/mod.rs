//! Builders for the logging-related files in the rolegroup ConfigMap: the
//! product log config (`logback.xml` / `log4j.properties`) and the Vector agent
//! config (`vector.yaml`).

use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{self, spec::AutomaticContainerLogConfig},
    v2::product_logging::framework::ValidatedContainerLogConfigChoice,
};

use crate::crd::STACKABLE_LOG_DIR;

/// The file the ZooKeeper server logs are written to.
pub const ZOOKEEPER_LOG_FILE: &str = "zookeeper.log4j.xml";

/// Maximum size of the ZooKeeper server log files (before rotation).
pub const MAX_ZK_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n";

/// The vendored Vector agent configuration (`vector.yaml`).
///
/// It is static: per-rolegroup values (namespace, cluster, role, role group, log/data dirs and the
/// aggregator address) are interpolated at runtime by Vector from environment variables that the
/// [`vector_container`](stackable_operator::v2::product_logging::framework::vector_container)
/// injects.
const VECTOR_CONFIG: &str = include_str!("vector.yaml");

/// Returns the Vector agent config (`vector.yaml`) content for the rolegroup `ConfigMap`.
pub fn vector_config_file_content() -> String {
    VECTOR_CONFIG.to_owned()
}

/// Renders the product log config (`logback.xml`) for the ZooKeeper server container.
///
/// Returns `None` when the container uses a custom log ConfigMap instead of the operator's
/// automatic logging configuration, in which case no `logback.xml` is added to the rolegroup
/// `ConfigMap`. Consumes the *validated* log-config choice (off `ValidatedLogging`) rather than
/// re-reading the raw `Logging`.
///
/// The Vector agent config (`vector.yaml`) is added separately via [`vector_config_file_content`].
pub fn build_logback_config(
    zookeeper_container: &ValidatedContainerLogConfigChoice,
) -> Option<String> {
    match zookeeper_container {
        ValidatedContainerLogConfigChoice::Automatic(log_config) => {
            Some(logback_config(log_config))
        }
        ValidatedContainerLogConfigChoice::Custom(_) => None,
    }
}

fn logback_config(log_config: &AutomaticContainerLogConfig) -> String {
    let log_dir = format!("{STACKABLE_LOG_DIR}/zookeeper");
    let max_log_file_size_mib = MAX_ZK_LOG_FILES_SIZE
        .scale_to(BinaryMultiple::Mebi)
        .floor()
        .value as u32;
    product_logging::framework::create_logback_config(
        &log_dir,
        ZOOKEEPER_LOG_FILE,
        max_log_file_size_mib,
        CONSOLE_CONVERSION_PATTERN,
        log_config,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_config_file_content() {
        let content = vector_config_file_content();
        assert!(!content.is_empty());
        // ZooKeeper's logback writes log4j-format XML to `*.log4j.xml`, so the `files_log4j`
        // source matches it.
        assert!(content.contains("files_log4j"));
    }
}
