//! Builders for the logging-related files in the rolegroup ConfigMap: the
//! product log config (`logback.xml` / `log4j.properties`) and the Vector agent
//! config (`vector.yaml`).

use std::collections::BTreeMap;

use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice, Logging},
    },
};

use super::ConfigFileName;
use crate::crd::{STACKABLE_LOG_DIR, v1alpha1};

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

/// Builds the product log config entry (`logback.xml`) for a role group.
///
/// `logging` is the merged [`Logging`] from the role group's validated config.
///
/// The Vector agent config (`vector.yaml`) is added separately via
/// [`vector_config_file_content`].
pub fn build_product_log_config(
    logging: &Logging<v1alpha1::Container>,
) -> BTreeMap<String, String> {
    let mut data = BTreeMap::new();

    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&v1alpha1::Container::Zookeeper)
    {
        let log_dir = format!("{STACKABLE_LOG_DIR}/zookeeper");
        let max_log_file_size_mib = MAX_ZK_LOG_FILES_SIZE
            .scale_to(BinaryMultiple::Mebi)
            .floor()
            .value as u32;

        data.insert(
            ConfigFileName::LogbackXml.to_string(),
            product_logging::framework::create_logback_config(
                &log_dir,
                ZOOKEEPER_LOG_FILE,
                max_log_file_size_mib,
                CONSOLE_CONVERSION_PATTERN,
                log_config,
                None,
            ),
        );
    }

    data
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
