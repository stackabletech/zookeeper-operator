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
    role_utils::RoleGroupRef,
};

use super::ConfigFileName;
use crate::crd::{LoggingFramework, STACKABLE_LOG_DIR, v1alpha1};

/// The file the ZooKeeper server logs are written to.
pub const ZOOKEEPER_LOG_FILE: &str = "zookeeper.log4j.xml";

/// Maximum size of the ZooKeeper server log files (before rotation).
pub const MAX_ZK_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n";

/// Builds the logging-related ConfigMap entries (product log config and the
/// Vector agent config) for a role group.
///
/// `logging` is the merged [`Logging`] from the role group's validated config and
/// `framework` selects the product log config format (see [`logging_framework`]).
///
/// [`logging_framework`]: crate::crd::logging_framework
pub fn build(
    logging: &Logging<v1alpha1::Container>,
    framework: LoggingFramework,
    rolegroup: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
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

        match framework {
            LoggingFramework::LOG4J => {
                data.insert(
                    ConfigFileName::Log4jProperties.to_string(),
                    product_logging::framework::create_log4j_config(
                        &log_dir,
                        ZOOKEEPER_LOG_FILE,
                        max_log_file_size_mib,
                        CONSOLE_CONVERSION_PATTERN,
                        log_config,
                    ),
                );
            }
            LoggingFramework::LOGBACK => {
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
        }
    }

    let vector_log_config = if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&v1alpha1::Container::Vector)
    {
        Some(log_config)
    } else {
        None
    };

    if logging.enable_vector_agent {
        data.insert(
            product_logging::framework::VECTOR_CONFIG_FILE.to_string(),
            product_logging::framework::create_vector_config(rolegroup, vector_log_config),
        );
    }

    data
}
