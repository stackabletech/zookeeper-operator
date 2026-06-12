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

/// Builds the product log config entries (`logback.xml` / `log4j.properties`) for a role group.
///
/// `logging` is the merged [`Logging`] from the role group's validated config and
/// `framework` selects the product log config format (see [`logging_framework`]).
///
/// The Vector agent config (`vector.yaml`) is rendered separately by [`build_vector_config`]
/// because it still requires a [`RoleGroupRef`] from the upstream v1 logging framework.
///
/// [`logging_framework`]: crate::crd::logging_framework
pub fn build_product_log_config(
    logging: &Logging<v1alpha1::Container>,
    framework: LoggingFramework,
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

    data
}

/// Renders the Vector agent config (`vector.yaml`) for a role group.
///
/// Returns `None` when the Vector agent is disabled for this role group.
///
/// This is the only remaining use of [`RoleGroupRef`]: the upstream v1
/// `product_logging::framework::create_vector_config` still requires it. It is built in the
/// controller (where the raw cluster is available) and the resulting string is threaded into the
/// ConfigMap builder.
pub fn build_vector_config(
    rolegroup: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
    logging: &Logging<v1alpha1::Container>,
) -> Option<String> {
    if !logging.enable_vector_agent {
        return None;
    }

    let vector_log_config = if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&v1alpha1::Container::Vector)
    {
        Some(log_config)
    } else {
        None
    };

    Some(product_logging::framework::create_vector_config(
        rolegroup,
        vector_log_config,
    ))
}
