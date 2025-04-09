use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    memory::BinaryMultiple,
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice},
    },
    role_utils::RoleGroupRef,
};

use crate::crd::{
    LOG4J_CONFIG_FILE, LOGBACK_CONFIG_FILE, LoggingFramework, MAX_ZK_LOG_FILES_SIZE,
    STACKABLE_LOG_DIR, ZOOKEEPER_LOG_FILE, ZookeeperRole, v1alpha1,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("failed to retrieve the ConfigMap {cm_name}"))]
    ConfigMapNotFound {
        source: stackable_operator::client::Error,
        cm_name: String,
    },

    #[snafu(display("failed to retrieve the entry {entry} for ConfigMap {cm_name}"))]
    MissingConfigMapEntry {
        entry: &'static str,
        cm_name: String,
    },

    #[snafu(display("crd validation failure"))]
    CrdValidationFailure { source: crate::crd::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n";

/// Extend the role group ConfigMap with logging and Vector configurations
pub fn extend_role_group_config_map(
    zk: &v1alpha1::ZookeeperCluster,
    role: ZookeeperRole,
    rolegroup: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
    cm_builder: &mut ConfigMapBuilder,
) -> Result<()> {
    let logging = zk
        .logging(&role, rolegroup)
        .context(CrdValidationFailureSnafu)?;

    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&v1alpha1::Container::Zookeeper)
    {
        match zk.logging_framework() {
            LoggingFramework::LOG4J => {
                cm_builder.add_data(
                    LOG4J_CONFIG_FILE,
                    product_logging::framework::create_log4j_config(
                        &format!("{STACKABLE_LOG_DIR}/zookeeper"),
                        ZOOKEEPER_LOG_FILE,
                        MAX_ZK_LOG_FILES_SIZE
                            .scale_to(BinaryMultiple::Mebi)
                            .floor()
                            .value as u32,
                        CONSOLE_CONVERSION_PATTERN,
                        log_config,
                    ),
                );
            }
            LoggingFramework::LOGBACK => {
                cm_builder.add_data(
                    LOGBACK_CONFIG_FILE,
                    product_logging::framework::create_logback_config(
                        &format!("{STACKABLE_LOG_DIR}/zookeeper"),
                        ZOOKEEPER_LOG_FILE,
                        MAX_ZK_LOG_FILES_SIZE
                            .scale_to(BinaryMultiple::Mebi)
                            .floor()
                            .value as u32,
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
        cm_builder.add_data(
            product_logging::framework::VECTOR_CONFIG_FILE,
            product_logging::framework::create_vector_config(rolegroup, vector_log_config),
        );
    }

    Ok(())
}
