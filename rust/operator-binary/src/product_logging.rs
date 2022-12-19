use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::ConfigMapBuilder,
    client::Client,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::ResourceExt,
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice},
    },
    role_utils::RoleGroupRef,
};
use stackable_zookeeper_crd::{
    Container, LoggingFramework, ZookeeperCluster, ZookeeperRole, LOG4J_CONFIG_FILE,
    LOGBACK_CONFIG_FILE, MAX_ZK_LOG_FILES_SIZE_IN_MIB, STACKABLE_LOG_DIR, ZOOKEEPER_LOG_FILE,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("failed to retrieve the ConfigMap {cm_name}"))]
    ConfigMapNotFound {
        source: stackable_operator::error::Error,
        cm_name: String,
    },
    #[snafu(display("failed to retrieve the entry {entry} for ConfigMap {cm_name}"))]
    MissingConfigMapEntry {
        entry: &'static str,
        cm_name: String,
    },
    #[snafu(display("crd validation failure"))]
    CrdValidationFailure {
        source: stackable_zookeeper_crd::Error,
    },
    #[snafu(display("vectorAggregatorConfigMapName must be set"))]
    MissingVectorAggregatorAddress,
}

type Result<T, E = Error> = std::result::Result<T, E>;

const VECTOR_AGGREGATOR_CM_ENTRY: &str = "ADDRESS";
const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n";

/// Return the address of the Vector aggregator if the corresponding ConfigMap name is given in the
/// cluster spec
pub async fn resolve_vector_aggregator_address(
    zk: &ZookeeperCluster,
    client: &Client,
) -> Result<Option<String>> {
    let vector_aggregator_address = if let Some(vector_aggregator_config_map_name) = &zk
        .spec
        .cluster_config
        .logging
        .as_ref()
        .and_then(|logging| logging.vector_aggregator_config_map_name.as_ref())
    {
        let vector_aggregator_address = client
            .get::<ConfigMap>(
                vector_aggregator_config_map_name,
                zk.namespace()
                    .as_deref()
                    .context(ObjectHasNoNamespaceSnafu)?,
            )
            .await
            .context(ConfigMapNotFoundSnafu {
                cm_name: vector_aggregator_config_map_name.to_string(),
            })?
            .data
            .and_then(|mut data| data.remove(VECTOR_AGGREGATOR_CM_ENTRY))
            .context(MissingConfigMapEntrySnafu {
                entry: VECTOR_AGGREGATOR_CM_ENTRY,
                cm_name: vector_aggregator_config_map_name.to_string(),
            })?;
        Some(vector_aggregator_address)
    } else {
        None
    };

    Ok(vector_aggregator_address)
}

/// Extend the role group ConfigMap with logging and Vector configurations
pub fn extend_role_group_config_map(
    zk: &ZookeeperCluster,
    role: ZookeeperRole,
    rolegroup: &RoleGroupRef<ZookeeperCluster>,
    vector_aggregator_address: Option<&str>,
    cm_builder: &mut ConfigMapBuilder,
) -> Result<()> {
    let logging = zk
        .logging(&role, rolegroup)
        .context(CrdValidationFailureSnafu)?;

    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&Container::Zookeeper)
    {
        match zk.logging_framework().context(CrdValidationFailureSnafu)? {
            LoggingFramework::LOG4J => {
                cm_builder.add_data(
                    LOG4J_CONFIG_FILE,
                    product_logging::framework::create_log4j_config(
                        &format!("{STACKABLE_LOG_DIR}/zookeeper"),
                        ZOOKEEPER_LOG_FILE,
                        MAX_ZK_LOG_FILES_SIZE_IN_MIB,
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
                        MAX_ZK_LOG_FILES_SIZE_IN_MIB,
                        CONSOLE_CONVERSION_PATTERN,
                        log_config,
                    ),
                );
            }
        }
    }

    let vector_log_config = if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&Container::Vector)
    {
        Some(log_config)
    } else {
        None
    };

    if logging.enable_vector_agent {
        cm_builder.add_data(
            product_logging::framework::VECTOR_CONFIG_FILE,
            product_logging::framework::create_vector_config(
                rolegroup,
                vector_aggregator_address.context(MissingVectorAggregatorAddressSnafu)?,
                vector_log_config,
            ),
        );
    }

    Ok(())
}
