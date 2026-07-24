//! Assembles the per-rolegroup `ConfigMap` from the [`ValidatedCluster`],
//! without reaching into the [`crate::crd::v1alpha1::ZookeeperCluster`] except for the owner
//! reference and object metadata.
//!
//! The individual files are rendered by the [`properties`](crate::zk_controller::build::properties)
//! submodules; this module only orchestrates them into the ConfigMap.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        config_file_writer::{PropertiesWriterError, to_java_properties_string},
        types::operator::RoleGroupName,
    },
};

use crate::zk_controller::{
    build::properties::{ConfigFileName, product_logging, security_properties, zoo_cfg},
    validate::{ValidatedCluster, ZookeeperRoleGroupConfig},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to serialize [{file}] for role group {role_group}"))]
    SerializeProperties {
        source: PropertiesWriterError,
        file: String,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build ConfigMap for role group {role_group}"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
        role_group: RoleGroupName,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Builds the rolegroup [`ConfigMap`] entirely from the [`ValidatedCluster`].
///
/// The owner reference, object metadata (name, namespace, labels) and resource name are derived
/// from `cluster` and the role-group's type-safe [`ResourceNames`]. The vendored Vector agent
/// config (`vector.yaml`) is added when the Vector agent is enabled for the role group.
///
/// [`ResourceNames`]: stackable_operator::v2::role_group_utils::ResourceNames
pub fn build_server_rolegroup_config_map(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    role_group_name: &RoleGroupName,
    rolegroup_config: &ZookeeperRoleGroupConfig,
) -> Result<ConfigMap> {
    let mut data: BTreeMap<String, String> = BTreeMap::new();

    // zoo.cfg
    let server_addresses = zoo_cfg::server_addresses(cluster, cluster_info);
    data.insert(
        ConfigFileName::ZooCfg.to_string(),
        to_java_properties_string(
            zoo_cfg::build(cluster, rolegroup_config, &server_addresses).iter(),
        )
        .with_context(|_| SerializePropertiesSnafu {
            file: ConfigFileName::ZooCfg.to_string(),
            role_group: role_group_name.clone(),
        })?,
    );

    // security.properties
    data.insert(
        ConfigFileName::SecurityProperties.to_string(),
        to_java_properties_string(security_properties::build(rolegroup_config).iter())
            .with_context(|_| SerializePropertiesSnafu {
                file: ConfigFileName::SecurityProperties.to_string(),
                role_group: role_group_name.clone(),
            })?,
    );

    // logback.xml
    if let Some(logback) =
        product_logging::build_logback_config(&rolegroup_config.config.logging.zookeeper_container)
    {
        data.insert(ConfigFileName::LogbackXml.to_string(), logback);
    }

    // vector.yaml (only present when the Vector agent is enabled)
    if rolegroup_config.config.logging.enable_vector_agent {
        data.insert(
            VECTOR_CONFIG_FILE.to_string(),
            product_logging::vector_config_file_content(),
        );
    }

    ConfigMapBuilder::new()
        .metadata(
            cluster
                .object_meta(
                    cluster
                        .role_group_resource_names(role_group_name)
                        .role_group_config_map()
                        .to_string(),
                    role_group_name,
                )
                .build(),
        )
        .data(data)
        .build()
        .with_context(|_| BuildConfigMapSnafu {
            role_group: role_group_name.clone(),
        })
}
