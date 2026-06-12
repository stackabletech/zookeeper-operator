//! Assembles the per-rolegroup `ConfigMap` from the [`ValidatedCluster`],
//! without reaching into the [`v1alpha1::ZookeeperCluster`] except for the owner
//! reference and object metadata.
//!
//! The individual files are rendered by the [`properties`](super::properties)
//! submodules; this module only orchestrates them into the ConfigMap. The Vector agent config is
//! rendered by the controller (it still needs a `RoleGroupRef`) and threaded in as `vector_config`.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE,
    v2::{
        builder::meta::ownerreference_from_resource,
        config_file_writer::{PropertiesWriterError, to_java_properties_string},
        types::operator::RoleGroupName,
    },
};

use crate::{
    crd::logging_framework,
    zk_controller::{
        build::properties::{ConfigFileName, logging, security_properties, zoo_cfg},
        validate::{ValidatedCluster, ZookeeperRoleGroupConfig},
    },
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
/// from `cluster` and the role-group's type-safe [`ResourceNames`]. `vector_config` is the
/// pre-rendered `vector.yaml` (present only when the Vector agent is enabled).
///
/// [`ResourceNames`]: stackable_operator::v2::role_group_utils::ResourceNames
pub fn build_server_rolegroup_config_map(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    rolegroup_config: &ZookeeperRoleGroupConfig,
    vector_config: Option<String>,
) -> Result<ConfigMap> {
    let mut data: BTreeMap<String, String> = BTreeMap::new();

    // zoo.cfg
    data.insert(
        ConfigFileName::ZooCfg.to_string(),
        to_java_properties_string(zoo_cfg::build(cluster, rolegroup_config).iter()).with_context(
            |_| SerializePropertiesSnafu {
                file: ConfigFileName::ZooCfg.to_string(),
                role_group: role_group_name.clone(),
            },
        )?,
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

    // logback.xml / log4j.properties
    data.extend(logging::build_product_log_config(
        &rolegroup_config.config.logging,
        logging_framework(&cluster.image.product_version),
    ));

    // vector.yaml (only present when the Vector agent is enabled)
    if let Some(vector_config) = vector_config {
        data.insert(VECTOR_CONFIG_FILE.to_string(), vector_config);
    }

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(cluster)
                .name(
                    cluster
                        .resource_names(role_group_name)
                        .role_group_config_map()
                        .to_string(),
                )
                .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
                .with_labels(cluster.recommended_labels(role_group_name))
                .build(),
        )
        .data(data)
        .build()
        .with_context(|_| BuildConfigMapSnafu {
            role_group: role_group_name.clone(),
        })
}
