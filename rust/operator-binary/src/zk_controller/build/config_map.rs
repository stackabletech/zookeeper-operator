//! Assembles the per-rolegroup `ConfigMap` from the [`ValidatedCluster`],
//! without reaching into the [`v1alpha1::ZookeeperCluster`] except for the owner
//! reference and object metadata.
//!
//! The individual files are rendered by the [`properties`](super::properties)
//! submodules; this module only orchestrates them into the ConfigMap.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    role_utils::RoleGroupRef,
};

use crate::{
    crd::{ZookeeperRole, v1alpha1},
    framework::writer::{PropertiesWriterError, to_java_properties_string},
    utils::build_recommended_labels,
    zk_controller::{
        ZK_CONTROLLER_NAME,
        build::properties::{
            ConfigFileName, into_optional_values, logging, security_properties, zoo_cfg,
        },
        validate::{ValidatedCluster, ZookeeperRoleGroupConfig},
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to serialize [{file}] for {rolegroup}"))]
    SerializeProperties {
        source: PropertiesWriterError,
        file: String,
        rolegroup: RoleGroupRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: logging::Error,
        cm_name: String,
    },

    #[snafu(display("failed to build ConfigMap for {rolegroup}"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<v1alpha1::ZookeeperCluster>,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Builds the rolegroup [`ConfigMap`].
///
/// `owner` is the [`v1alpha1::ZookeeperCluster`] and is used solely for the owner
/// reference and object metadata (name, namespace, labels).
pub fn build_server_rolegroup_config_map(
    cluster: &ValidatedCluster,
    role: &ZookeeperRole,
    rolegroup_ref: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
    rolegroup_config: &ZookeeperRoleGroupConfig,
    owner: &v1alpha1::ZookeeperCluster,
) -> Result<ConfigMap> {
    let mut data: BTreeMap<String, String> = BTreeMap::new();

    // zoo.cfg
    data.insert(
        ConfigFileName::ZooCfg.to_string(),
        render(
            ConfigFileName::ZooCfg,
            zoo_cfg::build(cluster, rolegroup_config),
            rolegroup_ref,
        )?,
    );

    // security.properties
    data.insert(
        ConfigFileName::SecurityProperties.to_string(),
        render(
            ConfigFileName::SecurityProperties,
            security_properties::build(rolegroup_config),
            rolegroup_ref,
        )?,
    );

    // logback.xml / log4j.properties and vector.yaml
    data.extend(
        logging::build(
            owner,
            role.clone(),
            rolegroup_ref,
            &cluster.image.product_version,
        )
        .context(InvalidLoggingConfigSnafu {
            cm_name: rolegroup_ref.object_name(),
        })?,
    );

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(owner)
                .name(rolegroup_ref.object_name())
                .ownerreference_from_resource(owner, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(&build_recommended_labels(
                    owner,
                    ZK_CONTROLLER_NAME,
                    &cluster.image.app_version_label_value,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                ))
                .context(ObjectMetaSnafu)?
                .build(),
        )
        .data(data)
        .build()
        .context(BuildConfigMapSnafu {
            rolegroup: rolegroup_ref.clone(),
        })
}

/// Serializes a property map to its Java-properties on-wire representation.
fn render(
    file: ConfigFileName,
    properties: BTreeMap<String, String>,
    rolegroup_ref: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
) -> Result<String> {
    to_java_properties_string(into_optional_values(properties).iter()).with_context(|_| {
        SerializePropertiesSnafu {
            file: file.to_string(),
            rolegroup: rolegroup_ref.clone(),
        }
    })
}
