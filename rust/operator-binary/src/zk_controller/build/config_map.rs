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
    v2::{
        builder::meta::ownerreference_from_resource,
        config_file_writer::{PropertiesWriterError, to_java_properties_string},
    },
};

use crate::{
    crd::{logging_framework, v1alpha1},
    utils::build_recommended_labels,
    zk_controller::{
        ZK_CONTROLLER_NAME,
        build::properties::{ConfigFileName, logging, security_properties, zoo_cfg},
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

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build ConfigMap for {rolegroup}"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<v1alpha1::ZookeeperCluster>,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Builds the rolegroup [`ConfigMap`] entirely from the [`ValidatedCluster`].
///
/// The owner reference and object metadata (name, namespace, labels) are derived
/// from `cluster`, which mirrors the raw [`v1alpha1::ZookeeperCluster`] metadata.
pub fn build_server_rolegroup_config_map(
    cluster: &ValidatedCluster,
    rolegroup_ref: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
    rolegroup_config: &ZookeeperRoleGroupConfig,
) -> Result<ConfigMap> {
    let mut data: BTreeMap<String, String> = BTreeMap::new();

    // zoo.cfg
    data.insert(
        ConfigFileName::ZooCfg.to_string(),
        to_java_properties_string(zoo_cfg::build(cluster, rolegroup_config).iter()).with_context(
            |_| SerializePropertiesSnafu {
                file: ConfigFileName::ZooCfg.to_string(),
                rolegroup: rolegroup_ref.clone(),
            },
        )?,
    );

    // security.properties
    data.insert(
        ConfigFileName::SecurityProperties.to_string(),
        to_java_properties_string(security_properties::build(rolegroup_config).iter())
            .with_context(|_| SerializePropertiesSnafu {
                file: ConfigFileName::SecurityProperties.to_string(),
                rolegroup: rolegroup_ref.clone(),
            })?,
    );

    // logback.xml / log4j.properties and vector.yaml
    data.extend(logging::build(
        &rolegroup_config.config.logging,
        logging_framework(&cluster.image.product_version),
        rolegroup_ref,
    ));

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(cluster)
                .name(rolegroup_ref.object_name())
                .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
                .with_recommended_labels(&build_recommended_labels(
                    cluster,
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
