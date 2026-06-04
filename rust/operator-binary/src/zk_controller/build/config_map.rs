//! Builds the rolegroup `ConfigMap` (`zoo.cfg` + `security.properties`) from the
//! [`ValidatedCluster`], without reaching into the
//! [`v1alpha1::ZookeeperCluster`] except for the owner reference and metadata.
//!
//! The operator-injected defaults seeded here used to live in
//! `deploy/config-spec/properties.yaml` and were injected by the `product-config`
//! crate during validation. They are reproduced here so the rendered config is
//! byte-identical to the previous implementation.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
    },
    config_overrides::KeyValueOverridesProvider,
    k8s_openapi::api::core::v1::ConfigMap,
    role_utils::RoleGroupRef,
};

use crate::{
    crd::{
        JVM_SECURITY_PROPERTIES_FILE, METRICS_PROVIDER_HTTP_PORT, METRICS_PROVIDER_HTTP_PORT_KEY,
        STACKABLE_DATA_DIR, ZOOKEEPER_PROPERTIES_FILE, ZookeeperRole,
        security::ZookeeperSecurity,
        v1alpha1::{self, ZookeeperConfig},
    },
    framework::writer::{PropertiesWriterError, to_java_properties_string},
    product_logging::extend_role_group_config_map,
    utils::build_recommended_labels,
    zk_controller::{
        ZK_CONTROLLER_NAME,
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
        source: crate::product_logging::Error,
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
    let zoo_cfg = build_zoo_cfg(cluster, rolegroup_config);
    let security_properties = build_security_properties(rolegroup_config);

    let zoo_cfg_data = into_optional_values(zoo_cfg);
    let security_properties_data = into_optional_values(security_properties);

    let mut cm_builder = ConfigMapBuilder::new();
    cm_builder
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
        .add_data(
            JVM_SECURITY_PROPERTIES_FILE,
            to_java_properties_string(security_properties_data.iter()).with_context(|_| {
                SerializePropertiesSnafu {
                    file: JVM_SECURITY_PROPERTIES_FILE.to_string(),
                    rolegroup: rolegroup_ref.clone(),
                }
            })?,
        )
        .add_data(
            ZOOKEEPER_PROPERTIES_FILE,
            to_java_properties_string(zoo_cfg_data.iter()).with_context(|_| {
                SerializePropertiesSnafu {
                    file: ZOOKEEPER_PROPERTIES_FILE.to_string(),
                    rolegroup: rolegroup_ref.clone(),
                }
            })?,
        );

    extend_role_group_config_map(
        owner,
        role.clone(),
        rolegroup_ref,
        &mut cm_builder,
        &cluster.image.product_version,
    )
    .context(InvalidLoggingConfigSnafu {
        cm_name: rolegroup_ref.object_name(),
    })?;

    cm_builder.build().context(BuildConfigMapSnafu {
        rolegroup: rolegroup_ref.clone(),
    })
}

/// Builds the `zoo.cfg` contents for a role group.
///
/// Precedence (lowest to highest):
/// 1. `server.<myid>` quorum entries (precomputed in validate)
/// 2. operator-injected defaults (formerly `product-config` `properties.yaml`)
/// 3. TLS / quorum settings from [`ZookeeperSecurity`]
/// 4. user-set merged config (`initLimit` / `syncLimit` / `tickTime`)
/// 5. `configOverrides` for `zoo.cfg`
pub fn build_zoo_cfg(
    cluster: &ValidatedCluster,
    rolegroup_config: &ZookeeperRoleGroupConfig,
) -> BTreeMap<String, String> {
    let security = &cluster.cluster_config.zookeeper_security;
    let config = &rolegroup_config.config;

    let mut zoo_cfg = cluster.cluster_config.server_addresses.clone();

    // Operator-injected defaults (former properties.yaml recommended/default values
    // and the `Configuration::compute_files` output).
    zoo_cfg.insert("admin.serverPort".to_string(), "8080".to_string());
    zoo_cfg.insert(
        ZookeeperSecurity::CLIENT_PORT_NAME.to_string(),
        security.client_port().to_string(),
    );
    zoo_cfg.insert(
        ZookeeperConfig::DATA_DIR.to_string(),
        STACKABLE_DATA_DIR.to_string(),
    );
    zoo_cfg.insert(ZookeeperConfig::INIT_LIMIT.to_string(), "5".to_string());
    zoo_cfg.insert(ZookeeperConfig::SYNC_LIMIT.to_string(), "2".to_string());
    zoo_cfg.insert(ZookeeperConfig::TICK_TIME.to_string(), "3000".to_string());
    zoo_cfg.insert(
        "metricsProvider.className".to_string(),
        "org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider".to_string(),
    );
    zoo_cfg.insert(
        METRICS_PROVIDER_HTTP_PORT_KEY.to_string(),
        METRICS_PROVIDER_HTTP_PORT.to_string(),
    );

    // TLS / quorum settings.
    zoo_cfg.extend(security.config_settings());

    // User-set merged config overrides the seeded defaults above.
    if let Some(init_limit) = config.init_limit {
        zoo_cfg.insert(ZookeeperConfig::INIT_LIMIT.to_string(), init_limit.to_string());
    }
    if let Some(sync_limit) = config.sync_limit {
        zoo_cfg.insert(ZookeeperConfig::SYNC_LIMIT.to_string(), sync_limit.to_string());
    }
    if let Some(tick_time) = config.tick_time {
        zoo_cfg.insert(ZookeeperConfig::TICK_TIME.to_string(), tick_time.to_string());
    }

    // configOverrides go last so they win.
    apply_overrides(
        &mut zoo_cfg,
        rolegroup_config
            .config_overrides
            .get_key_value_overrides(ZOOKEEPER_PROPERTIES_FILE),
    );

    zoo_cfg
}

/// Builds the `security.properties` contents for a role group.
pub fn build_security_properties(
    rolegroup_config: &ZookeeperRoleGroupConfig,
) -> BTreeMap<String, String> {
    let mut security_properties = BTreeMap::new();

    // Operator-injected defaults (former properties.yaml recommended values).
    security_properties.insert("networkaddress.cache.ttl".to_string(), "5".to_string());
    security_properties.insert(
        "networkaddress.cache.negative.ttl".to_string(),
        "0".to_string(),
    );

    apply_overrides(
        &mut security_properties,
        rolegroup_config
            .config_overrides
            .get_key_value_overrides(JVM_SECURITY_PROPERTIES_FILE),
    );

    security_properties
}

/// Resolves the metrics HTTP port for a role group, honoring a
/// `metricsProvider.httpPort` `configOverride` if present.
pub fn metrics_http_port(
    cluster: &ValidatedCluster,
    rolegroup_config: &ZookeeperRoleGroupConfig,
) -> u16 {
    build_zoo_cfg(cluster, rolegroup_config)
        .get(METRICS_PROVIDER_HTTP_PORT_KEY)
        .and_then(|port| port.parse().ok())
        .unwrap_or(METRICS_PROVIDER_HTTP_PORT)
}

/// Applies key-value overrides: `Some(value)` sets the key, `None` removes it.
fn apply_overrides(target: &mut BTreeMap<String, String>, overrides: BTreeMap<String, Option<String>>) {
    for (key, value) in overrides {
        match value {
            Some(value) => {
                target.insert(key, value);
            }
            None => {
                target.remove(&key);
            }
        }
    }
}

fn into_optional_values(map: BTreeMap<String, String>) -> BTreeMap<String, Option<String>> {
    map.into_iter().map(|(k, v)| (k, Some(v))).collect()
}
