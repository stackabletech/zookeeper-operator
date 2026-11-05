//! The validate step in the ZookeeperCluster controller.
//!
//! Synchronously validates and merges the cluster spec together with the
//! dereferenced inputs into a [`ValidatedCluster`], which is the single input
//! consumed by the build steps (e.g. the ConfigMap builder). After this step
//! the rest of `reconcile_zk` no longer needs to reach into the
//! [`v1alpha1::ZookeeperCluster`] for configuration (only for the owner
//! reference).

use std::{collections::BTreeMap, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::{
        affinity::StackableAffinity,
        cluster_operation::ClusterOperation,
        pdb::PdbConfig,
        product_image_selection::{self, ResolvedProductImage},
        resources::{NoRuntimeLimits, Resources},
    },
    config::fragment,
    deep_merger::ObjectOverrides,
    k8s_openapi::{api::core::v1::ConfigMap, apimachinery::pkg::apis::meta::v1::ObjectMeta},
    kube::Resource,
    product_logging::spec::Logging,
    shared::time::Duration,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        product_logging::framework::{
            ValidatedContainerLogConfigChoice, VectorContainerLogConfig,
            validate_logging_configuration_for_container,
        },
        role_group_utils::ResourceNames,
        role_utils::{self, JavaCommonConfig, RoleGroup, RoleGroupConfig, with_validated_config},
        types::{
            kubernetes::{ConfigMapName, ListenerClassName, NamespaceName, Uid},
            operator::{ClusterName, ProductVersion, RoleGroupName},
        },
    },
};

use crate::{
    crd::{
        CONTAINER_IMAGE_BASE_NAME, PRODUCT_NAME, ZOOKEEPER_SERVER_PORT_NAME, ZookeeperRole,
        ZookeeperServerRoleType, authentication,
        security::ZookeeperSecurity,
        v1alpha1::{self, ZookeeperConfig, ZookeeperConfigOverrides, ZookeeperServerRoleConfig},
    },
    listener_addresses::{self, ListenerAddresses, listener_addresses},
    zk_controller::dereference::DereferencedObjects,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to validate authentication classes"))]
    InvalidAuthenticationClassConfiguration { source: authentication::Error },

    #[snafu(display("failed to parse role group name {role_group:?}"))]
    ParseRoleGroupName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        role_group: String,
    },

    #[snafu(display("failed to parse the product version {product_version:?}"))]
    ParseProductVersion {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        product_version: String,
    },

    #[snafu(display("invalid config for role group {role_group:?}"))]
    ValidateConfig {
        source: fragment::ValidationError,
        role_group: String,
    },

    #[snafu(display("failed to get the cluster name"))]
    GetClusterName {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to get the namespace"))]
    GetNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to get the UID"))]
    GetUid {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to validate the logging configuration"))]
    ValidateLoggingConfig {
        source: stackable_operator::v2::product_logging::framework::Error,
    },

    #[snafu(display(
        "the Vector agent is enabled but no Vector aggregator discovery ConfigMap name is set"
    ))]
    MissingVectorAggregatorConfigMapName,

    #[snafu(display("failed to read the addresses published by the role Listener"))]
    ReadRoleListenerAddresses { source: listener_addresses::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub type ZookeeperRoleGroupConfig =
    RoleGroupConfig<ValidatedZookeeperConfig, JavaCommonConfig, ZookeeperConfigOverrides>;

#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedZookeeperConfig {
    pub init_limit: Option<u32>,
    pub sync_limit: Option<u32>,
    pub tick_time: Option<u32>,
    pub myid_offset: u16,
    pub resources: Resources<v1alpha1::ZookeeperStorageConfig, NoRuntimeLimits>,
    pub logging: ValidatedLogging,
    pub affinity: StackableAffinity,
    pub graceful_shutdown_timeout: Option<Duration>,
    pub requested_secret_lifetime: Option<Duration>,
}

impl ValidatedZookeeperConfig {
    /// Builds the validated config from the merged [`ZookeeperConfig`], swapping in the
    /// already-validated logging.
    fn from_merged(merged: ZookeeperConfig, logging: ValidatedLogging) -> Self {
        Self {
            init_limit: merged.init_limit,
            sync_limit: merged.sync_limit,
            tick_time: merged.tick_time,
            myid_offset: merged.myid_offset,
            resources: merged.resources,
            logging,
            affinity: merged.affinity,
            graceful_shutdown_timeout: merged.graceful_shutdown_timeout,
            requested_secret_lifetime: merged.requested_secret_lifetime,
        }
    }
}

/// Validated logging configuration for a ZooKeeper server role group.
///
/// Produced up-front by [`validate_logging`] (mirroring the hive- and opensearch-operators) so that
/// an invalid custom log ConfigMap name or a missing Vector aggregator discovery ConfigMap name
/// fails reconciliation during validation rather than at resource-build time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedLogging {
    /// Validated log config choice for the ZooKeeper server container. Used by the build step to
    /// decide whether the log-config volume mounts a custom ConfigMap or the role group's own one.
    /// The product log config file itself (`logback.xml`) is rendered separately.
    pub zookeeper_container: ValidatedContainerLogConfigChoice,
    /// Validated log config choice for the ZooKeeper Prepare init container. Consumed by the
    /// StatefulSet builder to capture the prepare-container shell output when the choice is
    /// `Automatic`.
    pub prepare_container: ValidatedContainerLogConfigChoice,
    /// Validated Vector container log config, present only when the Vector agent is enabled.
    pub vector_container: Option<VectorContainerLogConfig>,
    pub enable_vector_agent: bool,
}

/// Validates the logging configuration for the ZooKeeper server (and optional Vector) container.
///
/// `vector_aggregator_config_map_name` is the discovery ConfigMap name of the Vector aggregator; it
/// is required (and must be present) only when the Vector agent is enabled.
fn validate_logging(
    logging: &Logging<v1alpha1::Container>,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<ValidatedLogging> {
    let zookeeper_container =
        validate_logging_configuration_for_container(logging, &v1alpha1::Container::Zookeeper)
            .context(ValidateLoggingConfigSnafu)?;

    let prepare_container =
        validate_logging_configuration_for_container(logging, &v1alpha1::Container::Prepare)
            .context(ValidateLoggingConfigSnafu)?;

    let vector_container = if logging.enable_vector_agent {
        let vector_aggregator_config_map_name = vector_aggregator_config_map_name
            .clone()
            .context(MissingVectorAggregatorConfigMapNameSnafu)?;
        Some(VectorContainerLogConfig {
            log_config: validate_logging_configuration_for_container(
                logging,
                &v1alpha1::Container::Vector,
            )
            .context(ValidateLoggingConfigSnafu)?,
            vector_aggregator_config_map_name,
        })
    } else {
        None
    };

    Ok(ValidatedLogging {
        zookeeper_container,
        prepare_container,
        vector_container,
        enable_vector_agent: logging.enable_vector_agent,
    })
}

/// The validated [`v1alpha1::ZookeeperCluster`]. Output of the validate step and
/// the single input to the build steps.
pub struct ValidatedCluster {
    /// Mirrors the cluster's [`ObjectMeta`] (name, namespace, UID) so the build
    /// steps can derive owner references and object metadata without reaching back
    /// into the raw [`v1alpha1::ZookeeperCluster`].
    metadata: ObjectMeta,
    pub name: ClusterName,
    pub namespace: NamespaceName,
    pub uid: Uid,
    pub image: ResolvedProductImage,
    /// The product version as a valid label value, used for the recommended
    /// `app.kubernetes.io/version` label. Derived from the resolved image's app version label value.
    pub product_version: ProductVersion,
    pub cluster_config: ValidatedClusterConfig,
    /// Per-role config (currently just the PodDisruptionBudget), extracted during validation so the
    /// apply step does not reach into the raw [`crate::crd::v1alpha1::ZookeeperCluster`].
    pub role_config: ValidatedRoleConfig,
    pub role_group_configs:
        BTreeMap<ZookeeperRole, BTreeMap<RoleGroupName, ZookeeperRoleGroupConfig>>,
    /// The cluster's operation settings (pause/stop), from which the
    /// [`ClusterResourceApplyStrategy`](stackable_operator::cluster_resources::ClusterResourceApplyStrategy)
    /// is derived. Carried here so the apply step does not reach into the cluster spec.
    pub cluster_operation: ClusterOperation,
    /// Object overrides applied to the cluster's resources, carried so the apply step does not reach
    /// into the raw [`v1alpha1::ZookeeperCluster`].
    pub object_overrides: ObjectOverrides,
    /// The client addresses published by the role Listener, which the discovery ConfigMap
    /// advertises.
    ///
    /// `None` while there is no usable address: the Listener does not exist yet, has no status
    /// yet, or publishes no address for the ZooKeeper server port. The build step then re-emits
    /// [`Self::existing_discovery_config_map`] unchanged.
    pub discovery_addresses: Option<ListenerAddresses>,
    /// The discovery ConfigMap as stored by an earlier reconciliation, if any.
    pub existing_discovery_config_map: Option<ConfigMap>,
}

impl ValidatedCluster {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: ClusterName,
        namespace: NamespaceName,
        uid: Uid,
        image: ResolvedProductImage,
        product_version: ProductVersion,
        cluster_config: ValidatedClusterConfig,
        role_config: ValidatedRoleConfig,
        role_group_configs: BTreeMap<
            ZookeeperRole,
            BTreeMap<RoleGroupName, ZookeeperRoleGroupConfig>,
        >,
        cluster_operation: ClusterOperation,
        object_overrides: ObjectOverrides,
        discovery_addresses: Option<ListenerAddresses>,
        existing_discovery_config_map: Option<ConfigMap>,
    ) -> Self {
        Self {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                uid: Some(uid.to_string()),
                ..ObjectMeta::default()
            },
            name,
            namespace,
            uid,
            image,
            product_version,
            cluster_config,
            role_config,
            role_group_configs,
            cluster_operation,
            object_overrides,
            discovery_addresses,
            existing_discovery_config_map,
        }
    }

    /// Type-safe names for the resources of a given role group.
    pub(crate) fn role_group_resource_names(
        &self,
        role_group_name: &RoleGroupName,
    ) -> ResourceNames {
        ResourceNames {
            cluster_name: self.name.clone(),
            role_name: ZookeeperRole::Server.role_name(),
            role_group_name: role_group_name.clone(),
        }
    }

    /// Type-safe names for the per-cluster RBAC resources: the ServiceAccount shared by all
    /// Pods, its (namespaced) RoleBinding, and the operator-deployed ClusterRole it binds.
    pub fn cluster_resource_names(&self) -> role_utils::ResourceNames {
        role_utils::ResourceNames {
            cluster_name: self.name.clone(),
            product_name: PRODUCT_NAME.clone(),
        }
    }
}

impl HasName for ValidatedCluster {
    fn to_name(&self) -> String {
        self.name.to_string()
    }
}

impl HasUid for ValidatedCluster {
    fn to_uid(&self) -> Uid {
        self.uid.clone()
    }
}

impl NameIsValidLabelValue for ValidatedCluster {
    fn to_label_value(&self) -> String {
        self.name.to_label_value()
    }
}

impl Resource for ValidatedCluster {
    type DynamicType = <v1alpha1::ZookeeperCluster as Resource>::DynamicType;
    type Scope = <v1alpha1::ZookeeperCluster as Resource>::Scope;

    fn kind(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::ZookeeperCluster::kind(dt)
    }

    fn group(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::ZookeeperCluster::group(dt)
    }

    fn version(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::ZookeeperCluster::version(dt)
    }

    fn plural(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::ZookeeperCluster::plural(dt)
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

/// Cluster-wide validated configuration that the build steps need without
/// reaching back into the [`v1alpha1::ZookeeperCluster`].
pub struct ValidatedClusterConfig {
    pub zookeeper_security: ZookeeperSecurity,

    /// The ListenerClass used to expose the ZooKeeper servers, so the listener builder does not
    /// need the raw cluster object.
    pub listener_class: ListenerClassName,
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: PdbConfig,
}

/// Validates the cluster spec and the dereferenced inputs.
pub fn validate(
    zk: &v1alpha1::ZookeeperCluster,
    dereferenced_objects: &DereferencedObjects,
    operator_environment: &OperatorEnvironmentOptions,
) -> Result<ValidatedCluster> {
    let image = zk
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            &operator_environment.image_repository,
            &crate::built_info::PKG_VERSION_SEMVER,
        )
        .context(ResolveProductImageSnafu)?;

    let resolved_authentication_classes = dereferenced_objects
        .authentication_classes
        .validate()
        .context(InvalidAuthenticationClassConfigurationSnafu)?;

    let zookeeper_security = ZookeeperSecurity::new(zk, resolved_authentication_classes);

    // The per-role-group logging validation needs it to build the Vector container config.
    let vector_aggregator_config_map_name = zk
        .spec
        .cluster_config
        .vector_aggregator_config_map_name
        .clone();

    let name = get_cluster_name(zk).context(GetClusterNameSnafu)?;
    let namespace = get_namespace(zk).context(GetNamespaceSnafu)?;
    let uid = get_uid(zk).context(GetUidSnafu)?;

    let zk_role = ZookeeperRole::Server;
    let role = zk.role(&zk_role);
    let default_config = ZookeeperConfig::default_server_config(&name, &zk_role);

    let mut groups = BTreeMap::new();
    for (rg_name, rg) in &role.role_groups {
        let role_group_name =
            RoleGroupName::from_str(rg_name).with_context(|_| ParseRoleGroupNameSnafu {
                role_group: rg_name.clone(),
            })?;
        let validated_rg = validate_role_group_config(
            rg_name,
            rg,
            role,
            &default_config,
            &vector_aggregator_config_map_name,
        )?;
        groups.insert(role_group_name, validated_rg);
    }
    let role_group_configs = BTreeMap::from([(zk_role, groups)]);

    let product_version =
        ProductVersion::from_str(&image.app_version_label_value).with_context(|_| {
            ParseProductVersionSnafu {
                product_version: image.app_version_label_value.to_string(),
            }
        })?;

    let listener_class = zk
        .role(&ZookeeperRole::Server)
        .role_config
        .listener_class
        .clone();

    let ZookeeperServerRoleConfig { common, .. } = zk.role_config(&ZookeeperRole::Server);
    let role_config = ValidatedRoleConfig {
        pdb: common.pod_disruption_budget.clone(),
    };

    // No usable address yet means `None`, and the build step then re-emits the stored discovery
    // ConfigMap unchanged (see
    // [`build_discovery_configmap`](crate::discovery::build_discovery_configmap)):
    // - the role Listener does not exist (first reconciliation, or it was deleted),
    // - it has no status yet, or an empty address list (`Ok(None)`),
    // - it publishes addresses, but none for the server port, e.g. a stale status whose port was
    //   renamed (`PortNotFound`). Failing here would abort the run before the apply step, so the
    //   Listener spec carrying the right port would never be applied. This is not expected in
    //   normal operation, so it is logged as a warning.
    // Any other error (an invalid port number) still fails the run.
    let discovery_addresses = match &dereferenced_objects.maybe_role_listener {
        None => None,
        Some(listener) => match listener_addresses(listener, ZOOKEEPER_SERVER_PORT_NAME) {
            Ok(addresses) => addresses,
            Err(error @ listener_addresses::Error::PortNotFound { .. }) => {
                tracing::warn!(
                    error = %error,
                    "the role Listener publishes no address for the ZooKeeper server port, so the \
                     discovery ConfigMap is not updated until it does"
                );
                None
            }
            Err(source) => return Err(source).context(ReadRoleListenerAddressesSnafu),
        },
    };

    Ok(ValidatedCluster::new(
        name,
        namespace,
        uid,
        image,
        product_version,
        ValidatedClusterConfig {
            zookeeper_security,
            listener_class,
        },
        role_config,
        role_group_configs,
        zk.spec.cluster_operation.clone(),
        zk.spec.object_overrides.clone(),
        discovery_addresses,
        dereferenced_objects.existing_discovery_config_map.clone(),
    ))
}

/// Merges and validates one role group into a [`ZookeeperRoleGroupConfig`].
fn validate_role_group_config(
    role_group_name: &str,
    role_group: &RoleGroup<
        v1alpha1::ZookeeperConfigFragment,
        JavaCommonConfig,
        ZookeeperConfigOverrides,
    >,
    role: &ZookeeperServerRoleType,
    default_config: &v1alpha1::ZookeeperConfigFragment,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<ZookeeperRoleGroupConfig> {
    let merged = with_validated_config::<
        ZookeeperConfig,
        JavaCommonConfig,
        v1alpha1::ZookeeperConfigFragment,
        ZookeeperServerRoleConfig,
        ZookeeperConfigOverrides,
    >(role_group, role, default_config)
    .with_context(|_| ValidateConfigSnafu {
        role_group: role_group_name.to_owned(),
    })?;

    let config = merged.config.config;
    let logging = validate_logging(&config.logging, vector_aggregator_config_map_name)?;

    Ok(ZookeeperRoleGroupConfig {
        replicas: merged.replicas,
        config: ValidatedZookeeperConfig::from_merged(config, logging),
        config_overrides: merged.config.config_overrides,
        // The env override names are validated on deserialization.
        env_overrides: merged.config.env_overrides.into(),
        cli_overrides: merged.config.cli_overrides,
        pod_overrides: merged.config.pod_overrides,
        product_specific_common_config: merged.config.product_specific_common_config,
    })
}

#[cfg(test)]
mod tests {
    use stackable_operator::k8s_openapi::apimachinery::pkg::api::resource::Quantity;

    use super::*;
    use crate::{
        listener_addresses::test_support::{ingress_address, role_listener},
        zk_controller::test_support::{
            app_version_label, minimal_zk, try_validate, try_validate_with_role_listener,
            validated_cluster,
        },
    };

    const MINIMAL_ZK_YAML: &str = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.6"
          servers:
            roleGroups:
              default:
                replicas: 3
        "#;

    /// Locks every value the validate step itself derives from the minimal fixture — so a
    /// validation regression fails here, with a validate-shaped message, instead of surfacing as
    /// a confusing build-test failure downstream.
    ///
    /// The merged per-role-group config (resources, affinity, logging defaults, …) is produced by
    /// the config merge machinery, whose contracts are tested in operator-rs; only the values
    /// this module derives on top are re-asserted here.
    #[test]
    fn validate_ok_derives_expected_values() {
        let zookeeper = minimal_zk(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: simple-zookeeper
            spec:
              image:
                productVersion: "3.9.6"
              servers:
                roleGroups:
                  default:
                    replicas: 3
            "#,
        );
        let validated = validated_cluster(&zookeeper);

        assert_eq!(validated.name.to_string(), "simple-zookeeper");
        assert_eq!(validated.namespace.to_string(), "default");
        assert_eq!(
            validated.uid.to_string(),
            "c27b3971-ca72-42c1-80a4-abdfc1db0ddd"
        );
        assert_eq!(
            validated.image.image,
            format!("oci.example.org/zookeeper:{}", app_version_label("3.9.6"))
        );
        assert_eq!(validated.image.product_version, "3.9.6");
        assert_eq!(
            validated.product_version.to_string(),
            app_version_label("3.9.6")
        );

        // TLS towards clients is enabled by default (the secure client port), and the listener
        // class falls back to its default.
        let cluster_config = &validated.cluster_config;
        assert!(cluster_config.zookeeper_security.tls_enabled());
        assert_eq!(
            cluster_config.listener_class.to_string(),
            "cluster-internal"
        );

        // The role config falls back to its defaults: PDBs enabled.
        assert!(validated.role_config.pdb.enabled);
        assert_eq!(validated.role_config.pdb.max_unavailable, None);

        // The single `server` role with the single `default` role group; the Vector agent is off.
        assert_eq!(validated.role_group_configs.len(), 1);
        let role_groups = &validated.role_group_configs[&ZookeeperRole::Server];
        let role_group_names: Vec<String> = role_groups.keys().map(ToString::to_string).collect();
        assert_eq!(role_group_names, ["default"]);
        let role_group = role_groups
            .values()
            .next()
            .expect("the default role group exists");
        assert_eq!(role_group.replicas, Some(3));
        assert!(!role_group.config.logging.enable_vector_agent);
        assert_eq!(role_group.config.logging.vector_container, None);
    }

    #[test]
    fn enabling_vector_without_aggregator_name_fails_validation() {
        // The Vector agent requires the aggregator discovery ConfigMap name; omitting it must fail
        // during validation rather than at build time.
        let zk = minimal_zk(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: test-zk
            spec:
              image:
                productVersion: "3.9.6"
              servers:
                roleGroups:
                  default:
                    replicas: 1
                    config:
                      logging:
                        enableVectorAgent: true
            "#,
        );
        assert!(matches!(
            try_validate(&zk),
            Err(Error::MissingVectorAggregatorConfigMapName)
        ));
    }

    #[test]
    fn enabling_vector_with_aggregator_name_validates_vector_container() {
        let zk = minimal_zk(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: test-zk
            spec:
              image:
                productVersion: "3.9.6"
              clusterConfig:
                vectorAggregatorConfigMapName: vector-aggregator-discovery
              servers:
                roleGroups:
                  default:
                    replicas: 1
                    config:
                      logging:
                        enableVectorAgent: true
            "#,
        );
        let validated = validated_cluster(&zk);
        let rg = server_role_group(&validated, "default");
        assert!(rg.config.logging.enable_vector_agent);
        assert!(rg.config.logging.vector_container.is_some());
    }

    /// Looks up the validated, merged config of a single server role group by name.
    fn server_role_group(
        validated: &ValidatedCluster,
        role_group: &str,
    ) -> ZookeeperRoleGroupConfig {
        let role_group_name = RoleGroupName::from_str(role_group).expect("valid role group name");
        validated
            .role_group_configs
            .get(&ZookeeperRole::Server)
            .and_then(|groups| groups.get(&role_group_name))
            .unwrap_or_else(|| panic!("server role group {role_group:?} should exist"))
            .clone()
    }

    /// Mirrors the `resources` integration test (which can no longer use >16 character role-group
    /// names): a role group without its own `resources` inherits the role-level resources, while a
    /// role group that sets `resources` overrides the role-level values.
    #[test]
    fn role_group_resources_override_role_level_resources() {
        let zk = minimal_zk(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: test-zk
            spec:
              image:
                productVersion: "3.9.6"
              servers:
                config:
                  resources:
                    cpu:
                      min: 400m
                      max: "4"
                    memory:
                      limit: 4Gi
                roleGroups:
                  from-role:
                    replicas: 1
                  from-role-group:
                    replicas: 1
                    config:
                      resources:
                        cpu:
                          min: 300m
                          max: "3"
                        memory:
                          limit: 3Gi
            "#,
        );
        let validated = validated_cluster(&zk);

        // `from-role` has no own `resources` and inherits the role-level values.
        let from_role = server_role_group(&validated, "from-role");
        assert_eq!(
            from_role.config.resources.cpu.min,
            Some(Quantity("400m".to_owned()))
        );
        assert_eq!(
            from_role.config.resources.cpu.max,
            Some(Quantity("4".to_owned()))
        );
        assert_eq!(
            from_role.config.resources.memory.limit,
            Some(Quantity("4Gi".to_owned()))
        );

        // `from-role-group` overrides the role-level values with its own `resources`.
        let from_role_group = server_role_group(&validated, "from-role-group");
        assert_eq!(
            from_role_group.config.resources.cpu.min,
            Some(Quantity("300m".to_owned()))
        );
        assert_eq!(
            from_role_group.config.resources.cpu.max,
            Some(Quantity("3".to_owned()))
        );
        assert_eq!(
            from_role_group.config.resources.memory.limit,
            Some(Quantity("3Gi".to_owned()))
        );
    }

    /// A Listener whose status does not (yet) carry the server port, e.g. a stale status after a
    /// port rename, is treated like a Listener without addresses: the stored discovery ConfigMap
    /// is re-emitted rather than failing the run before the apply step.
    #[test]
    fn role_listener_without_the_expected_port_validates_to_no_addresses() {
        let zk = minimal_zk(MINIMAL_ZK_YAML);
        let listener = role_listener(Some(vec![ingress_address(
            "node-0",
            "not-the-zk-port",
            2181,
        )]));

        let validated = try_validate_with_role_listener(&zk, Some(listener), None)
            .expect("validate should succeed for the test fixture");

        assert_eq!(validated.discovery_addresses, None);
    }

    /// The published addresses reach the discovery ConfigMap through `discovery_addresses`.
    #[test]
    fn role_listener_addresses_are_carried_into_the_validated_cluster() {
        let zk = minimal_zk(MINIMAL_ZK_YAML);
        let listener = role_listener(Some(vec![
            ingress_address("node-1", ZOOKEEPER_SERVER_PORT_NAME, 2181),
            ingress_address("node-0", ZOOKEEPER_SERVER_PORT_NAME, 2181),
        ]));

        let validated = try_validate_with_role_listener(&zk, Some(listener), None)
            .expect("validate should succeed for the test fixture");

        assert_eq!(
            validated
                .discovery_addresses
                .expect("valid addresses")
                .to_connection_string(),
            "node-0:2181,node-1:2181"
        );
    }

    /// Unlike the znode controller, missing addresses are not an error here and will
    /// result in the existing ConfigMap being re-emitted: the reconciliation that
    /// creates the role Listener in the first place has to get as far as the apply
    /// step.
    #[test]
    fn missing_role_listener_addresses_validate_to_no_addresses() {
        let zk = minimal_zk(MINIMAL_ZK_YAML);

        // The Listener does not exist yet.
        let validated = validated_cluster(&zk);
        assert_eq!(validated.discovery_addresses, None);

        // The Listener exists, but the listener operator has not published its status yet.
        let validated = try_validate_with_role_listener(&zk, Some(role_listener(None)), None)
            .expect("validate should succeed for the test fixture");
        assert_eq!(validated.discovery_addresses, None);
    }

    /// When no listener addresses are available the existing ConfigMap will be emitted.
    #[test]
    fn existing_discovery_config_map_is_carried_into_the_validated_cluster() {
        let zk = minimal_zk(MINIMAL_ZK_YAML);

        let stored = ConfigMap {
            data: Some([("ZOOKEEPER".to_owned(), "node-0:2181".to_owned())].into()),
            ..ConfigMap::default()
        };

        let validated = try_validate_with_role_listener(&zk, None, Some(stored.clone()))
            .expect("validate should succeed for the test fixture");

        assert_eq!(validated.existing_discovery_config_map, Some(stored));
    }

    /// A Listener status with an empty address list is treated like no addresses.
    #[test]
    fn role_listener_with_empty_ingress_list_validates_to_no_addresses() {
        let zk = minimal_zk(MINIMAL_ZK_YAML);

        let validated =
            try_validate_with_role_listener(&zk, Some(role_listener(Some(vec![]))), None)
                .expect("validate should succeed for the test fixture");

        assert_eq!(validated.discovery_addresses, None);
    }

    /// A port number that does not fit into a u16 is a fault, unlike a missing port, and still
    /// fails validation.
    #[test]
    fn role_listener_with_an_invalid_port_fails_validation() {
        let zk = minimal_zk(MINIMAL_ZK_YAML);
        let listener = role_listener(Some(vec![ingress_address(
            "node-0",
            ZOOKEEPER_SERVER_PORT_NAME,
            70_000,
        )]));

        assert!(matches!(
            try_validate_with_role_listener(&zk, Some(listener), None),
            Err(Error::ReadRoleListenerAddresses { .. })
        ));
    }
}
