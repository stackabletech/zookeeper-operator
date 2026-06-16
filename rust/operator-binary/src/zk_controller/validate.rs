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
    builder::meta::ObjectMetaBuilder,
    cli::OperatorEnvironmentOptions,
    commons::{
        cluster_operation::ClusterOperation,
        pdb::PdbConfig,
        product_image_selection::{self, ResolvedProductImage},
    },
    config::fragment,
    deep_merger::ObjectOverrides,
    k8s_openapi::{api::core::v1::PodTemplateSpec, apimachinery::pkg::apis::meta::v1::ObjectMeta},
    kube::{Resource, ResourceExt},
    kvp::Labels,
    product_logging::spec::Logging,
    role_utils::RoleGroup,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::{
            meta::ownerreference_from_resource,
            pod::container::{self, EnvVarName, EnvVarSet},
        },
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        jvm_argument_overrides::JvmArgumentOverrides,
        kvp::label::{recommended_labels, role_group_selector},
        product_logging::framework::{
            ValidatedContainerLogConfigChoice, VectorContainerLogConfig,
            validate_logging_configuration_for_container,
        },
        role_group_utils::ResourceNames,
        role_utils::{JavaCommonConfig, ResourceNames as RbacResourceNames, with_validated_config},
        types::{
            kubernetes::{
                ConfigMapName, ListenerClassName, NamespaceName, ServiceAccountName, Uid,
            },
            operator::{
                ClusterName, ControllerName, OperatorName, ProductName, ProductVersion,
                RoleGroupName, RoleName,
            },
        },
    },
};
use strum::IntoEnumIterator;

use crate::{
    crd::{
        APP_NAME, CONTAINER_IMAGE_BASE_NAME, OPERATOR_NAME, ZOOKEEPER_ELECTION_PORT,
        ZOOKEEPER_LEADER_PORT, ZookeeperPodRef, ZookeeperRole, ZookeeperServerRoleType,
        authentication, default_listener_class,
        security::ZookeeperSecurity,
        v1alpha1::{self, ZookeeperConfig, ZookeeperConfigOverrides, ZookeeperServerRoleConfig},
    },
    zk_controller::{ZK_CONTROLLER_NAME, dereference::DereferencedObjects},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to validate authentication classes"))]
    InvalidAuthenticationClassConfiguration { source: authentication::Error },

    #[snafu(display("failed to retrieve role {role:?}"))]
    MissingRole {
        source: crate::crd::Error,
        role: String,
    },

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

    #[snafu(display("invalid environment variable override name in role group {role_group:?}"))]
    ParseEnvVarName {
        source: container::Error,
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
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// A validated, merged view of a single ZooKeeper server role group.
///
/// Built by [`validate`] from the upstream
/// [`stackable_operator::v2::role_utils::with_validated_config`] result. Carries only the
/// fields the build steps consume. The merged `envOverrides` are converted into an
/// [`EnvVarSet`] during validation so invalid names fail early.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedRoleGroupConfig {
    pub replicas: u16,
    pub config: ZookeeperConfig,
    pub config_overrides: ZookeeperConfigOverrides,
    pub env_overrides: EnvVarSet,
    pub pod_overrides: PodTemplateSpec,
    pub jvm_argument_overrides: JvmArgumentOverrides,
    pub logging: ValidatedLogging,
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
    pub role_config: Option<ValidatedRoleConfig>,
    pub role_group_configs:
        BTreeMap<ZookeeperRole, BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>>,
    /// The cluster's operation settings (pause/stop), from which the
    /// [`ClusterResourceApplyStrategy`](stackable_operator::cluster_resources::ClusterResourceApplyStrategy)
    /// is derived. Carried here so the apply step does not reach into the cluster spec.
    pub cluster_operation: ClusterOperation,
    /// Object overrides applied to the cluster's resources, carried so the apply step does not reach
    /// into the raw [`v1alpha1::ZookeeperCluster`].
    pub object_overrides: ObjectOverrides,
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
        role_config: Option<ValidatedRoleConfig>,
        role_group_configs: BTreeMap<
            ZookeeperRole,
            BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>,
        >,
        cluster_operation: ClusterOperation,
        object_overrides: ObjectOverrides,
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
        }
    }

    /// The one ZooKeeper role name (`server`).
    pub fn role_name() -> RoleName {
        RoleName::from_str(&ZookeeperRole::Server.to_string())
            .expect("the server role name is a valid role name")
    }

    /// Type-safe names for the resources of a given role group.
    pub(crate) fn resource_names(&self, role_group_name: &RoleGroupName) -> ResourceNames {
        ResourceNames {
            cluster_name: self.name.clone(),
            role_name: Self::role_name(),
            role_group_name: role_group_name.clone(),
        }
    }

    /// The RBAC ServiceAccount name for this cluster, `<cluster>-serviceaccount`.
    ///
    /// Matches the name produced by
    /// [`build_rbac_resources`](stackable_operator::commons::rbac::build_rbac_resources) so the
    /// StatefulSet can reference the ServiceAccount without depending on the built object.
    pub(crate) fn rbac_service_account_name(&self) -> ServiceAccountName {
        RbacResourceNames {
            cluster_name: self.name.clone(),
            product_name: product_name(),
        }
        .service_account_name()
    }

    /// Recommended labels for a role-group resource, using the given product version.
    ///
    /// Used for PVC templates that cannot be modified once deployed: passing a constant version
    /// (e.g. `none`) keeps those labels stable across product version upgrades.
    pub(crate) fn recommended_labels_for(
        &self,
        product_version: &ProductVersion,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        recommended_labels(
            self,
            &product_name(),
            product_version,
            &operator_name(),
            &controller_name(),
            &Self::role_name(),
            role_group_name,
        )
    }

    /// Recommended labels for a role-group resource.
    pub fn recommended_labels(&self, role_group_name: &RoleGroupName) -> Labels {
        self.recommended_labels_for(&self.product_version, role_group_name)
    }

    /// Selector labels matching the pods of a role group.
    pub fn role_group_selector(&self, role_group_name: &RoleGroupName) -> Labels {
        role_group_selector(self, &product_name(), &Self::role_name(), role_group_name)
    }

    /// Returns an [`ObjectMetaBuilder`] pre-filled with the namespace, an owner reference back to
    /// this cluster, and the recommended labels for a resource named `name` in `role_group_name`.
    ///
    /// Consolidates the metadata chain repeated by the child-resource builders. Call sites that
    /// need extra labels/annotations chain them onto the returned builder.
    pub(crate) fn object_meta(
        &self,
        name: impl Into<String>,
        role_group_name: &RoleGroupName,
    ) -> ObjectMetaBuilder {
        let mut builder = ObjectMetaBuilder::new();
        builder
            .name_and_namespace(self)
            .name(name)
            .ownerreference(ownerreference_from_resource(self, None, Some(true)))
            .with_labels(self.recommended_labels(role_group_name));
        builder
    }
}

/// The product name (`zookeeper`) as a type-safe label value.
pub(crate) fn product_name() -> ProductName {
    ProductName::from_str(APP_NAME).expect("'zookeeper' is a valid product name")
}

/// The operator name as a type-safe label value.
pub(crate) fn operator_name() -> OperatorName {
    OperatorName::from_str(OPERATOR_NAME).expect("the operator name is a valid label value")
}

/// The controller name as a type-safe label value.
pub(crate) fn controller_name() -> ControllerName {
    ControllerName::from_str(ZK_CONTROLLER_NAME)
        .expect("the controller name is a valid label value")
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

    /// The `server.<myid>` entries for `zoo.cfg`, precomputed from the expected
    /// pods so the ConfigMap builder does not need the cluster object.
    pub server_addresses: BTreeMap<String, String>,

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
    cluster_info: &KubernetesClusterInfo,
) -> Result<ValidatedCluster> {
    let image = zk
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            &operator_environment.image_repository,
            crate::built_info::PKG_VERSION,
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

    let mut role_group_configs = BTreeMap::new();
    for zk_role in ZookeeperRole::iter() {
        let role = zk.role(&zk_role).with_context(|_| MissingRoleSnafu {
            role: zk_role.to_string(),
        })?;
        let default_config = ZookeeperConfig::default_server_config(&zk.name_any(), &zk_role);

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
        role_group_configs.insert(zk_role, groups);
    }

    let name = get_cluster_name(zk).context(GetClusterNameSnafu)?;
    let namespace = get_namespace(zk).context(GetNamespaceSnafu)?;
    let uid = get_uid(zk).context(GetUidSnafu)?;

    let product_version =
        ProductVersion::from_str(&image.app_version_label_value).with_context(|_| {
            ParseProductVersionSnafu {
                product_version: image.app_version_label_value.to_string(),
            }
        })?;

    let listener_class = zk
        .role(&ZookeeperRole::Server)
        .map(|role| role.role_config.listener_class.clone())
        .unwrap_or_else(|_| default_listener_class());

    let server_addresses = server_addresses(
        &name,
        &namespace,
        &role_group_configs,
        &zookeeper_security,
        cluster_info,
    );

    let role_config = zk.role_config(&ZookeeperRole::Server).map(
        |ZookeeperServerRoleConfig { common, .. }| ValidatedRoleConfig {
            pdb: common.pod_disruption_budget.clone(),
        },
    );

    Ok(ValidatedCluster::new(
        name,
        namespace,
        uid,
        image,
        product_version,
        ValidatedClusterConfig {
            zookeeper_security,
            server_addresses,
            listener_class,
        },
        role_config,
        role_group_configs,
        zk.spec.cluster_operation.clone(),
        zk.spec.object_overrides.clone(),
    ))
}

/// Merges and validates one role group into a [`ValidatedRoleGroupConfig`].
///
/// Uses the upstream [`with_validated_config`], which merges the config fragment, the
/// `configOverrides`, the `envOverrides`, the `podOverrides` and the product-specific
/// [`JavaCommonConfig`] (including its `jvmArgumentOverrides`). The merged `envOverrides`
/// (`HashMap`) are converted into an [`EnvVarSet`] here so invalid names fail validation early.
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
) -> Result<ValidatedRoleGroupConfig> {
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

    let mut env_overrides = EnvVarSet::new();
    for (env_var_name, env_var_value) in merged.config.env_overrides {
        env_overrides = env_overrides.with_value(
            &EnvVarName::from_str(&env_var_name).with_context(|_| ParseEnvVarNameSnafu {
                role_group: role_group_name.to_owned(),
            })?,
            env_var_value,
        );
    }

    let config = merged.config.config;
    let logging = validate_logging(&config.logging, vector_aggregator_config_map_name)?;

    Ok(ValidatedRoleGroupConfig {
        replicas: merged.replicas.unwrap_or(1),
        config,
        config_overrides: merged.config.config_overrides,
        env_overrides,
        pod_overrides: merged.config.pod_overrides,
        jvm_argument_overrides: merged
            .config
            .product_specific_common_config
            .jvm_argument_overrides,
        logging,
    })
}

/// Builds the `server.<myid>` quorum entries for `zoo.cfg` from the expected pods.
///
/// The pods are predicted from the validated role-group configs (`replicas` + `myidOffset`)
/// rather than from the live cluster state, to avoid instance churn.
fn server_addresses(
    cluster_name: &ClusterName,
    namespace: &NamespaceName,
    role_group_configs: &BTreeMap<ZookeeperRole, BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>>,
    zookeeper_security: &ZookeeperSecurity,
    cluster_info: &KubernetesClusterInfo,
) -> BTreeMap<String, String> {
    let mut server_addresses = BTreeMap::new();
    for (rg_name, rg_config) in role_group_configs
        .get(&ZookeeperRole::Server)
        .into_iter()
        .flatten()
    {
        let resource_names = ResourceNames {
            cluster_name: cluster_name.clone(),
            role_name: ValidatedCluster::role_name(),
            role_group_name: rg_name.clone(),
        };
        let headless_service_name = resource_names.headless_service_name();
        let stateful_set_name = resource_names.stateful_set_name().to_string();
        for i in 0..rg_config.replicas {
            let pod_ref = ZookeeperPodRef {
                namespace: namespace.clone(),
                role_group_headless_service_name: headless_service_name.clone(),
                pod_name: format!("{stateful_set_name}-{i}"),
                zookeeper_myid: i + rg_config.config.myid_offset,
            };
            server_addresses.insert(
                format!("server.{id}", id = pod_ref.zookeeper_myid),
                format!(
                    "{internal_fqdn}:{ZOOKEEPER_LEADER_PORT}:{ZOOKEEPER_ELECTION_PORT};{client_port}",
                    internal_fqdn = pod_ref.internal_fqdn(cluster_info),
                    client_port = zookeeper_security.client_port()
                ),
            );
        }
    }
    server_addresses
}

#[cfg(test)]
mod tests {
    use stackable_operator::k8s_openapi::apimachinery::pkg::api::resource::Quantity;

    use super::*;
    use crate::zk_controller::test_support::{minimal_zk, validated_cluster};

    /// Looks up the validated, merged config of a single server role group by name.
    fn server_role_group(
        validated: &ValidatedCluster,
        role_group: &str,
    ) -> ValidatedRoleGroupConfig {
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
                productVersion: "3.9.5"
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
}
