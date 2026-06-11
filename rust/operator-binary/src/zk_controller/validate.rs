//! The validate step in the ZookeeperCluster controller.
//!
//! Synchronously validates and merges the cluster spec together with the
//! dereferenced inputs into a [`ValidatedCluster`], which is the single input
//! consumed by the build steps (e.g. the ConfigMap builder). After this step
//! the rest of `reconcile_zk` no longer needs to reach into the
//! [`v1alpha1::ZookeeperCluster`] for configuration (only for the owner
//! reference).

use std::{collections::BTreeMap, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
    config::fragment,
    k8s_openapi::{api::core::v1::PodTemplateSpec, apimachinery::pkg::apis::meta::v1::ObjectMeta},
    kube::{Resource, ResourceExt},
    role_utils::RoleGroup,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        HasName, HasUid,
        builder::pod::container::{self, EnvVarName, EnvVarSet},
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        jvm_argument_overrides::JvmArgumentOverrides,
        role_utils::{JavaCommonConfig, with_validated_config},
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::ClusterName,
        },
    },
};
use strum::IntoEnumIterator;

use crate::{
    crd::{
        CONTAINER_IMAGE_BASE_NAME, ZOOKEEPER_ELECTION_PORT, ZOOKEEPER_LEADER_PORT, ZookeeperPodRef,
        ZookeeperRole, ZookeeperServerRoleType, authentication,
        security::ZookeeperSecurity,
        v1alpha1::{self, ZookeeperConfig, ZookeeperConfigOverrides, ZookeeperServerRoleConfig},
    },
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

    #[snafu(display("failed to retrieve role {role:?}"))]
    MissingRole {
        source: crate::crd::Error,
        role: String,
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
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// A validated, merged view of a single ZooKeeper server role group.
///
/// Built by [`validate`] from the upstream
/// [`stackable_operator::v2::role_utils::with_validated_config`] result. Carries only the
/// fields the build steps consume. The merged `envOverrides` are converted into an
/// [`EnvVarSet`] during validation so invalid names fail early.
#[derive(Clone, Debug, PartialEq)]
pub struct ZookeeperRoleGroupConfig {
    pub replicas: u16,
    pub config: ZookeeperConfig,
    pub config_overrides: ZookeeperConfigOverrides,
    pub env_overrides: EnvVarSet,
    pub pod_overrides: PodTemplateSpec,
    pub jvm_argument_overrides: JvmArgumentOverrides,
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
    pub cluster_config: ValidatedClusterConfig,
    pub role_group_configs: BTreeMap<ZookeeperRole, BTreeMap<String, ZookeeperRoleGroupConfig>>,
}

impl ValidatedCluster {
    pub fn new(
        name: ClusterName,
        namespace: NamespaceName,
        uid: Uid,
        image: ResolvedProductImage,
        cluster_config: ValidatedClusterConfig,
        role_group_configs: BTreeMap<ZookeeperRole, BTreeMap<String, ZookeeperRoleGroupConfig>>,
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
            cluster_config,
            role_group_configs,
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

    let mut role_group_configs = BTreeMap::new();
    for zk_role in ZookeeperRole::iter() {
        let role = zk.role(&zk_role).with_context(|_| MissingRoleSnafu {
            role: zk_role.to_string(),
        })?;
        let default_config = ZookeeperConfig::default_server_config(&zk.name_any(), &zk_role);

        let mut groups = BTreeMap::new();
        for (rg_name, rg) in &role.role_groups {
            let validated_rg = validate_role_group_config(rg_name, rg, role, &default_config)?;
            groups.insert(rg_name.clone(), validated_rg);
        }
        role_group_configs.insert(zk_role, groups);
    }

    let name = get_cluster_name(zk).context(GetClusterNameSnafu)?;
    let namespace = get_namespace(zk).context(GetNamespaceSnafu)?;
    let uid = get_uid(zk).context(GetUidSnafu)?;

    let server_addresses = server_addresses(
        zk,
        &namespace,
        &role_group_configs,
        &zookeeper_security,
        cluster_info,
    );

    Ok(ValidatedCluster::new(
        name,
        namespace,
        uid,
        image,
        ValidatedClusterConfig {
            zookeeper_security,
            server_addresses,
        },
        role_group_configs,
    ))
}

/// Merges and validates one role group into a [`ZookeeperRoleGroupConfig`].
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

    let mut env_overrides = EnvVarSet::new();
    for (env_var_name, env_var_value) in merged.config.env_overrides {
        env_overrides = env_overrides.with_value(
            &EnvVarName::from_str(&env_var_name).with_context(|_| ParseEnvVarNameSnafu {
                role_group: role_group_name.to_owned(),
            })?,
            env_var_value,
        );
    }

    Ok(ZookeeperRoleGroupConfig {
        replicas: merged.replicas.unwrap_or(1),
        config: merged.config.config,
        config_overrides: merged.config.config_overrides,
        env_overrides,
        pod_overrides: merged.config.pod_overrides,
        jvm_argument_overrides: merged
            .config
            .product_specific_common_config
            .jvm_argument_overrides,
    })
}

/// Builds the `server.<myid>` quorum entries for `zoo.cfg` from the expected pods.
///
/// The pods are predicted from the validated role-group configs (`replicas` + `myidOffset`)
/// rather than from the live cluster state, to avoid instance churn.
fn server_addresses(
    zk: &v1alpha1::ZookeeperCluster,
    namespace: &NamespaceName,
    role_group_configs: &BTreeMap<ZookeeperRole, BTreeMap<String, ZookeeperRoleGroupConfig>>,
    zookeeper_security: &ZookeeperSecurity,
    cluster_info: &KubernetesClusterInfo,
) -> BTreeMap<String, String> {
    let mut server_addresses = BTreeMap::new();
    for (rg_name, rg_config) in role_group_configs
        .get(&ZookeeperRole::Server)
        .into_iter()
        .flatten()
    {
        let rolegroup_ref = zk.server_rolegroup_ref(rg_name);
        for i in 0..rg_config.replicas {
            let pod_ref = ZookeeperPodRef {
                namespace: namespace.to_string(),
                role_group_headless_service_name: rolegroup_ref.rolegroup_headless_service_name(),
                pod_name: format!("{role_group}-{i}", role_group = rolegroup_ref.object_name()),
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
