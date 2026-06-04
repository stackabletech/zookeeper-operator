//! The validate step in the ZookeeperCluster controller.
//!
//! Synchronously validates and merges the cluster spec together with the
//! dereferenced inputs into a [`ValidatedCluster`], which is the single input
//! consumed by the build steps (e.g. the ConfigMap builder). After this step
//! the rest of `reconcile_zk` no longer needs to reach into the
//! [`v1alpha1::ZookeeperCluster`] for configuration (only for the owner
//! reference).

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
    config::fragment,
    kube::ResourceExt,
    role_utils::JavaCommonConfig,
    utils::cluster_info::KubernetesClusterInfo,
};
use strum::IntoEnumIterator;

use crate::{
    crd::{
        CONTAINER_IMAGE_BASE_NAME, ZOOKEEPER_ELECTION_PORT, ZOOKEEPER_LEADER_PORT, ZookeeperRole,
        authentication,
        security::ZookeeperSecurity,
        v1alpha1::{self, ZookeeperConfig, ZookeeperConfigOverrides},
    },
    framework::role_utils::{self, with_validated_config},
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

    #[snafu(display("object defines no server role"))]
    NoServerRole,

    #[snafu(display("failed to retrieve role {role:?}"))]
    MissingRole {
        source: crate::crd::Error,
        role: String,
    },

    #[snafu(display("failed to list expected pods"))]
    ListPods { source: crate::crd::Error },

    #[snafu(display("invalid config fragment for role group {role_group:?}"))]
    InvalidConfigFragment {
        source: fragment::ValidationError,
        role_group: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// A validated, merged view of a single ZooKeeper server role group.
pub type ZookeeperRoleGroupConfig =
    role_utils::RoleGroupConfig<ZookeeperConfig, JavaCommonConfig, ZookeeperConfigOverrides>;

/// The validated [`v1alpha1::ZookeeperCluster`]. Output of the validate step and
/// the single input to the build steps.
pub struct ValidatedCluster {
    /// The cluster name. Part of the `ValidatedCluster` contract (mirrors
    /// trino-operator); consumed by the statefulset/service build steps that move
    /// off the raw CRD in the follow-up PR.
    #[allow(dead_code)]
    pub name: String,
    pub image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    pub role_group_configs: BTreeMap<ZookeeperRole, BTreeMap<String, ZookeeperRoleGroupConfig>>,
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

    let server_addresses = server_addresses(zk, &zookeeper_security, cluster_info)?;

    let mut role_group_configs = BTreeMap::new();
    for zk_role in ZookeeperRole::iter() {
        let role = zk.role(&zk_role).with_context(|_| MissingRoleSnafu {
            role: zk_role.to_string(),
        })?;
        let default_config = ZookeeperConfig::default_server_config(&zk.name_any(), &zk_role);

        let mut groups = BTreeMap::new();
        for (rg_name, rg) in &role.role_groups {
            let validated_rg = with_validated_config::<
                ZookeeperConfig,
                JavaCommonConfig,
                v1alpha1::ZookeeperConfigFragment,
                _,
                ZookeeperConfigOverrides,
            >(rg, role, &default_config)
            .with_context(|_| InvalidConfigFragmentSnafu {
                role_group: rg_name.clone(),
            })?;
            groups.insert(rg_name.clone(), validated_rg);
        }
        role_group_configs.insert(zk_role, groups);
    }

    Ok(ValidatedCluster {
        name: zk.name_any(),
        image,
        cluster_config: ValidatedClusterConfig {
            zookeeper_security,
            server_addresses,
        },
        role_group_configs,
    })
}

/// Builds the `server.<myid>` quorum entries for `zoo.cfg` from the expected pods.
fn server_addresses(
    zk: &v1alpha1::ZookeeperCluster,
    zookeeper_security: &ZookeeperSecurity,
    cluster_info: &KubernetesClusterInfo,
) -> Result<BTreeMap<String, String>> {
    Ok(zk
        .pods()
        .context(ListPodsSnafu)?
        .map(|pod| {
            (
                format!("server.{id}", id = pod.zookeeper_myid),
                format!(
                    "{internal_fqdn}:{ZOOKEEPER_LEADER_PORT}:{ZOOKEEPER_ELECTION_PORT};{client_port}",
                    internal_fqdn = pod.internal_fqdn(cluster_info),
                    client_port = zookeeper_security.client_port()
                ),
            )
        })
        .collect())
}
