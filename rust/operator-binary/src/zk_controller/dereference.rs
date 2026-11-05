//! The dereference step in the ZookeeperCluster controller.
//!
//! Fetches all Kubernetes objects referenced by the [`v1alpha1::ZookeeperCluster`] spec and
//! returns them in [`DereferencedObjects`]. Synchronous validation of the fetched objects
//! (image resolution, config validation, security struct assembly) happens in the
//! validate step.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    crd::listener,
    k8s_openapi::api::core::v1::ConfigMap,
    v2::{
        controller_utils::{get_cluster_name, get_namespace},
        types::{kubernetes::NamespaceName, operator::ClusterName},
    },
};

use crate::crd::{
    ZookeeperRole,
    authentication::{self, DereferencedAuthenticationClasses},
    role_listener_name, v1alpha1,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to fetch authentication classes"))]
    FetchAuthenticationClasses { source: authentication::Error },

    #[snafu(display("failed to get the cluster name"))]
    GetClusterName {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to get the cluster namespace"))]
    GetNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to fetch the role Listener"))]
    FetchRoleListener {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to fetch the discovery ConfigMap {config_map}"))]
    FetchExistingDiscoveryConfigMap {
        source: stackable_operator::client::Error,
        config_map: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Kubernetes objects referenced from the [`v1alpha1::ZookeeperCluster`] spec, already fetched but
/// not yet validated.
pub struct DereferencedObjects {
    pub authentication_classes: DereferencedAuthenticationClasses,

    /// The role Listener as created by an earlier reconciliation, if it exists already.
    ///
    /// The discovery ConfigMap advertises the addresses that the listener operator publishes on
    /// this object, so it can only be built once the Listener exists and carries them. The
    /// controller watches Listeners, so a reconciliation is triggered as soon as that happens.
    pub maybe_role_listener: Option<listener::v1alpha1::Listener>,

    /// The discovery ConfigMap as stored by an earlier reconciliation, or `None` before the first
    /// successful build. The build step re-emits it while the role Listener has no usable address.
    pub existing_discovery_config_map: Option<ConfigMap>,
}

/// Fetches all Kubernetes objects referenced from the [`v1alpha1::ZookeeperCluster`] spec.
pub async fn dereference(
    client: &Client,
    zk: &v1alpha1::ZookeeperCluster,
) -> Result<DereferencedObjects> {
    let cluster_name = get_cluster_name(zk).context(GetClusterNameSnafu)?;
    let namespace = get_namespace(zk).context(GetNamespaceSnafu)?;

    let authentication_classes = DereferencedAuthenticationClasses::fetch_references(
        client,
        &zk.spec.cluster_config.authentication,
    )
    .await
    .context(FetchAuthenticationClassesSnafu)?;

    let maybe_role_listener = fetch_role_listener(client, &cluster_name, &namespace).await?;

    let discovery_config_map_name = cluster_name.as_ref();
    let existing_discovery_config_map = client
        .get_opt::<ConfigMap>(discovery_config_map_name, namespace.as_ref())
        .await
        .context(FetchExistingDiscoveryConfigMapSnafu {
            config_map: discovery_config_map_name,
        })?;

    Ok(DereferencedObjects {
        authentication_classes,
        maybe_role_listener,
        existing_discovery_config_map,
    })
}

async fn fetch_role_listener(
    client: &Client,
    cluster_name: &ClusterName,
    namespace: &NamespaceName,
) -> Result<Option<listener::v1alpha1::Listener>> {
    let listener_name = role_listener_name(cluster_name, &ZookeeperRole::Server);

    client
        .get_opt(listener_name.as_ref(), namespace.as_ref())
        .await
        .context(FetchRoleListenerSnafu)
}
