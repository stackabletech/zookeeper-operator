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
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::ClusterName,
        },
    },
};

use crate::{
    crd::{
        ZookeeperRole,
        authentication::{self, DereferencedAuthenticationClasses},
        role_listener_name, v1alpha1,
    },
    discovery::discovery_config_map_name,
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

    #[snafu(display("failed to get the cluster UID"))]
    GetUid {
        source: stackable_operator::v2::controller_utils::Error,
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

    /// The discovery ConfigMap as stored by an earlier reconciliation (named by
    /// [`discovery_config_map_name`]), or `None` while none owned by this cluster is stored.
    /// The build step re-emits it while the role Listener has no usable address.
    pub existing_discovery_config_map: Option<ConfigMap>,
}

/// Fetches all Kubernetes objects referenced from the [`v1alpha1::ZookeeperCluster`] spec
/// (only considering a ConfigMap owned by this cluster).
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
    let uid = get_uid(zk).context(GetUidSnafu)?;

    let discovery_config_map_name = discovery_config_map_name(&cluster_name);
    let existing_discovery_config_map = client
        .get_opt::<ConfigMap>(discovery_config_map_name.as_ref(), namespace.as_ref())
        .await
        .context(FetchExistingDiscoveryConfigMapSnafu {
            config_map: discovery_config_map_name.as_ref(),
        })?
        .filter(|config_map| is_owned_by(config_map, &uid));

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

/// Whether `config_map` is owned by the object with the given UID.
///
/// A ConfigMap of the same name left behind by a deleted cluster (e.g. after
/// `kubectl delete --cascade=orphan`) must not be re-emitted for a new cluster.
fn is_owned_by(config_map: &ConfigMap, uid: &Uid) -> bool {
    config_map
        .metadata
        .owner_references
        .iter()
        .flatten()
        .any(|owner_reference| owner_reference.uid == uid.to_string())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::{
        ObjectMeta, OwnerReference,
    };

    use super::*;
    use crate::zk_controller::test_support::CLUSTER_UID;

    fn config_map_owned_by(owner_uids: &[&str]) -> ConfigMap {
        ConfigMap {
            metadata: ObjectMeta {
                owner_references: Some(
                    owner_uids
                        .iter()
                        .map(|uid| OwnerReference {
                            uid: (*uid).to_owned(),
                            ..OwnerReference::default()
                        })
                        .collect(),
                ),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        }
    }

    fn cluster_uid() -> Uid {
        Uid::from_str(CLUSTER_UID).expect("valid UID")
    }

    #[test]
    fn config_map_owned_by_this_cluster_is_considered() {
        assert!(is_owned_by(
            &config_map_owned_by(&[CLUSTER_UID]),
            &cluster_uid()
        ));
    }

    /// E.g. left behind by a deleted cluster of the same name.
    #[test]
    fn config_map_owned_by_another_object_is_ignored() {
        assert!(!is_owned_by(
            &config_map_owned_by(&["00000000-0000-0000-0000-000000000000"]),
            &cluster_uid()
        ));
    }

    #[test]
    fn config_map_without_owner_references_is_ignored() {
        assert!(!is_owned_by(&ConfigMap::default(), &cluster_uid()));
    }
}
