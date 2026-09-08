//! The dereference step in the ZookeeperZnode controller.
//!
//! Fetches the parent [`v1alpha1::ZookeeperCluster`] referenced by the znode's
//! `spec.clusterRef`, plus the [`DereferencedAuthenticationClasses`] and the role Listener of that
//! cluster. Both Apply and Cleanup paths in `reconcile_znode` share this output. Synchronous
//! validation of the fetched objects happens in the validate step.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    crd::listener,
    kube::{self, runtime::reflector::ObjectRef},
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
    #[snafu(display("object does not refer to ZookeeperCluster"))]
    InvalidZkReference,

    #[snafu(display("could not find {zk:?}"))]
    FindZk {
        source: stackable_operator::client::Error,
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("could not find {zk:?}"))]
    ZkDoesNotExist {
        source: stackable_operator::client::Error,
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("failed to fetch authentication classes"))]
    FetchAuthenticationClasses { source: authentication::Error },

    #[snafu(display("failed to get the cluster name of {zk}"))]
    GetClusterName {
        source: stackable_operator::v2::controller_utils::Error,
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("failed to get the namespace of {zk}"))]
    GetNamespace {
        source: stackable_operator::v2::controller_utils::Error,
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("failed to fetch the role Listener of {zk}"))]
    FetchRoleListener {
        source: stackable_operator::client::Error,
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Kubernetes objects referenced from the [`v1alpha1::ZookeeperZnode`] spec, already fetched.
pub struct DereferencedObjects {
    pub zk: v1alpha1::ZookeeperCluster,
    /// The referenced cluster's name and namespace as typed values, from which the role Listener
    /// name and the management address are derived.
    pub zk_name: ClusterName,
    pub zk_namespace: NamespaceName,
    pub authentication_classes: DereferencedAuthenticationClasses,

    /// The role Listener of the referenced cluster, if it exists already.
    ///
    /// The znode's discovery ConfigMap advertises the addresses that the listener operator
    /// publishes on it. The Cleanup path does not need it, so a missing Listener is not an error
    /// here and never blocks finalizer removal.
    pub maybe_role_listener: Option<listener::v1alpha1::Listener>,
}

/// Fetches all Kubernetes objects referenced from the [`v1alpha1::ZookeeperZnode`] spec.
pub async fn dereference(
    client: &Client,
    znode: &v1alpha1::ZookeeperZnode,
) -> Result<DereferencedObjects> {
    let zk = find_zk_of_znode(client, znode).await?;
    let zk_ref = ObjectRef::from_obj(&zk);
    let zk_name =
        get_cluster_name(&zk).with_context(|_| GetClusterNameSnafu { zk: zk_ref.clone() })?;
    let zk_namespace =
        get_namespace(&zk).with_context(|_| GetNamespaceSnafu { zk: zk_ref.clone() })?;

    let authentication_classes = DereferencedAuthenticationClasses::fetch_references(
        client,
        &zk.spec.cluster_config.authentication,
    )
    .await
    .context(FetchAuthenticationClassesSnafu)?;

    let maybe_role_listener = fetch_role_listener(client, &zk_name, &zk_namespace, zk_ref).await?;

    Ok(DereferencedObjects {
        zk,
        zk_name,
        zk_namespace,
        authentication_classes,
        maybe_role_listener,
    })
}

async fn fetch_role_listener(
    client: &Client,
    zk_name: &ClusterName,
    zk_namespace: &NamespaceName,
    zk_ref: ObjectRef<v1alpha1::ZookeeperCluster>,
) -> Result<Option<listener::v1alpha1::Listener>> {
    let listener_name = role_listener_name(zk_name, &ZookeeperRole::Server);

    client
        .get_opt(listener_name.as_ref(), zk_namespace.as_ref())
        .await
        .with_context(|_| FetchRoleListenerSnafu { zk: zk_ref })
}

async fn find_zk_of_znode(
    client: &Client,
    znode: &v1alpha1::ZookeeperZnode,
) -> Result<v1alpha1::ZookeeperCluster> {
    let zk_ref = &znode.spec.cluster_ref;
    let (Some(zk_name), Some(zk_ns)) = (
        zk_ref.name.as_deref(),
        zk_ref.namespace_relative_from(znode),
    ) else {
        return InvalidZkReferenceSnafu.fail();
    };

    match client
        .get::<v1alpha1::ZookeeperCluster>(zk_name, zk_ns)
        .await
    {
        Ok(zk) => Ok(zk),
        Err(err) => match &err {
            stackable_operator::client::Error::GetResource {
                source: kube::Error::Api(s),
                ..
            } if s.is_not_found() => Err(err).with_context(|_| ZkDoesNotExistSnafu {
                zk: ObjectRef::new(zk_name).within(zk_ns),
            }),
            _ => Err(err).with_context(|_| FindZkSnafu {
                zk: ObjectRef::new(zk_name).within(zk_ns),
            }),
        },
    }
}
