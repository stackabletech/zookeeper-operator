//! Reconciles state for ZooKeeper znodes between Kubernetes [`v1alpha1::ZookeeperZnode`] objects and the ZooKeeper cluster
//!
//! See [`v1alpha1::ZookeeperZnode`] for more details.
use std::{borrow::Cow, convert::Infallible, sync::Arc};

use const_format::concatcp;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::product_image_selection::ResolvedProductImage,
    crd::listener,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{
        self, Resource,
        api::ObjectMeta,
        core::{DeserializeGuard, DynamicObject, error_boundary},
        runtime::{controller, finalizer, reflector::ObjectRef},
    },
    logging::controller::ReconcilerError,
    time::Duration,
    utils::cluster_info::KubernetesClusterInfo,
};
use strum::{EnumDiscriminants, IntoStaticStr};
use tracing::{debug, info};

use crate::{
    APP_NAME, OPERATOR_NAME,
    crd::{DOCKER_IMAGE_BASE_NAME, ZookeeperRole, security::ZookeeperSecurity, v1alpha1},
    discovery::{self, build_discovery_configmap},
    listener::role_listener_name,
};

pub const ZNODE_CONTROLLER_NAME: &str = "znode";
pub const ZNODE_FULL_CONTROLLER_NAME: &str = concatcp!(ZNODE_CONTROLLER_NAME, '.', OPERATOR_NAME);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("ZookeeperZnode object is invalid"))]
    InvalidZookeeperZnode {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display(
        "object is missing metadata that should be created by the Kubernetes cluster",
    ))]
    ObjectMissingMetadata,

    #[snafu(display("object does not refer to ZookeeperCluster"))]
    InvalidZkReference,

    #[snafu(display("could not find {zk:?}"))]
    FindZk {
        source: stackable_operator::client::Error,
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
    },

    ZkDoesNotExist {
        source: stackable_operator::client::Error,
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("could not find server role service name for {zk:?}"))]
    NoZkSvcName {
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("could not find server role service for {zk:?}"))]
    FindZkSvc {
        source: stackable_operator::client::Error,
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("failed to calculate FQDN for {zk:?}"))]
    NoZkFqdn {
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("failed to ensure that ZNode {znode_path:?} exists in {zk:?}"))]
    EnsureZnode {
        source: znode_mgmt::Error,
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
        znode_path: String,
    },

    #[snafu(display("failed to ensure that ZNode {znode_path:?} is missing from {zk:?}"))]
    EnsureZnodeMissing {
        source: znode_mgmt::Error,
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
        znode_path: String,
    },

    #[snafu(display("failed to build discovery information"))]
    BuildDiscoveryConfigMap { source: discovery::Error },

    #[snafu(display("failed to save discovery information to {cm:?}"))]
    ApplyDiscoveryConfigMap {
        source: stackable_operator::cluster_resources::Error,
        cm: ObjectRef<ConfigMap>,
    },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("error managing finalizer"))]
    Finalizer {
        source: finalizer::Error<Infallible>,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphans {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("failed to initialize security context"))]
    FailedToInitializeSecurityContext { source: crate::crd::security::Error },

    #[snafu(display("Znode {znode:?} missing expected keys (name and/or namespace)"))]
    ZnodeMissingExpectedKeys {
        source: stackable_operator::cluster_resources::Error,
        znode: ObjectRef<v1alpha1::ZookeeperZnode>,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    fn extract_finalizer_err(err: finalizer::Error<Self>) -> Self {
        match err {
            finalizer::Error::ApplyFailed(source) => source,
            finalizer::Error::CleanupFailed(source) => source,
            finalizer::Error::AddFinalizer(source) => Error::Finalizer {
                source: finalizer::Error::AddFinalizer(source),
            },
            finalizer::Error::RemoveFinalizer(source) => Error::Finalizer {
                source: finalizer::Error::RemoveFinalizer(source),
            },
            finalizer::Error::UnnamedObject => Error::Finalizer {
                source: finalizer::Error::UnnamedObject,
            },
            finalizer::Error::InvalidFinalizer => Error::Finalizer {
                source: finalizer::Error::InvalidFinalizer,
            },
        }
    }
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::InvalidZookeeperZnode { .. } => None,
            Error::ObjectMissingMetadata => None,
            Error::InvalidZkReference => None,
            Error::FindZk { zk, .. } => Some(zk.clone().erase()),
            Error::ZkDoesNotExist { zk, .. } => Some(zk.clone().erase()),
            Error::NoZkSvcName { zk } => Some(zk.clone().erase()),
            Error::FindZkSvc { zk, .. } => Some(zk.clone().erase()),
            Error::NoZkFqdn { zk } => Some(zk.clone().erase()),
            Error::EnsureZnode { zk, .. } => Some(zk.clone().erase()),
            Error::EnsureZnodeMissing { zk, .. } => Some(zk.clone().erase()),
            Error::BuildDiscoveryConfigMap { .. } => None,
            Error::ApplyDiscoveryConfigMap { cm, .. } => Some(cm.clone().erase()),
            Error::ApplyStatus { .. } => None,
            Error::Finalizer { .. } => None,
            Error::DeleteOrphans { .. } => None,
            Error::ObjectHasNoNamespace => None,
            Error::FailedToInitializeSecurityContext { .. } => None,
            Error::ZnodeMissingExpectedKeys { .. } => None,
        }
    }
}

pub async fn reconcile_znode(
    znode: Arc<DeserializeGuard<v1alpha1::ZookeeperZnode>>,
    ctx: Arc<Ctx>,
) -> Result<controller::Action> {
    tracing::info!("Starting reconcile");
    let znode = znode
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidZookeeperZnodeSnafu)?;
    let (ns, uid) = if let ObjectMeta {
        namespace: Some(ns),
        uid: Some(uid),
        ..
    } = &znode.metadata
    {
        (ns.clone(), uid)
    } else {
        return ObjectMissingMetadataSnafu.fail();
    };
    let client = &ctx.client;

    let zk = find_zk_of_znode(client, znode).await;
    let mut default_status_updates: Option<v1alpha1::ZookeeperZnodeStatus> = None;
    // Store the znode path in the status rather than the object itself, to ensure that only K8s administrators can override it
    let znode_path = match znode.status.as_ref().and_then(|s| s.znode_path.as_deref()) {
        Some(znode_path) => {
            debug!(znode.path = znode_path, "Using configured znode path");
            Cow::Borrowed(znode_path)
        }
        None => {
            // Default to the uid (managed by k8s itself) rather than the object name, to ensure that malicious users can't trick the controller
            // into letting them take over a znode owned by someone else
            let znode_path = format!("/znode-{}", uid);
            info!(
                znode.path = znode_path,
                "No znode path set, setting to default"
            );
            default_status_updates
                .get_or_insert_with(Default::default)
                .znode_path = Some(znode_path.clone());
            Cow::Owned(znode_path)
        }
    };

    if let Some(status) = default_status_updates {
        info!("Writing default configuration to status");
        ctx.client
            .merge_patch_status(znode, &status)
            .await
            .context(ApplyStatusSnafu)?;
    }

    finalizer(
        &client.get_api::<v1alpha1::ZookeeperZnode>(&ns),
        &format!("{OPERATOR_NAME}/znode"),
        Arc::new(znode.clone()),
        |ev| async {
            match ev {
                finalizer::Event::Apply(znode) => {
                    let zk = zk?;
                    let resolved_product_image = zk
                        .spec
                        .image
                        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION);
                    reconcile_apply(client, &znode, Ok(zk), &znode_path, &resolved_product_image)
                        .await
                }
                finalizer::Event::Cleanup(_znode) => {
                    reconcile_cleanup(client, zk, &znode_path).await
                }
            }
        },
    )
    .await
    .map_err(Error::extract_finalizer_err)
}

async fn reconcile_apply(
    client: &stackable_operator::client::Client,
    znode: &v1alpha1::ZookeeperZnode,
    zk: Result<v1alpha1::ZookeeperCluster>,
    znode_path: &str,
    resolved_product_image: &ResolvedProductImage,
) -> Result<controller::Action> {
    let zk = zk?;

    let zookeeper_security = ZookeeperSecurity::new_from_zookeeper_cluster(client, &zk)
        .await
        .context(FailedToInitializeSecurityContextSnafu)?;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        ZNODE_CONTROLLER_NAME,
        &znode.object_ref(&()),
        ClusterResourceApplyStrategy::from(&zk.spec.cluster_operation),
    )
    .context(ZnodeMissingExpectedKeysSnafu { znode })?;

    znode_mgmt::ensure_znode_exists(
        &zk_mgmt_addr(&zk, &zookeeper_security, &client.kubernetes_cluster_info)?,
        znode_path,
    )
    .await
    .with_context(|_| EnsureZnodeSnafu {
        zk: ObjectRef::from_obj(&zk),
        znode_path,
    })?;

    let listener = client
        .get::<listener::v1alpha1::Listener>(
            &role_listener_name(&zk, &ZookeeperRole::Server),
            zk.metadata
                .namespace
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
        )
        .await
        .context(FindZkSvcSnafu {
            zk: ObjectRef::from_obj(&zk),
        })?;

    let discovery_cm = build_discovery_configmap(
        &zk,
        znode,
        ZNODE_CONTROLLER_NAME,
        listener,
        Some(znode_path),
        resolved_product_image,
        &zookeeper_security,
    )
    .context(BuildDiscoveryConfigMapSnafu)?;

    let obj_ref = ObjectRef::from_obj(&discovery_cm);
    cluster_resources
        .add(client, discovery_cm)
        .await
        .with_context(|_| ApplyDiscoveryConfigMapSnafu { cm: obj_ref })?;

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphansSnafu)?;
    Ok(controller::Action::await_change())
}

async fn reconcile_cleanup(
    client: &stackable_operator::client::Client,
    zk: Result<v1alpha1::ZookeeperCluster>,
    znode_path: &str,
) -> Result<controller::Action> {
    let zk = match zk {
        Err(Error::ZkDoesNotExist { zk, .. }) => {
            tracing::info!(%zk, "Tried to clean up ZookeeperZnode bound to a ZookeeperCluster that does not exist, assuming it is already gone");
            return Ok(controller::Action::await_change());
        }
        res => res?,
    };

    let zookeeper_security = ZookeeperSecurity::new_from_zookeeper_cluster(client, &zk)
        .await
        .context(FailedToInitializeSecurityContextSnafu)?;

    // Clean up znode from the ZooKeeper cluster before letting Kubernetes delete the object
    znode_mgmt::ensure_znode_missing(
        &zk_mgmt_addr(&zk, &zookeeper_security, &client.kubernetes_cluster_info)?,
        znode_path,
    )
    .await
    .with_context(|_| EnsureZnodeMissingSnafu {
        zk: ObjectRef::from_obj(&zk),
        znode_path,
    })?;
    // No need to delete the ConfigMap, since that has an OwnerReference on the ZookeeperZnode object
    Ok(controller::Action::await_change())
}

/// Get the ZooKeeper management host:port for the operator to manage the ZooKeeper cluster.
///
/// This uses the _Server_ Role [Listener] address because it covers ZooKeeper replicas across all
/// RoleGroups.
/// This does mean that when the listenerClass is `external-stable`, the operator will need to be
/// able to access the external address (eg: Load Balancer).
///
/// [Listener]: ::stackable_operator::crd::listener::v1alpha1::Listener
// NOTE (@NickLarsenNZ): If we want to keep this traffic internal, we would need to choose one of
// the RoleGroups headless services - or make a dedicated ClusterIP service for the operator to use.
fn zk_mgmt_addr(
    zk: &v1alpha1::ZookeeperCluster,
    zookeeper_security: &ZookeeperSecurity,
    cluster_info: &KubernetesClusterInfo,
) -> Result<String> {
    // Rust ZooKeeper client does not support client-side load-balancing, so use
    // (load-balanced) global service instead.
    Ok(format!(
        "{hostname}:{port}",
        hostname = zk
            .server_role_listener_fqdn(cluster_info)
            .with_context(|| NoZkFqdnSnafu {
                zk: ObjectRef::from_obj(zk),
            })?,
        port = zookeeper_security.client_port(),
    ))
}

async fn find_zk_of_znode(
    client: &stackable_operator::client::Client,
    znode: &v1alpha1::ZookeeperZnode,
) -> Result<v1alpha1::ZookeeperCluster> {
    let zk_ref = &znode.spec.cluster_ref;
    if let (Some(zk_name), Some(zk_ns)) = (
        zk_ref.name.as_deref(),
        zk_ref.namespace_relative_from(znode),
    ) {
        match client
            .get::<v1alpha1::ZookeeperCluster>(zk_name, zk_ns)
            .await
        {
            Ok(zk) => Ok(zk),
            Err(err) => match &err {
                stackable_operator::client::Error::GetResource {
                    source: kube::Error::Api(kube::core::ErrorResponse { ref reason, .. }),
                    ..
                } if reason == "NotFound" => Err(err).with_context(|_| ZkDoesNotExistSnafu {
                    zk: ObjectRef::new(zk_name).within(zk_ns),
                }),
                _ => Err(err).with_context(|_| FindZkSnafu {
                    zk: ObjectRef::new(zk_name).within(zk_ns),
                }),
            },
        }
    } else {
        InvalidZkReferenceSnafu.fail()
    }
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::ZookeeperZnode>>,
    _error: &Error,
    _ctx: Arc<Ctx>,
) -> controller::Action {
    controller::Action::requeue(*Duration::from_secs(5))
}

mod znode_mgmt {
    use std::{collections::VecDeque, net::SocketAddr};

    use snafu::{OptionExt, ResultExt, Snafu};
    use tokio::net::lookup_host;
    use tokio_zookeeper::{Acl, Permission, ZooKeeper};

    #[derive(Snafu, Debug)]
    pub enum Error {
        #[snafu(display("invalid address {}", addr))]
        InvalidAddr {
            source: std::io::Error,
            addr: String,
        },
        #[snafu(display("address {addr:?} did not resolve to any socket addresses"))]
        AddrResolution { addr: String },
        #[snafu(display("failed to connect to {addr:?}"))]
        Connect {
            source: tokio_zookeeper::error::Error,
            addr: SocketAddr,
        },
        #[snafu(display("protocol error creating znode {path:?}"))]
        CreateZnodeProtocol {
            source: tokio_zookeeper::error::Error,
            path: String,
        },
        #[snafu(display("failed to create znode {path:?}"))]
        CreateZnode {
            source: tokio_zookeeper::error::Create,
            path: String,
        },
        #[snafu(display("protocol error deleting znode {path:?}"))]
        DeleteZnodeProtocol {
            source: tokio_zookeeper::error::Error,
            path: String,
        },
        #[snafu(display("failed to delete znode {path:?}"))]
        DeleteZnode {
            source: tokio_zookeeper::error::Delete,
            path: String,
        },
        #[snafu(display("failed to find children to delete of {path:?}"))]
        DeleteZnodeFindChildrenProtocol {
            source: tokio_zookeeper::error::Error,
            path: String,
        },
    }

    async fn connect(addr: &str) -> Result<ZooKeeper, Error> {
        tracing::debug!(addr, "Connecting to ZooKeeper");
        // TODO: Happy eyeballs?
        let addr = lookup_host(addr)
            .await
            .context(InvalidAddrSnafu { addr })?
            .next()
            .context(AddrResolutionSnafu { addr })?;
        let (zk, _) = ZooKeeper::connect(&addr)
            .await
            .context(ConnectSnafu { addr })?;
        tracing::debug!("Connected to ZooKeeper");
        Ok(zk)
    }

    #[tracing::instrument]
    /// Creates a znode, and ensure that any metadata (such as ACLs) match the desired state
    pub async fn ensure_znode_exists(addr: &str, path: &str) -> Result<(), Error> {
        tracing::info!(znode = path, "Creating ZNode");
        let zk = connect(addr).await?;
        let create_res = zk
            .create(
                path,
                vec![],
                vec![Acl {
                    perms: Permission::ALL,
                    scheme: "world".to_string(),
                    id: "anyone".to_string(),
                }],
                tokio_zookeeper::CreateMode::Persistent,
            )
            .await
            .context(CreateZnodeProtocolSnafu { path })?;
        match create_res {
            Ok(_) => {
                tracing::info!(znode = "Created ZNode");
                Ok(())
            }
            Err(tokio_zookeeper::error::Create::NodeExists) => {
                tracing::info!(znode = "ZNode already exists, ignoring...");
                Ok(())
            }
            Err(err) => Err(err).context(CreateZnodeSnafu { path }),
        }
    }

    #[tracing::instrument]
    /// Deletes a znode recursively
    ///
    /// Returns `Ok` if the znode could not be found (for idempotence).
    pub async fn ensure_znode_missing(addr: &str, path: &str) -> Result<(), Error> {
        tracing::info!(znode = path, "Deleting ZNode");
        let zk = connect(addr).await?;
        let mut queue = VecDeque::new();
        queue.push_front(path.to_string());
        while let Some(curr_path) = queue.pop_front() {
            tracing::info!(
                znode = curr_path.as_str(),
                ?queue,
                "Deleting ZNode from queue"
            );
            let children = zk
                .get_children(&curr_path)
                .await
                .context(DeleteZnodeFindChildrenProtocolSnafu { path: &curr_path })?;
            match children {
                None => {
                    tracing::warn!(
                        znode = curr_path.as_str(),
                        "ZNode could not be found, assuming it has already been deleted..."
                    );
                }
                Some(children) if children.is_empty() => {
                    tracing::info!(
                        znode = curr_path.as_str(),
                        "ZNode has no children, deleting..."
                    );
                    let delete_res = zk
                        .delete(&curr_path, None)
                        .await
                        .context(DeleteZnodeProtocolSnafu { path: &curr_path })?;
                    match delete_res {
                        Ok(_) => tracing::info!(znode = curr_path.as_str(), "Deleted ZNode"),
                        Err(tokio_zookeeper::error::Delete::NoNode) => tracing::info!(
                            znode = curr_path.as_str(),
                            "ZNode couldn't be found, assuming it has already been deleted..."
                        ),
                        Err(err) => return Err(err).context(DeleteZnodeSnafu { path }),
                    }
                }
                Some(children) => {
                    tracing::info!(
                        znode = curr_path.as_str(),
                        ?children,
                        "ZNode has children, scheduling them for deletion..."
                    );
                    queue.push_front(curr_path.clone());
                    for child in children {
                        queue.push_front(format!("{}/{}", curr_path, child));
                    }
                }
            }
        }
        Ok(())
    }
}
