//! Reconciles state for ZooKeeper znodes between Kubernetes [`ZookeeperZnode`] objects and the ZooKeeper cluster
//!
//! See [`ZookeeperZnode`] for more details.

use std::{convert::Infallible, sync::Arc, time::Duration};

use crate::discovery::{self, build_discovery_configmaps};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    k8s_openapi::api::core::v1::{ConfigMap, Endpoints, Service},
    kube::{
        self,
        api::ObjectMeta,
        core::DynamicObject,
        runtime::{
            controller::{self, Context},
            finalizer,
            reflector::ObjectRef,
        },
    },
    logging::controller::ReconcilerError,
};
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperZnode};
use strum::{EnumDiscriminants, IntoStaticStr};

const FIELD_MANAGER_SCOPE: &str = "zookeeperznode";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display(
        "object is missing metadata that should be created by the Kubernetes cluster",
    ))]
    ObjectMissingMetadata,
    #[snafu(display("object does not refer to ZookeeperCluster"))]
    InvalidZkReference,
    #[snafu(display("could not find {}", zk))]
    FindZk {
        source: stackable_operator::error::Error,
        zk: ObjectRef<ZookeeperCluster>,
    },
    ZkDoesNotExist {
        source: stackable_operator::error::Error,
        zk: ObjectRef<ZookeeperCluster>,
    },
    #[snafu(display("could not find server role service name for {}", zk))]
    NoZkSvcName { zk: ObjectRef<ZookeeperCluster> },
    #[snafu(display("could not find server role service for {}", zk))]
    FindZkSvc {
        source: stackable_operator::error::Error,
        zk: ObjectRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to calculate FQDN for {}", zk))]
    NoZkFqdn { zk: ObjectRef<ZookeeperCluster> },
    #[snafu(display("failed to ensure that ZNode {} exists in {}", znode_path, zk))]
    EnsureZnode {
        source: znode_mgmt::Error,
        zk: ObjectRef<ZookeeperCluster>,
        znode_path: String,
    },
    #[snafu(display("failed to ensure that ZNode {} is missing from {}", znode_path, zk))]
    EnsureZnodeMissing {
        source: znode_mgmt::Error,
        zk: ObjectRef<ZookeeperCluster>,
        znode_path: String,
    },
    #[snafu(display("failed to build discovery information"))]
    BuildDiscoveryConfigMap { source: discovery::Error },
    #[snafu(display("failed to save discovery information to {}", cm))]
    ApplyDiscoveryConfigMap {
        source: stackable_operator::error::Error,
        cm: ObjectRef<ConfigMap>,
    },
    #[snafu(display("error managing finalizer"))]
    Finalizer {
        source: finalizer::Error<Infallible>,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
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
        }
    }
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::ObjectMissingMetadata => None,
            Error::InvalidZkReference => None,
            Error::FindZk { zk, .. } => Some(zk.clone().erase()),
            Error::ZkDoesNotExist { zk, .. } => Some(zk.clone().erase()),
            Error::NoZkSvcName { zk } => Some(zk.clone().erase()),
            Error::FindZkSvc { zk, .. } => Some(zk.clone().erase()),
            Error::NoZkFqdn { zk } => Some(zk.clone().erase()),
            Error::EnsureZnode { zk, .. } => Some(zk.clone().erase()),
            Error::EnsureZnodeMissing { zk, .. } => Some(zk.clone().erase()),
            Error::BuildDiscoveryConfigMap { source: _ } => None,
            Error::ApplyDiscoveryConfigMap { cm, .. } => Some(cm.clone().erase()),
            Error::Finalizer { source: _ } => None,
            Error::ObjectMissingMetadataForOwnerRef { source: _ } => None,
        }
    }
}

pub async fn reconcile_znode(
    znode: Arc<ZookeeperZnode>,
    ctx: Context<Ctx>,
) -> Result<controller::Action> {
    tracing::info!("Starting reconcile");
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
    let client = &ctx.get_ref().client;

    let zk = find_zk_of_znode(client, &znode).await;
    // Use the uid (managed by k8s itself) rather than the object name, to ensure that malicious users can't trick the controller
    // into letting them take over a znode owned by someone else
    let znode_path = format!("/znode-{}", uid);

    finalizer(
        &client.get_namespaced_api::<ZookeeperZnode>(&ns),
        "zookeeper.stackable.tech/znode",
        znode,
        |ev| async {
            match ev {
                finalizer::Event::Apply(znode) => {
                    reconcile_apply(client, &znode, zk, &znode_path).await
                }
                finalizer::Event::Cleanup(_znode) => reconcile_cleanup(zk, &znode_path).await,
            }
        },
    )
    .await
    .map_err(Error::extract_finalizer_err)
}

async fn reconcile_apply(
    client: &stackable_operator::client::Client,
    znode: &ZookeeperZnode,
    zk: Result<ZookeeperCluster>,
    znode_path: &str,
) -> Result<controller::Action> {
    let zk = zk?;

    let server_role_service_endpoints = client
        .get::<Endpoints>(
            &zk.server_role_service_name()
                .with_context(|| NoZkSvcNameSnafu {
                    zk: ObjectRef::from_obj(&zk),
                })?,
            zk.metadata.namespace.as_deref(),
        )
        .await
        .context(FindZkSvcSnafu {
            zk: ObjectRef::from_obj(&zk),
        })?;

    tracing::info!(
        "service endpoints: {:?}",
        server_role_service_endpoints.subsets
    );

    znode_mgmt::ensure_znode_exists(&zk_mgmt_addr(&zk)?, znode_path)
        .await
        .with_context(|_| EnsureZnodeSnafu {
            zk: ObjectRef::from_obj(&zk),
            znode_path,
        })?;

    let server_role_service = client
        .get::<Service>(
            &zk.server_role_service_name()
                .with_context(|| NoZkSvcNameSnafu {
                    zk: ObjectRef::from_obj(&zk),
                })?,
            zk.metadata.namespace.as_deref(),
        )
        .await
        .context(FindZkSvcSnafu {
            zk: ObjectRef::from_obj(&zk),
        })?;
    for discovery_cm in
        build_discovery_configmaps(client, znode, &zk, &server_role_service, Some(znode_path))
            .await
            .context(BuildDiscoveryConfigMapSnafu)?
    {
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &discovery_cm, &discovery_cm)
            .await
            .with_context(|_| ApplyDiscoveryConfigMapSnafu {
                cm: ObjectRef::from_obj(&discovery_cm),
            })?;
    }

    Ok(controller::Action::await_change())
}

async fn reconcile_cleanup(
    zk: Result<ZookeeperCluster>,
    znode_path: &str,
) -> Result<controller::Action> {
    let zk = match zk {
        Err(Error::ZkDoesNotExist { zk, .. }) => {
            tracing::info!(%zk, "Tried to clean up ZookeeperZnode bound to a ZookeeperCluster that does not exist, assuming it is already gone");
            return Ok(controller::Action::await_change());
        }
        res => res?,
    };
    // Clean up znode from the ZooKeeper cluster before letting Kubernetes delete the object
    znode_mgmt::ensure_znode_missing(&zk_mgmt_addr(&zk)?, znode_path)
        .await
        .with_context(|_| EnsureZnodeMissingSnafu {
            zk: ObjectRef::from_obj(&zk),
            znode_path,
        })?;
    // No need to delete the ConfigMap, since that has an OwnerReference on the ZookeeperZnode object
    Ok(controller::Action::await_change())
}

fn zk_mgmt_addr(zk: &ZookeeperCluster) -> Result<String> {
    // Rust ZooKeeper client does not support client-side load-balancing, so use
    // (load-balanced) global service instead.
    Ok(format!(
        "{}:{}",
        zk.server_role_service_fqdn()
            .with_context(|| NoZkFqdnSnafu {
                zk: ObjectRef::from_obj(zk),
            })?,
        zk.client_port(),
    ))
}

async fn find_zk_of_znode(
    client: &stackable_operator::client::Client,
    znode: &ZookeeperZnode,
) -> Result<ZookeeperCluster> {
    let zk_ref = &znode.spec.cluster_ref;
    if let (Some(zk_name), Some(zk_ns)) = (
        zk_ref.name.as_deref(),
        zk_ref.namespace_relative_from(znode),
    ) {
        match client.get::<ZookeeperCluster>(zk_name, Some(zk_ns)).await {
            Ok(zk) => Ok(zk),
            Err(err) => match &err {
                stackable_operator::error::Error::KubeError {
                    source: kube::Error::Api(kube::core::ErrorResponse { ref reason, .. }),
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

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> controller::Action {
    controller::Action::requeue(Duration::from_secs(5))
}

mod znode_mgmt {
    use futures::compat::Future01CompatExt;
    use snafu::{OptionExt, ResultExt, Snafu};
    use std::{collections::VecDeque, net::SocketAddr};
    use tokio::net::lookup_host;
    use tokio_zookeeper::{Acl, Permission, ZooKeeper};

    #[derive(Snafu, Debug)]
    pub enum Error {
        #[snafu(display("invalid address {}", addr))]
        InvalidAddr {
            source: std::io::Error,
            addr: String,
        },
        #[snafu(display("address {} did not resolve to any socket addresses", addr))]
        AddrResolution { addr: String },
        #[snafu(display("failed to connect to {}", addr))]
        Connect {
            #[snafu(source(from(failure::Error, failure::Error::compat)))]
            source: failure::Compat<failure::Error>,
            addr: SocketAddr,
        },
        #[snafu(display("protocol error creating znode {}", path))]
        CreateZnodeProtocol {
            #[snafu(source(from(failure::Error, failure::Error::compat)))]
            source: failure::Compat<failure::Error>,
            path: String,
        },
        #[snafu(display("failed to create znode {}", path))]
        CreateZnode {
            #[snafu(source(from(tokio_zookeeper::error::Create, failure::Fail::compat)))]
            source: failure::Compat<tokio_zookeeper::error::Create>,
            path: String,
        },
        #[snafu(display("protocol error deleting znode {}", path))]
        DeleteZnodeProtocol {
            #[snafu(source(from(failure::Error, failure::Error::compat)))]
            source: failure::Compat<failure::Error>,
            path: String,
        },
        #[snafu(display("failed to delete znode {}", path))]
        DeleteZnode {
            #[snafu(source(from(tokio_zookeeper::error::Delete, failure::Fail::compat)))]
            source: failure::Compat<tokio_zookeeper::error::Delete>,
            path: String,
        },
        #[snafu(display("failed to find children to delete of {}", path))]
        DeleteZnodeFindChildrenProtocol {
            #[snafu(source(from(failure::Error, failure::Error::compat)))]
            source: failure::Compat<failure::Error>,
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
            .compat()
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
        let (_zk, create_res) = zk
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
            .compat()
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
        let mut zk = connect(addr).await?;
        let mut queue = VecDeque::new();
        queue.push_front(path.to_string());
        while let Some(curr_path) = queue.pop_front() {
            tracing::info!(
                znode = curr_path.as_str(),
                ?queue,
                "Deleting ZNode from queue"
            );
            let (zk2, children) = zk
                .get_children(&curr_path)
                .compat()
                .await
                .context(DeleteZnodeFindChildrenProtocolSnafu { path: &curr_path })?;
            zk = zk2;
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
                    let (zk2, delete_res) = zk
                        .delete(&curr_path, None)
                        .compat()
                        .await
                        .context(DeleteZnodeProtocolSnafu { path: &curr_path })?;
                    zk = zk2;
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
