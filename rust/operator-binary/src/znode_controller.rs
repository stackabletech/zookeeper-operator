//! Reconciles state for ZooKeeper znodes between Kubernetes [`ZookeeperZnode`] objects and the ZooKeeper cluster
//!
//! See [`ZookeeperZnode`] for more details.

use std::{convert::Infallible, time::Duration};

use crate::{
    discovery::{self, build_discovery_configmaps},
    APP_PORT,
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    k8s_openapi::api::core::v1::{ConfigMap, Service},
    kube::{
        api::ObjectMeta,
        runtime::{
            controller::{Context, ReconcilerAction},
            finalizer,
            reflector::ObjectRef,
        },
    },
};
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperClusterRef, ZookeeperZnode};

const FIELD_MANAGER_SCOPE: &str = "zookeeperznode";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display(
        "object {} is missing metadata that should be created by the Kubernetes cluster",
        obj_ref
    ))]
    ObjectMissingMetadata { obj_ref: ObjectRef<ZookeeperZnode> },
    #[snafu(display("object {} does not refer to ZookeeperCluster", znode))]
    InvalidZkReference { znode: ObjectRef<ZookeeperZnode> },
    #[snafu(display("could not find {}", zk))]
    FindZk {
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
    #[snafu(display("failed to build discovery information for {}", znode))]
    BuildDiscoveryConfigMap {
        source: discovery::Error,
        znode: ObjectRef<ZookeeperZnode>,
    },
    #[snafu(display("failed to save discovery information for {} to {}", znode, obj_ref))]
    ApplyDiscoveryConfigMap {
        source: stackable_operator::error::Error,
        znode: ObjectRef<ZookeeperZnode>,
        obj_ref: ObjectRef<ConfigMap>,
    },
    #[snafu(display("error managing finalizer"))]
    Finalizer {
        source: finalizer::Error<Infallible>,
    },
    #[snafu(display("object {} is missing metadata to build owner reference", znode))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        znode: ObjectRef<ZookeeperZnode>,
    },
}

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

pub async fn reconcile_znode(
    znode: ZookeeperZnode,
    ctx: Context<Ctx>,
) -> Result<ReconcilerAction, Error> {
    tracing::info!("Starting reconcile");
    let (ns, uid) = if let ObjectMeta {
        namespace: Some(ns),
        uid: Some(uid),
        ..
    } = &znode.metadata
    {
        (ns.clone(), uid)
    } else {
        return ObjectMissingMetadata {
            obj_ref: ObjectRef::from_obj(&znode),
        }
        .fail();
    };
    let client = &ctx.get_ref().client;

    let zk = find_zk_of_znode(client, &znode).await?;
    // Use the uid (managed by k8s itself) rather than the object name, to ensure that malicious users can't trick the controller
    // into letting them take over a znode owned by someone else
    let znode_path = format!("/znode-{}", uid);
    // Rust ZooKeeper client does not support client-side load-balancing, so use
    // (load-balanced) global service instead.
    let zk_mgmt_addr = format!(
        "{}:{}",
        zk.server_role_service_fqdn().with_context(|| NoZkFqdn {
            zk: ObjectRef::from_obj(&zk),
        })?,
        APP_PORT,
    );

    finalizer(
        &client.get_namespaced_api::<ZookeeperZnode>(&ns),
        "zookeeper.stackable.tech/znode",
        znode,
        |ev| async {
            match ev {
                finalizer::Event::Apply(znode) => {
                    znode_mgmt::ensure_znode_exists(&zk_mgmt_addr, &znode_path)
                        .await
                        .with_context(|| EnsureZnode {
                            zk: ObjectRef::from_obj(&zk),
                            znode_path: &znode_path,
                        })?;

                    let server_role_service = client
                        .get::<Service>(
                            &zk.server_role_service_name().with_context(|| NoZkSvcName {
                                zk: ObjectRef::from_obj(&zk),
                            })?,
                            zk.metadata.namespace.as_deref(),
                        )
                        .await
                        .context(FindZkSvc {
                            zk: ObjectRef::from_obj(&zk),
                        })?;
                    for discovery_cm in build_discovery_configmaps(
                        client,
                        &znode,
                        &zk,
                        &server_role_service,
                        Some(&znode_path),
                    )
                    .await
                    .with_context(|| BuildDiscoveryConfigMap {
                        znode: ObjectRef::from_obj(&znode),
                    })? {
                        client
                            .apply_patch(FIELD_MANAGER_SCOPE, &discovery_cm, &discovery_cm)
                            .await
                            .with_context(|| ApplyDiscoveryConfigMap {
                                znode: ObjectRef::from_obj(&znode),
                                obj_ref: ObjectRef::from_obj(&discovery_cm),
                            })?;
                    }

                    Ok(ReconcilerAction {
                        requeue_after: None,
                    })
                }
                finalizer::Event::Cleanup(_znode) => {
                    // Clean up znode from the ZooKeeper cluster before letting Kubernetes delete the object
                    znode_mgmt::ensure_znode_missing(&zk_mgmt_addr, &znode_path)
                        .await
                        .with_context(|| EnsureZnodeMissing {
                            zk: ObjectRef::from_obj(&zk),
                            znode_path: &znode_path,
                        })?;
                    // No need to delete the ConfigMap, since that has an OwnerReference on the ZookeeperZnode object
                    Ok(ReconcilerAction {
                        requeue_after: None,
                    })
                }
            }
        },
    )
    .await
    .map_err(Error::extract_finalizer_err)
}

async fn find_zk_of_znode(
    client: &stackable_operator::client::Client,
    znode: &ZookeeperZnode,
) -> Result<ZookeeperCluster, Error> {
    if let ZookeeperClusterRef {
        name: Some(zk_name),
        namespace: Some(zk_ns),
    } = &znode.spec.cluster_ref
    {
        client
            .get::<ZookeeperCluster>(zk_name, Some(zk_ns))
            .await
            .with_context(|| FindZk {
                zk: ObjectRef::new(zk_name).within(zk_ns),
            })
    } else {
        InvalidZkReference {
            znode: ObjectRef::from_obj(znode),
        }
        .fail()
    }
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
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
            .context(InvalidAddr { addr })?
            .next()
            .context(AddrResolution { addr })?;
        let (zk, _) = ZooKeeper::connect(&addr)
            .compat()
            .await
            .context(Connect { addr })?;
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
            .context(CreateZnodeProtocol { path })?;
        match create_res {
            Ok(_) => {
                tracing::info!(znode = "Created ZNode");
                Ok(())
            }
            Err(tokio_zookeeper::error::Create::NodeExists) => {
                tracing::info!(znode = "ZNode already exists, ignoring...");
                Ok(())
            }
            Err(err) => Err(err).context(CreateZnode { path }),
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
                .context(DeleteZnodeFindChildrenProtocol { path: &curr_path })?;
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
                        .context(DeleteZnodeProtocol { path: &curr_path })?;
                    zk = zk2;
                    match delete_res {
                        Ok(_) => tracing::info!(znode = curr_path.as_str(), "Deleted ZNode"),
                        Err(tokio_zookeeper::error::Delete::NoNode) => tracing::info!(
                            znode = curr_path.as_str(),
                            "ZNode couldn't be found, assuming it has already been deleted..."
                        ),
                        Err(err) => return Err(err).context(DeleteZnode { path }),
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
