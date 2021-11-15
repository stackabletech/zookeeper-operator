//! Ensures that ZooKeeper ZNodes (filesystem nodes) exist for each [`ZookeeperZnode`], and creates discovery [`ConfigMap`]s for them

use std::{convert::Infallible, time::Duration};

use crate::utils::{apply_owned, controller_reference_to_obj};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{
        self,
        api::ObjectMeta,
        runtime::{
            controller::{Context, ReconcilerAction},
            finalizer,
            reflector::ObjectRef,
        },
    },
};
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperClusterRef, ZookeeperZnode};

const FIELD_MANAGER: &str = "zookeeper.stackable.tech/zookeeperznode";

pub struct Ctx {
    pub kube: kube::Client,
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
    #[snafu(display("could not find {}", obj_ref))]
    FindZk {
        source: kube::Error,
        obj_ref: ObjectRef<ZookeeperCluster>,
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
    #[snafu(display("failed to save discovery information to {}", obj_ref))]
    ApplyConfigMap {
        source: kube::Error,
        obj_ref: ObjectRef<ConfigMap>,
    },
    Finalizer {
        source: finalizer::Error<Infallible>,
    },
}

impl Error {
    fn extract_finalizer_err(err: finalizer::Error<Self>) -> Self {
        match err {
            finalizer::Error::ApplyFailed { source } => source,
            finalizer::Error::CleanupFailed { source } => source,
            finalizer::Error::AddFinalizer { source } => Error::Finalizer {
                source: finalizer::Error::AddFinalizer { source },
            },
            finalizer::Error::RemoveFinalizer { source } => Error::Finalizer {
                source: finalizer::Error::RemoveFinalizer { source },
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
    let (ns, name, uid) = if let ObjectMeta {
        namespace: Some(ns),
        name: Some(name),
        uid: Some(uid),
        ..
    } = &znode.metadata
    {
        (ns.clone(), name.clone(), uid)
    } else {
        return ObjectMissingMetadata {
            obj_ref: ObjectRef::from_obj(&znode),
        }
        .fail();
    };
    let kube = ctx.get_ref().kube.clone();
    let znodes = kube::Api::<ZookeeperZnode>::namespaced(kube.clone(), &ns);

    let zk = find_zk_of_znode(&kube, &znode).await?;
    let zk_port = 2181;
    let znode_path = format!("/znode-{}", uid);
    let zk_mgmt_addr = format!(
        "{}:{}",
        zk.global_service_fqdn().with_context(|| NoZkFqdn {
            zk: ObjectRef::from_obj(&zk),
        })?,
        zk_port,
    );

    finalizer(
        &znodes,
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

                    let mut znode_conn_str = zk
                        .pods()
                        .unwrap()
                        .map(|pod| format!("{}:{}", pod.fqdn(), zk_port))
                        .collect::<Vec<_>>()
                        .join(",");
                    znode_conn_str.push_str(&znode_path);

                    let discovery_cm = ConfigMap {
                        metadata: ObjectMeta {
                            namespace: Some(ns.to_string()),
                            name: Some(name.to_string()),
                            owner_references: Some(vec![controller_reference_to_obj(&znode)]),
                            ..ObjectMeta::default()
                        },
                        data: Some([("ZOOKEEPER_BROKERS".to_string(), znode_conn_str)].into()),
                        ..ConfigMap::default()
                    };
                    apply_owned(&kube, FIELD_MANAGER, &discovery_cm)
                        .await
                        .context(ApplyConfigMap {
                            obj_ref: ObjectRef::from_obj(&discovery_cm),
                        })?;
                    Ok(ReconcilerAction {
                        requeue_after: None,
                    })
                }
                finalizer::Event::Cleanup(_znode) => {
                    znode_mgmt::ensure_znode_missing(&zk_mgmt_addr, &znode_path)
                        .await
                        .with_context(|| EnsureZnodeMissing {
                            zk: ObjectRef::from_obj(&zk),
                            znode_path: &znode_path,
                        })?;
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
    kube: &kube::Client,
    znode: &ZookeeperZnode,
) -> Result<ZookeeperCluster, Error> {
    if let ZookeeperClusterRef {
        name: Some(zk_name),
        namespace: Some(zk_ns),
    } = &znode.spec.cluster_ref
    {
        let zks = kube::Api::<ZookeeperCluster>::namespaced(kube.clone(), zk_ns);
        zks.get(zk_name).await.with_context(|| FindZk {
            obj_ref: ObjectRef::new(zk_name).within(zk_ns),
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
        Ok(zk)
    }

    #[tracing::instrument]
    pub async fn ensure_znode_exists(addr: &str, path: &str) -> Result<(), Error> {
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
                tracing::info!("Created ZNode");
                Ok(())
            }
            Err(tokio_zookeeper::error::Create::NodeExists) => {
                tracing::info!("ZNode already exists, ignoring...");
                Ok(())
            }
            Err(err) => Err(err).context(CreateZnode { path }),
        }
    }

    #[tracing::instrument]
    pub async fn ensure_znode_missing(addr: &str, path: &str) -> Result<(), Error> {
        let mut zk = connect(addr).await?;
        let mut queue = VecDeque::new();
        queue.push_front(path.to_string());
        while let Some(curr_path) = queue.pop_front() {
            tracing::info!(
                path = curr_path.as_str(),
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
                        path = curr_path.as_str(),
                        "ZNode could not be found, assuming it has already been deleted..."
                    );
                }
                Some(children) if children.is_empty() => {
                    tracing::info!(
                        path = curr_path.as_str(),
                        "ZNode has no children, deleting..."
                    );
                    let (zk2, delete_res) = zk
                        .delete(&curr_path, None)
                        .compat()
                        .await
                        .context(DeleteZnodeProtocol { path: &curr_path })?;
                    zk = zk2;
                    match delete_res {
                        Ok(_) => tracing::info!(path = curr_path.as_str(), "Deleted ZNode"),
                        Err(tokio_zookeeper::error::Delete::NoNode) => tracing::info!(
                            path = curr_path.as_str(),
                            "ZNode couldn't be found, assuming it has already been deleted..."
                        ),
                        Err(err) => return Err(err).context(DeleteZnode { path }),
                    }
                }
                Some(children) => {
                    tracing::info!(
                        path = curr_path.as_str(),
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
