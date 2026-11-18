//! Reconciles state for ZooKeeper znodes between Kubernetes [`v1alpha1::ZookeeperZnode`] objects and the ZooKeeper cluster
//!
//! See [`v1alpha1::ZookeeperZnode`] for more details.
//!
//! This is the controller driver: it runs the `dereference -> validate -> build -> apply`
//! pipeline, with each step living in its own submodule. There is no update_status step, because
//! the only status the ZookeeperZnode carries (the znode path) is written before the finalizer
//! runs.
use std::{borrow::Cow, convert::Infallible, str::FromStr, sync::Arc};

use const_format::concatcp;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::ClusterResourceApplyStrategy,
    constant,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{
        api::ObjectMeta,
        core::{DeserializeGuard, DynamicObject, error_boundary},
        runtime::{controller, finalizer, reflector::ObjectRef},
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
    utils::cluster_info::KubernetesClusterInfo,
    v2::types::{
        kubernetes::NamespaceName,
        operator::{ClusterName, ControllerName},
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};
use tracing::{debug, info};

use crate::{
    ZOOKEEPER_OPERATOR_NAME,
    crd::{ZookeeperRole, role_listener_fqdn, security::ZookeeperSecurity, v1alpha1},
    znode_controller::apply::{Applier, ensure_znode_exists},
};

pub(crate) mod apply;
pub(crate) mod build;
mod dereference;
pub(crate) mod validate;

pub const ZNODE_CONTROLLER_NAME: &str = "znode";
pub const ZNODE_FULL_CONTROLLER_NAME: &str =
    concatcp!(ZNODE_CONTROLLER_NAME, '.', ZOOKEEPER_OPERATOR_NAME);

constant!(pub(crate) CONTROLLER_NAME: ControllerName = ZNODE_CONTROLLER_NAME);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub operator_environment: OperatorEnvironmentOptions,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("ZookeeperZnode object is invalid"))]
    InvalidZookeeperZnode {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to dereference resources"))]
    Dereference { source: dereference::Error },

    #[snafu(display("failed to validate cluster"))]
    ValidateCluster { source: validate::Error },

    #[snafu(display(
        "object is missing metadata that should be created by the Kubernetes cluster",
    ))]
    ObjectMissingMetadata,

    #[snafu(display("failed to ensure that ZNode {znode_path:?} is missing from {zk:?}"))]
    EnsureZnodeMissing {
        source: znode_mgmt::Error,
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
        znode_path: String,
    },

    #[snafu(display("failed to build the Kubernetes resources"))]
    BuildResources { source: build::Error },

    #[snafu(display("failed to apply the Kubernetes resources"))]
    ApplyResources { source: apply::Error },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("error managing finalizer"))]
    Finalizer {
        source: finalizer::Error<Infallible>,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Whether the reconcile left an abandoned ZooKeeper session behind.
    ///
    /// Both the apply and the cleanup step talk to the ensemble, so either error can carry one.
    fn abandoned_zookeeper_session(&self) -> bool {
        let zookeeper_error = match self {
            Error::EnsureZnodeMissing { source, .. } => source,
            Error::ApplyResources {
                source: apply::Error::EnsureZnode { source, .. },
            } => source,
            _ => return false,
        };
        zookeeper_error.abandoned_session()
    }

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
            Error::Dereference { .. } => None,
            Error::ValidateCluster { .. } => None,
            Error::ObjectMissingMetadata => None,
            Error::EnsureZnodeMissing { zk, .. } => Some(zk.clone().erase()),
            Error::BuildResources { .. } => None,
            Error::ApplyResources { .. } => None,
            Error::ApplyStatus { .. } => None,
            Error::Finalizer { .. } => None,
        }
    }
}

/// Every Kubernetes resource produced by the client-free [`build()`](build::build) step.
///
/// The znode path inside the ZooKeeper ensemble is not a Kubernetes object, so it is absent here
/// and created by the apply step instead.
pub struct KubernetesResources {
    pub discovery_config_maps: Vec<ConfigMap>,
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

    // dereference (client required)
    // Capturing the Result here (rather than the inner value) is intentional as ZkDoesNotExist will be handled explicitly below
    let dereferenced_objects = dereference::dereference(client, znode).await;
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
        &format!("{ZOOKEEPER_OPERATOR_NAME}/znode"),
        Arc::new(znode.clone()),
        |ev| async {
            match ev {
                finalizer::Event::Apply(znode) => {
                    let dereferenced = dereferenced_objects.context(DereferenceSnafu)?;
                    let validated_znode =
                        validate::validate(&znode, &dereferenced, &ctx.operator_environment)
                            .context(ValidateClusterSnafu)?;
                    reconcile_apply(client, &validated_znode, &dereferenced, &znode_path).await
                }
                finalizer::Event::Cleanup(_znode) => {
                    let dereferenced = match dereferenced_objects {
                        Ok(d) => d,
                        Err(dereference::Error::ZkDoesNotExist { zk, .. }) => {
                            tracing::info!(%zk, "Tried to clean up ZookeeperZnode bound to a ZookeeperCluster that does not exist, assuming it is already gone");
                            return Ok(controller::Action::await_change());
                        }
                        Err(e) => return Err(e).context(DereferenceSnafu),
                    };
                    // Cleanup only needs ZookeeperSecurity to talk to the cluster; skip the
                    // apply-time image resolution in `validate` so a bad image spec can't
                    // block finalizer removal.
                    let zookeeper_security = ZookeeperSecurity::new(
                        &dereferenced.zk,
                        dereferenced.authentication_classes.clone(),
                    );
                    reconcile_cleanup(client, &dereferenced, &zookeeper_security, &znode_path)
                        .await
                }
            }
        },
    )
    .await
    .map_err(Error::extract_finalizer_err)
}

async fn reconcile_apply(
    client: &stackable_operator::client::Client,
    validated_znode: &validate::ValidatedZnode,
    dereferenced: &dereference::DereferencedObjects,
    znode_path: &str,
) -> Result<controller::Action> {
    // The znode must exist in the ZooKeeper ensemble before the discovery ConfigMap advertises it.
    ensure_znode_exists(
        &zk_mgmt_addr(
            &dereferenced.zk_name,
            &dereferenced.zk_namespace,
            &validated_znode.zookeeper_security,
            &client.kubernetes_cluster_info,
        ),
        znode_path,
    )
    .await
    .context(ApplyResourcesSnafu)?;

    // build (no client required)
    let resources = build::build(validated_znode, znode_path).context(BuildResourcesSnafu)?;

    // apply (client required)
    Applier::new(
        client,
        validated_znode,
        ClusterResourceApplyStrategy::from(&validated_znode.cluster_operation),
        &validated_znode.object_overrides,
    )
    .apply(resources)
    .await
    .context(ApplyResourcesSnafu)?;

    Ok(controller::Action::await_change())
}

async fn reconcile_cleanup(
    client: &stackable_operator::client::Client,
    dereferenced: &dereference::DereferencedObjects,
    zookeeper_security: &ZookeeperSecurity,
    znode_path: &str,
) -> Result<controller::Action> {
    // Clean up znode from the ZooKeeper cluster before letting Kubernetes delete the object
    znode_mgmt::ensure_znode_missing(
        &zk_mgmt_addr(
            &dereferenced.zk_name,
            &dereferenced.zk_namespace,
            zookeeper_security,
            &client.kubernetes_cluster_info,
        ),
        znode_path,
    )
    .await
    .with_context(|_| EnsureZnodeMissingSnafu {
        zk: ObjectRef::from_obj(&dereferenced.zk),
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
    zk_name: &ClusterName,
    zk_namespace: &NamespaceName,
    zookeeper_security: &ZookeeperSecurity,
    cluster_info: &KubernetesClusterInfo,
) -> String {
    // Rust ZooKeeper client does not support client-side load-balancing, so use
    // (load-balanced) global service instead.
    format!(
        "{hostname}:{port}",
        hostname = role_listener_fqdn(zk_name, zk_namespace, &ZookeeperRole::Server, cluster_info),
        port = zookeeper_security.client_port(),
    )
}

/// How long to wait before retrying a reconcile that left a ZooKeeper session behind.
///
/// Timing out a request unblocks the reconcile but leaves the session behind until the kernel
/// reaps it, up to ~15 minutes later (see `REQUEST_TIMEOUT` in [`znode_mgmt`]). On the usual
/// [`DEFAULT_REQUEUE`] interval those abandoned sessions stack up against an ensemble that accepts
/// connections but cannot answer in time - around 60 per ZookeeperZnode, which is also ZooKeeper's
/// default `maxClientCnxns` per client IP, past which it refuses every further connection from the
/// operator. A minute keeps a handful in flight instead, while still recovering promptly from a
/// transient blip such as a leader election.
///
/// Only requests qualify. A connect that times out usually leaves nothing behind, and waiting a
/// minute on it would slow down the Namespace deletion this is all meant to unblock.
const ABANDONED_SESSION_REQUEUE: Duration = Duration::from_secs(60);

/// How long to wait before retrying a reconcile that failed for any other reason.
const DEFAULT_REQUEUE: Duration = Duration::from_secs(5);

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::ZookeeperZnode>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> controller::Action {
    let requeue_after = if error.abandoned_zookeeper_session() {
        ABANDONED_SESSION_REQUEUE
    } else {
        DEFAULT_REQUEUE
    };
    controller::Action::requeue(*requeue_after)
}

/// Shared helpers for building validated test znodes from minimal YAML fixtures.
#[cfg(test)]
pub(crate) mod test_support {
    use stackable_operator::{
        crd::listener,
        v2::controller_utils::{get_cluster_name, get_namespace},
    };

    use crate::{
        crd::{authentication::DereferencedAuthenticationClasses, v1alpha1},
        zk_controller::test_support::{minimal_zk, operator_environment},
        znode_controller::{
            dereference::DereferencedObjects,
            validate::{ValidatedZnode, validate},
        },
    };

    /// Parses a minimal `ZookeeperZnode` test fixture, defaulting `namespace`/`uid` so the validate
    /// step can build a [`ValidatedZnode`].
    pub fn minimal_znode(yaml: &str) -> v1alpha1::ZookeeperZnode {
        let mut znode: v1alpha1::ZookeeperZnode =
            yaml_serde::from_str(yaml).expect("invalid test ZookeeperZnode YAML");
        znode
            .metadata
            .namespace
            .get_or_insert_with(|| "default".to_owned());
        znode
            .metadata
            .uid
            .get_or_insert_with(|| "e5dbf9c2-d8b0-4c1e-9f4a-1d2e3f4a5b6c".to_owned());
        znode
    }

    /// The `ZookeeperCluster` that the znode fixtures reference. The znode validate step reads the
    /// image, security settings and cluster operation from it.
    pub fn referenced_zk() -> v1alpha1::ZookeeperCluster {
        minimal_zk(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: simple-zookeeper
            spec:
              image:
                productVersion: "3.9.6"
              servers:
                roleGroups:
                  default:
                    replicas: 3
            "#,
        )
    }

    /// Runs the real validate step against a minimal (auth-free) fixture and the referenced
    /// cluster's role Listener, returning the result so tests can assert on validation errors.
    pub fn try_validate(
        znode: &v1alpha1::ZookeeperZnode,
        maybe_role_listener: Option<listener::v1alpha1::Listener>,
    ) -> Result<ValidatedZnode, super::validate::Error> {
        let zk = referenced_zk();
        validate(
            znode,
            &DereferencedObjects {
                zk_name: get_cluster_name(&zk).expect("the fixture has a valid cluster name"),
                zk_namespace: get_namespace(&zk).expect("the fixture has a namespace"),
                zk,
                authentication_classes: DereferencedAuthenticationClasses::new_for_tests(),
                maybe_role_listener,
            },
            &operator_environment(),
        )
    }

    /// Runs the real validate step against a minimal (auth-free) fixture whose role Listener
    /// publishes `node-0:2282`, the secure client port that the referenced cluster serves because
    /// its fixture keeps TLS enabled.
    pub fn validated_znode(znode: &v1alpha1::ZookeeperZnode) -> ValidatedZnode {
        use crate::{
            crd::ZOOKEEPER_SERVER_PORT_NAME,
            listener_addresses::test_support::{ingress_address, role_listener},
        };

        try_validate(
            znode,
            Some(role_listener(Some(vec![ingress_address(
                "node-0",
                ZOOKEEPER_SERVER_PORT_NAME,
                2282,
            )]))),
        )
        .expect("validate should succeed for the test fixture")
    }
}

#[cfg(test)]
mod tests {
    use super::CONTROLLER_NAME;

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *CONTROLLER_NAME;
    }
}

mod znode_mgmt {
    use std::{collections::VecDeque, future::Future, net::SocketAddr, time::Duration};

    use snafu::{OptionExt, ResultExt, Snafu};
    use tokio::{net::lookup_host, time::timeout};
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
        #[snafu(display("failed to resolve {addr:?} within {timeout:?}"))]
        ResolveTimeout {
            source: tokio::time::error::Elapsed,
            addr: String,
            timeout: Duration,
        },
        #[snafu(display("failed to connect to {addr:?}"))]
        Connect {
            source: tokio_zookeeper::error::Error,
            addr: SocketAddr,
        },
        #[snafu(display("failed to connect to {addr:?} within {timeout:?}"))]
        ConnectTimeout {
            source: tokio::time::error::Elapsed,
            addr: SocketAddr,
            timeout: Duration,
        },
        #[snafu(display(
            "{request} request for znode {path:?} was not answered within {timeout:?}"
        ))]
        RequestTimeout {
            source: tokio::time::error::Elapsed,
            request: &'static str,
            path: String,
            timeout: Duration,
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

    impl Error {
        /// Whether giving up left an established ZooKeeper session behind to be reaped.
        ///
        /// Only a request can: the session is established by then, and abandoning it costs the
        /// full retransmission budget (see [`REQUEST_TIMEOUT`]). A connect that times out is
        /// nearly always still in [`ZooKeeper::connect`]'s `TcpStream::connect`, which has spawned
        /// nothing yet, and resolving never reaches the ensemble at all.
        pub fn abandoned_session(&self) -> bool {
            matches!(self, Error::RequestTimeout { .. })
        }
    }

    /// How long to wait for the ensemble's address to resolve before giving up.
    ///
    /// [`lookup_host`] applies no deadline of its own, so an unresponsive CoreDNS costs
    /// `timeout × attempts × search domains` before the resolver gives up. A deleted Service is not
    /// the cause - that answers NXDOMAIN immediately - an unreachable or overloaded one is.
    ///
    /// [`lookup_host`] resolves on a blocking thread, so timing out unblocks the reconcile without
    /// aborting the resolver call itself.
    const RESOLVE_TIMEOUT: Duration = Duration::from_secs(10);

    /// How long to wait for a connection to the ZooKeeper ensemble before giving up.
    ///
    /// [`ZooKeeper::connect`] applies no deadline of its own, so without this the TCP connect falls
    /// back to the kernel's SYN retry budget, over two minutes on Linux. That matters most while a
    /// Namespace is being deleted: its contents go in no particular order, so the ensemble's
    /// Service and Pods are typically gone before the ZookeeperCluster is. The cleanup then dials a
    /// ClusterIP that no longer routes anywhere, so the SYNs are dropped rather than refused and
    /// the znode finalizer blocks Namespace termination for the whole budget.
    ///
    /// Timing out instead lets the reconcile fail fast and be retried by
    /// [`error_policy`][super::error_policy], by which point the ZookeeperCluster is usually gone
    /// too and the cleanup is skipped entirely.
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

    /// How long to wait for one request on an established ZooKeeper connection before giving up.
    ///
    /// A successful connect is no guarantee that the ensemble is still reachable for the rest of
    /// the operation, and the requests of [`ZooKeeper`] apply no deadline of their own either.
    /// Deleting a znode walks its subtree one request at a time, so the ensemble's Pods can
    /// disappear midway through. Because they go away without closing the TCP connection, the
    /// in-flight request is neither answered nor failed until the kernel exhausts its
    /// retransmission budget, roughly 15 minutes with the default `tcp_retries2` on Linux. A
    /// Namespace deletion was observed to be held up for 957 seconds by exactly this, well past
    /// the point where the znode had become unreachable for good.
    ///
    /// The deletion is resumable, so the retry from [`error_policy`][super::error_policy] either
    /// makes further progress or finds the ZookeeperCluster gone and skips the cleanup entirely.
    ///
    /// The timeout only unblocks the reconcile, not the connection underneath it: [`ZooKeeper`] is
    /// a handle to a task that `tokio-zookeeper` spawns, and dropping it makes that task queue a
    /// `CloseSession` and wait for a close that costs the same retransmission budget again.
    /// Retries must therefore be spaced out - see
    /// [`ABANDONED_SESSION_REQUEUE`][super::ABANDONED_SESSION_REQUEUE].
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

    /// Applies [`REQUEST_TIMEOUT`] to a single ZooKeeper request.
    ///
    /// The request's own result passes through untouched, so callers keep attaching their own
    /// error context to it.
    async fn with_request_timeout<T>(
        request: &'static str,
        path: &str,
        zk_request: impl Future<Output = T>,
    ) -> Result<T, Error> {
        timeout(REQUEST_TIMEOUT, zk_request)
            .await
            .context(RequestTimeoutSnafu {
                request,
                path,
                timeout: REQUEST_TIMEOUT,
            })
    }

    async fn connect(addr: &str) -> Result<ZooKeeper, Error> {
        tracing::debug!(addr, "Connecting to ZooKeeper");
        // TODO: Happy eyeballs?
        let addr = timeout(RESOLVE_TIMEOUT, lookup_host(addr))
            .await
            .context(ResolveTimeoutSnafu {
                addr,
                timeout: RESOLVE_TIMEOUT,
            })?
            .context(InvalidAddrSnafu { addr })?
            .next()
            .context(AddrResolutionSnafu { addr })?;
        let (zk, _) = timeout(CONNECT_TIMEOUT, ZooKeeper::connect(&addr))
            .await
            .context(ConnectTimeoutSnafu {
                addr,
                timeout: CONNECT_TIMEOUT,
            })?
            .context(ConnectSnafu { addr })?;
        tracing::debug!("Connected to ZooKeeper");
        Ok(zk)
    }

    #[tracing::instrument]
    /// Creates a znode, and ensure that any metadata (such as ACLs) match the desired state
    pub async fn ensure_znode_exists(addr: &str, path: &str) -> Result<(), Error> {
        tracing::info!(znode = path, "Creating ZNode");
        let zk = connect(addr).await?;
        let create_request = zk.create(
            path,
            vec![],
            vec![Acl {
                perms: Permission::ALL,
                scheme: "world".to_string(),
                id: "anyone".to_string(),
            }],
            tokio_zookeeper::CreateMode::Persistent,
        );
        let create_res = with_request_timeout("create", path, create_request)
            .await?
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
            let children =
                with_request_timeout("get_children", &curr_path, zk.get_children(&curr_path))
                    .await?
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
                    let delete_res =
                        with_request_timeout("delete", &curr_path, zk.delete(&curr_path, None))
                            .await?
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
