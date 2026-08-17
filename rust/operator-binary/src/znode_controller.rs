//! Reconciles state for ZooKeeper znodes between Kubernetes [`v1alpha1::ZookeeperZnode`] objects and the ZooKeeper cluster
//!
//! See [`v1alpha1::ZookeeperZnode`] for more details.
use std::{borrow::Cow, convert::Infallible, sync::Arc};

use const_format::concatcp;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    crd::listener,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{
        Resource, ResourceExt,
        api::ObjectMeta,
        core::{DeserializeGuard, DynamicObject, error_boundary},
        runtime::{controller, finalizer, reflector::ObjectRef},
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
    status::condition::{ClusterCondition, HasStatusCondition, compute_conditions},
    utils::cluster_info::KubernetesClusterInfo,
    v2::types::common::Port,
};
use strum::{EnumDiscriminants, IntoStaticStr};
use tracing::{debug, info};

use crate::{
    APP_NAME, OPERATOR_NAME,
    crd::{ZookeeperRole, role_listener_name, security, v1alpha1},
    zk_controller::build::resource::discovery::{self, build_znode_discovery_configmap},
};

pub(crate) mod condition;
mod dereference;
pub(crate) mod lease;
pub(crate) mod mode;
pub(crate) mod run;
pub(crate) mod validate;

pub use mode::{Disposition, Mode};

pub const ZNODE_CONTROLLER_NAME: &str = "znode";
pub const ZNODE_FULL_CONTROLLER_NAME: &str = concatcp!(ZNODE_CONTROLLER_NAME, '.', OPERATOR_NAME);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    /// Product image repository, used to resolve the referenced cluster's image for the discovery
    /// ConfigMap's version label. Sourced from the operator environment (fallback) or the agent's
    /// `--image-repository` flag.
    pub image_repository: String,
    /// Which `ZookeeperZnode`s this instance is responsible for reconciling. See [`Mode`].
    pub mode: Mode,
    /// The x509 principal (subject DN) the agent authenticates as, read from its mounted client
    /// certificate at startup. `None` for the operator fallback (which connects in plaintext) and
    /// for agents without a credential directory. When set, agent-created znodes are ACL'd to this
    /// principal instead of `world:anyone` (step 6).
    pub platform_access_principal: Option<String>,
    /// Directory holding the agent's mounted client credential (`tls.crt` / `tls.key` / `ca.crt`).
    /// When set, the agent connects to ZooKeeper over mutual TLS (step 5). `None` ⇒ plaintext.
    pub platform_access_cert_dir: Option<std::path::PathBuf>,
    /// Directory holding the ZooKeeper *server's* CA (`ca.crt`), mounted from the cluster's
    /// `serverSecretClass`. The agent verifies the server certificate against this CA rather than
    /// its own credential CA, so the two may differ (cross-CA mTLS): the credential is issued by the
    /// `trustAnchorSecretClass` the servers trust, while the servers present certs from their own
    /// `serverSecretClass`. Falls back to [`Self::platform_access_cert_dir`]'s `ca.crt` when unset
    /// (the single-CA case). `None` for the operator fallback (plaintext).
    pub platform_access_server_ca_dir: Option<std::path::PathBuf>,
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
            Error::Dereference { .. } => None,
            Error::ValidateCluster { .. } => None,
            Error::ObjectMissingMetadata => None,
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

    // dereference (client required)
    // Capturing the Result here (rather than the inner value) is intentional as ZkDoesNotExist will be handled explicitly below
    let dereferenced_objects = dereference::dereference(client, znode).await;

    // Ownership split: exactly one instance — the operator fallback OR the per-cluster agent —
    // provisions each znode. Decide before any write so a foreign object is never touched.
    let dereferenced_zk = dereferenced_objects.as_ref().ok().map(|d| &d.zk);
    match ctx.mode.disposition(znode, dereferenced_zk) {
        Disposition::Reconcile => {}
        Disposition::Ignore => {
            debug!(
                znode = znode.name_any(),
                ?ctx.mode,
                "ZookeeperZnode is owned by another instance; skipping"
            );
            return Ok(controller::Action::await_change());
        }
        Disposition::ReportAgentLiveness => {
            return report_agent_liveness(client, znode, dereferenced_zk).await;
        }
    }

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
                    let dereferenced = dereferenced_objects.context(DereferenceSnafu)?;
                    let validated_znode =
                        validate::validate(&znode, &dereferenced, &ctx.image_repository)
                            .context(ValidateClusterSnafu)?;
                    let result = reconcile_apply(
                        client,
                        &validated_znode,
                        dereferenced.zk,
                        &znode_path,
                        ctx.platform_access_principal.as_deref(),
                        ctx.platform_access_cert_dir.as_deref(),
                        ctx.platform_access_server_ca_dir.as_deref(),
                    )
                    .await;

                    // Surface the outcome as a readable condition (spike 4b). Best-effort: a
                    // status-write failure must not mask the real reconcile result.
                    let condition_builder = match &result {
                        Ok(_) => condition::ZnodeConditionBuilder::provisioned(),
                        Err(error) => condition::ZnodeConditionBuilder::degraded(
                            error.category(),
                            error.to_string(),
                        ),
                    };
                    let conditions = compute_conditions(&*znode, &[&condition_builder]);
                    if let Err(status_error) =
                        write_conditions(client, &znode, conditions).await
                    {
                        tracing::warn!(
                            error = %status_error,
                            "Failed to write ZookeeperZnode condition"
                        );
                    }
                    result
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
                    // Cleanup only needs the client port to talk to the cluster; skip the
                    // apply-time image resolution in `validate` so a bad image spec can't
                    // block finalizer removal.
                    let client_port = security::client_port(&dereferenced.zk);
                    reconcile_cleanup(
                        client,
                        dereferenced.zk,
                        client_port,
                        &znode_path,
                        ctx.platform_access_cert_dir.as_deref(),
                        ctx.platform_access_server_ca_dir.as_deref(),
                    )
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
    zk: v1alpha1::ZookeeperCluster,
    znode_path: &str,
    platform_access_principal: Option<&str>,
    platform_access_cert_dir: Option<&std::path::Path>,
    platform_access_server_ca_dir: Option<&std::path::Path>,
) -> Result<controller::Action> {
    // Infallible: `ValidatedZnode`'s object reference always contains name, namespace and uid
    // (set unconditionally during the validate step), which is all `ClusterResources::new`
    // requires.
    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        ZNODE_CONTROLLER_NAME,
        &validated_znode.object_ref(&()),
        ClusterResourceApplyStrategy::from(&validated_znode.cluster_operation),
        &validated_znode.object_overrides,
    )
    .expect(
        "ClusterResources should be created because the ValidatedZnode's object reference \
         always contains name, namespace and uid",
    );

    znode_mgmt::ensure_znode_exists(
        &zk_mgmt_addr(
            &zk,
            validated_znode.client_port.clone(),
            &client.kubernetes_cluster_info,
        )?,
        znode_path,
        platform_access_principal,
        platform_access_cert_dir,
        platform_access_server_ca_dir,
    )
    .await
    .with_context(|_| EnsureZnodeSnafu {
        zk: ObjectRef::from_obj(&zk),
        znode_path,
    })?;

    let listener = client
        .get::<listener::v1alpha1::Listener>(
            role_listener_name(&zk.name_any(), &ZookeeperRole::Server).as_ref(),
            zk.metadata
                .namespace
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
        )
        .await
        .context(FindZkSvcSnafu {
            zk: ObjectRef::from_obj(&zk),
        })?;

    let discovery_cm = build_znode_discovery_configmap(
        validated_znode,
        ZNODE_CONTROLLER_NAME,
        listener,
        znode_path,
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
    zk: v1alpha1::ZookeeperCluster,
    client_port: Port,
    znode_path: &str,
    platform_access_cert_dir: Option<&std::path::Path>,
    platform_access_server_ca_dir: Option<&std::path::Path>,
) -> Result<controller::Action> {
    // Clean up znode from the ZooKeeper cluster before letting Kubernetes delete the object
    znode_mgmt::ensure_znode_missing(
        &zk_mgmt_addr(&zk, client_port, &client.kubernetes_cluster_info)?,
        znode_path,
        platform_access_cert_dir,
        platform_access_server_ca_dir,
    )
    .await
    .with_context(|_| EnsureZnodeMissingSnafu {
        zk: ObjectRef::from_obj(&zk),
        znode_path,
    })?;
    // No need to delete the ConfigMap, since that has an OwnerReference on the ZookeeperZnode object
    Ok(controller::Action::await_change())
}

/// The operator's handling of a znode a per-cluster agent owns (spike step 4c): it does not
/// provision it, but if the agent's liveness [`lease`] is stale or absent it records an
/// `AgentUnavailable` condition — because the agent is the sole writer of the znode's status, so
/// without this nothing reports why provisioning stopped. Writing only when the lease is stale is
/// safe against the agent by construction: that is exactly when the agent is *not* writing.
async fn report_agent_liveness(
    client: &stackable_operator::client::Client,
    znode: &v1alpha1::ZookeeperZnode,
    zk: Option<&v1alpha1::ZookeeperCluster>,
) -> Result<controller::Action> {
    // `ReportAgentLiveness` only arises when the cluster exists and has platformAccess.
    let Some(zk) = zk else {
        return Ok(controller::Action::await_change());
    };
    let namespace = zk.namespace();
    let cluster_name = zk.name_any();

    let alive = match &namespace {
        Some(namespace) => lease::is_agent_alive(client, namespace, &cluster_name)
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(%error, "Failed to read the agent liveness lease; assuming the agent is not running");
                false
            }),
        None => false,
    };

    if !alive {
        let message = format!(
            "The ZookeeperZnode agent for ZookeeperCluster {namespace}/{cluster_name} is not \
             running (its liveness lease is stale or absent); znode provisioning is paused.",
            namespace = namespace.as_deref().unwrap_or("?"),
        );
        info!(znode = znode.name_any(), "Agent unavailable; recording condition");
        let condition_builder = condition::ZnodeConditionBuilder::agent_unavailable(message);
        let conditions = compute_conditions(znode, &[&condition_builder]);
        write_conditions(client, znode, conditions).await?;
    }

    // Re-check within one lease period: a lease going stale produces no watch event, so we must poll.
    Ok(controller::Action::requeue(*Duration::from_secs(
        lease::LEASE_DURATION_SECONDS as u64,
    )))
}

/// Writes the znode's status conditions. The conditions are computed by the caller (synchronously,
/// via [`compute_conditions`]) so that no `!Send` `dyn ConditionBuilder` is held across this await —
/// otherwise the whole reconcile future stops being `Send`, which `Controller::run` requires.
async fn write_conditions(
    client: &stackable_operator::client::Client,
    znode: &v1alpha1::ZookeeperZnode,
    conditions: Vec<ClusterCondition>,
) -> Result<()> {
    // Idempotent: skip the status write when nothing changed. Writing status unconditionally on
    // every reconcile updates the object, which re-triggers the controller (its primary watch fires
    // on the status update) — a self-perpetuating reconcile loop. Only patch on an actual change.
    if conditions == znode.conditions() {
        return Ok(());
    }
    let status = v1alpha1::ZookeeperZnodeStatus {
        conditions,
        ..Default::default()
    };
    client
        .merge_patch_status(znode, &status)
        .await
        .context(ApplyStatusSnafu)?;
    Ok(())
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
    client_port: Port,
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
        port = client_port,
    ))
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::ZookeeperZnode>>,
    _error: &Error,
    _ctx: Arc<Ctx>,
) -> controller::Action {
    controller::Action::requeue(*Duration::from_secs(5))
}

mod znode_mgmt {
    use std::{
        collections::VecDeque,
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use rustls::{ClientConfig, pki_types::ServerName};
    use snafu::{OptionExt, ResultExt, Snafu};
    use tokio::net::lookup_host;
    use tokio_zookeeper::{Acl, Permission, ZooKeeper, ZooKeeperBuilder};

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
        #[snafu(display("failed to read the platform-access credential file {path:?}"))]
        ReadCredentialFile {
            source: std::io::Error,
            path: PathBuf,
        },
        #[snafu(display("failed to parse PEM from {path:?}"))]
        ParsePem {
            source: rustls::pki_types::pem::Error,
            path: PathBuf,
        },
        #[snafu(display("failed to add the platform trust anchor to the root certificate store"))]
        AddRootCert { source: rustls::Error },
        #[snafu(display("failed to build the mTLS client config"))]
        BuildTlsConfig { source: rustls::Error },
        #[snafu(display("invalid TLS server name {host:?}"))]
        InvalidServerName {
            source: rustls::pki_types::InvalidDnsNameError,
            host: String,
        },
    }

    /// Connects to ZooKeeper, over mutual TLS when `cert_dir` is set (the agent) or plaintext
    /// otherwise (the operator fallback).
    ///
    /// `server_ca_dir` holds the CA the server certificate is verified against. It may differ from
    /// the credential's own CA (`cert_dir/ca.crt`) — the server presents a cert from its
    /// `serverSecretClass` while the agent's client cert is issued by the `trustAnchorSecretClass`.
    /// When `None`, it falls back to `cert_dir` (the single-CA case).
    async fn connect(
        addr: &str,
        cert_dir: Option<&Path>,
        server_ca_dir: Option<&Path>,
    ) -> Result<ZooKeeper, Error> {
        tracing::debug!(addr, tls = cert_dir.is_some(), "Connecting to ZooKeeper");
        // TODO: Happy eyeballs?
        let socket_addr = lookup_host(addr)
            .await
            .context(InvalidAddrSnafu { addr })?
            .next()
            .context(AddrResolutionSnafu { addr })?;
        let zk = match cert_dir {
            Some(cert_dir) => {
                // The TLS server name is the hostname portion of `addr` (the ZooKeeper listener
                // FQDN), not the resolved IP: the server certificate is issued for the FQDN.
                let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
                let server_name =
                    ServerName::try_from(host.to_owned()).context(InvalidServerNameSnafu { host })?;
                let config = build_client_config(cert_dir, server_ca_dir.unwrap_or(cert_dir))?;
                let (zk, _) = ZooKeeperBuilder::default()
                    .connect_tls(&socket_addr, config, server_name)
                    .await
                    .context(ConnectSnafu { addr: socket_addr })?;
                zk
            }
            None => {
                let (zk, _) = ZooKeeper::connect(&socket_addr)
                    .await
                    .context(ConnectSnafu { addr: socket_addr })?;
                zk
            }
        };
        tracing::debug!("Connected to ZooKeeper");
        Ok(zk)
    }

    /// Builds a rustls [`ClientConfig`] for mutual TLS.
    ///
    /// The client identity — certificate chain (`tls.crt`) and private key (`tls.key`) — comes from
    /// `cert_dir` (the mounted platform-access credential). The root store used to verify the
    /// *server* comes from `server_ca_dir/ca.crt`, which may be a different CA than the credential's
    /// own (cross-CA mTLS): the server presents a cert from its `serverSecretClass` while the client
    /// cert is issued by the `trustAnchorSecretClass` the servers trust. In the single-CA case the
    /// caller passes `cert_dir` as `server_ca_dir`.
    fn build_client_config(
        cert_dir: &Path,
        server_ca_dir: &Path,
    ) -> Result<Arc<ClientConfig>, Error> {
        use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

        let ca_path = server_ca_dir.join("ca.crt");
        let cert_path = cert_dir.join("tls.crt");
        let key_path = cert_dir.join("tls.key");

        let ca_bytes =
            std::fs::read(&ca_path).context(ReadCredentialFileSnafu { path: &ca_path })?;
        let mut roots = rustls::RootCertStore::empty();
        for ca in CertificateDer::pem_slice_iter(&ca_bytes) {
            let ca = ca.context(ParsePemSnafu { path: &ca_path })?;
            roots.add(ca).context(AddRootCertSnafu)?;
        }

        let cert_bytes =
            std::fs::read(&cert_path).context(ReadCredentialFileSnafu { path: &cert_path })?;
        let cert_chain = CertificateDer::pem_slice_iter(&cert_bytes)
            .collect::<Result<Vec<_>, _>>()
            .context(ParsePemSnafu { path: &cert_path })?;
        let key_bytes =
            std::fs::read(&key_path).context(ReadCredentialFileSnafu { path: &key_path })?;
        let key = PrivateKeyDer::from_pem_slice(&key_bytes).context(ParsePemSnafu { path: &key_path })?;

        // Explicit ring provider (matches the tokio-zookeeper `tls` feature), so no process-wide
        // default CryptoProvider needs installing.
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .context(BuildTlsConfigSnafu)?
            .with_root_certificates(roots)
            .with_client_auth_cert(cert_chain, key)
            .context(BuildTlsConfigSnafu)?;

        Ok(Arc::new(config))
    }

    /// The ACL applied to a znode created by this controller.
    ///
    /// With a `platform_access_principal` (the agent's own x509 subject DN, read from its mounted
    /// client cert), the znode is locked to that principal: an unauthenticated session — e.g. a
    /// plaintext `zkCli.sh` connecting via port unification — carries no auth ids and so fails the
    /// ACL with `NoAuth`. This is what makes credentials actually gate access on ZooKeeper 3.9.x,
    /// where port unification cannot be removed (blocked upstream until 3.10).
    ///
    /// Without a principal (operator fallback, plaintext) the historical `world:anyone` ACL is kept
    /// so today's behaviour is unchanged.
    fn desired_acl(platform_access_principal: Option<&str>) -> Vec<Acl> {
        match platform_access_principal {
            Some(principal) => vec![Acl {
                perms: Permission::ALL,
                scheme: "x509".to_string(),
                id: principal.to_string(),
            }],
            None => vec![Acl {
                perms: Permission::ALL,
                scheme: "world".to_string(),
                id: "anyone".to_string(),
            }],
        }
    }

    #[tracing::instrument]
    /// Creates a znode, and ensure that any metadata (such as ACLs) match the desired state
    pub async fn ensure_znode_exists(
        addr: &str,
        path: &str,
        platform_access_principal: Option<&str>,
        cert_dir: Option<&Path>,
        server_ca_dir: Option<&Path>,
    ) -> Result<(), Error> {
        tracing::info!(znode = path, "Creating ZNode");
        let zk = connect(addr, cert_dir, server_ca_dir).await?;
        let create_res = zk
            .create(
                path,
                vec![],
                desired_acl(platform_access_principal),
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
    pub async fn ensure_znode_missing(
        addr: &str,
        path: &str,
        cert_dir: Option<&Path>,
        server_ca_dir: Option<&Path>,
    ) -> Result<(), Error> {
        tracing::info!(znode = path, "Deleting ZNode");
        let zk = connect(addr, cert_dir, server_ca_dir).await?;
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
