// TODO: Look into how to properly resolve `clippy::result_large_err`.
// This will need changes in our and upstream error types.
#![allow(clippy::result_large_err)]
use std::{path::PathBuf, sync::Arc};

use anyhow::anyhow;
use clap::Parser;
use crd::{
    APP_NAME, OPERATOR_NAME, ZookeeperCluster, ZookeeperClusterVersion, ZookeeperZnode,
    ZookeeperZnodeVersion, v1alpha1,
};
use futures::{FutureExt, StreamExt, TryFutureExt};
use stackable_operator::{
    YamlSchema,
    cli::{CommonOptions, RunArguments},
    eos::EndOfSupportChecker,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    kube::{
        CustomResourceExt as _,
        core::DeserializeGuard,
        runtime::{
            Controller,
            events::{Recorder, Reporter},
            reflector::ObjectRef,
            watcher,
        },
    },
    logging::controller::report_controller_reconciled,
    namespace::WatchNamespace,
    shared::yaml::SerializeOptions,
    telemetry::Tracing,
    utils::signal::{self, SignalWatcher},
    v2::types::common::Port,
};

use crate::{
    webhooks::conversion::create_webhook_server, zk_controller::ZK_FULL_CONTROLLER_NAME,
    znode_controller::Mode,
};

pub mod crd;
mod webhooks;
mod zk_controller;
mod znode_controller;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(clap::Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

/// The operator's own subcommands.
///
/// This replaces `stackable_operator::cli::Command` so a per-cluster `agent` mode can be added
/// alongside the framework's `Crd`/`Run` behaviours (both of which are handled inline below, as they
/// already were).
#[derive(clap::Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Print the CustomResourceDefinitions.
    Crd,
    /// Run the operator: both controllers plus the CRD conversion webhook.
    Run(ZookeeperRunArguments),
    /// Run as a per-cluster `ZookeeperZnode` agent (spike). Owns znode provisioning for a single
    /// `ZookeeperCluster`, authenticating with a mounted platform-access credential.
    Agent(AgentArguments),
}

/// The operator's run arguments: the framework's [`RunArguments`] plus the operator image.
#[derive(clap::Args)]
struct ZookeeperRunArguments {
    #[clap(flatten)]
    common_run: RunArguments,

    /// The operator's own container image. Propagated to per-cluster agent Deployments so they run
    /// the same binary in `agent` mode. Already set from the `internal.stackable.tech/image` pod
    /// annotation via `OPERATOR_IMAGE` in the Helm chart.
    #[arg(long, env)]
    operator_image: Option<String>,
}

/// Arguments for `agent` mode.
///
/// Deliberately *not* [`RunArguments`]: that drags in the mandatory `operator_namespace` /
/// `operator_service_name`, which exist only for the conversion webhook the agent does not run.
#[derive(clap::Args)]
struct AgentArguments {
    /// Name of the `ZookeeperCluster` this agent provisions znodes for.
    #[arg(long, env)]
    zookeeper_cluster_name: String,

    /// Namespace of the `ZookeeperCluster` (and the only namespace this agent watches).
    #[arg(long, env)]
    zookeeper_cluster_namespace: String,

    /// The ZooKeeper client port to connect to.
    #[arg(long, env)]
    zookeeper_client_port: u16,

    /// The product image repository, used to resolve the cluster image for the discovery ConfigMap
    /// version label.
    #[arg(long, env)]
    image_repository: String,

    /// Directory the platform-access client credential (`tls.crt` / `tls.key`) is mounted at.
    #[arg(long, env)]
    platform_access_cert_dir: Option<PathBuf>,

    /// Directory the ZooKeeper *server's* CA (`ca.crt`) is mounted at, from the cluster's
    /// `serverSecretClass`. The agent verifies the server certificate against this CA, which may
    /// differ from the credential's own CA (cross-CA mTLS). When unset, the credential's `ca.crt`
    /// (in `--platform-access-cert-dir`) is used instead (the single-CA case).
    #[arg(long, env)]
    platform_access_server_ca_dir: Option<PathBuf>,

    /// The x509 subject DN (RFC 2253) ZooKeeper derives from the mounted client certificate, used
    /// for per-znode ACLs.
    ///
    /// Spike note: ideally this is read from the mounted certificate at startup, but the exact DN
    /// string ZooKeeper produces is precisely what the step-0 experiment pins down (and reading it
    /// needs an X.509 parser this build does not yet vendor). Until then it is passed explicitly, so
    /// the demo is deterministic. Log it once; a mismatch here is the most confusing possible
    /// failure.
    #[arg(long, env)]
    platform_access_principal: Option<String>,

    #[clap(flatten)]
    common: CommonOptions,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => {
            ZookeeperCluster::merged_crd(ZookeeperClusterVersion::V1Alpha1)?
                .print_yaml_schema(built_info::PKG_VERSION, &SerializeOptions::default())?;
            ZookeeperZnode::merged_crd(ZookeeperZnodeVersion::V1Alpha1)?
                .print_yaml_schema(built_info::PKG_VERSION, &SerializeOptions::default())?;
        }
        Command::Run(ZookeeperRunArguments {
            common_run:
                RunArguments {
                    operator_environment,
                    watch_namespace,
                    maintenance,
                    common,
                },
            operator_image,
        }) => {
            // NOTE (@NickLarsenNZ): Before stackable-telemetry was used:
            // - The console log level was set by `ZOOKEEPER_OPERATOR_LOG`, and is now `CONSOLE_LOG` (when using Tracing::pre_configured).
            // - The file log level was set by `ZOOKEEPER_OPERATOR_LOG`, and is now set via `FILE_LOG` (when using Tracing::pre_configured).
            // - The file log directory was set by `ZOOKEEPER_OPERATOR_LOG_DIRECTORY`, and is now set by `ROLLING_LOGS_DIR` (or via `--rolling-logs <DIRECTORY>`).
            let _tracing_guard =
                Tracing::pre_configured(built_info::PKG_NAME, common.telemetry).init()?;

            tracing::info!(
                built_info.pkg_version = built_info::PKG_VERSION,
                built_info.git_version = built_info::GIT_VERSION,
                built_info.target = built_info::TARGET,
                built_info.built_time_utc = built_info::BUILT_TIME_UTC,
                built_info.rustc_version = built_info::RUSTC_VERSION,
                "Starting {description}",
                description = built_info::PKG_DESCRIPTION
            );

            // Watches for the SIGTERM signal and sends a signal to all receivers, which gracefully
            // shuts down all concurrent tasks below (EoS checker, controller).
            let sigterm_watcher = SignalWatcher::sigterm()?;

            let eos_checker =
                EndOfSupportChecker::new(built_info::BUILT_TIME_UTC, &maintenance.end_of_support)?
                    .run(sigterm_watcher.handle())
                    .map(anyhow::Ok);

            let client = stackable_operator::client::initialize_operator(
                Some(OPERATOR_NAME.to_string()),
                &common.cluster_info,
            )
            .await?;

            let webhook_server = create_webhook_server(
                &operator_environment,
                maintenance.disable_crd_maintenance,
                client.as_kube_client(),
            )
            .await?;

            let webhook_server = webhook_server
                .run(sigterm_watcher.handle())
                .map_err(|err| anyhow!(err).context("failed to run webhook server"));

            let zk_controller = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<v1alpha1::ZookeeperCluster>>(&client),
                watcher::Config::default(),
            );

            let zk_event_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: ZK_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                },
            ));
            let zk_controller = zk_controller
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<Service>>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<StatefulSet>>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
                    watcher::Config::default(),
                )
                .graceful_shutdown_on(sigterm_watcher.handle())
                .run(
                    zk_controller::reconcile_zk,
                    zk_controller::error_policy,
                    Arc::new(zk_controller::Ctx {
                        operator_environment: operator_environment.clone(),
                        client: client.clone(),
                        operator_image,
                    }),
                )
                // We can let the reporting happen in the background
                .for_each_concurrent(
                    16, // concurrency limit
                    |result| {
                        // The event_recorder needs to be shared across all invocations, so that
                        // events are correctly aggregated
                        let event_recorder = zk_event_recorder.clone();
                        async move {
                            report_controller_reconciled(
                                &event_recorder,
                                ZK_FULL_CONTROLLER_NAME,
                                &result,
                            )
                            .await;
                        }
                    },
                )
                .map(anyhow::Ok);

            // The ZookeeperZnode controller runs as the operator fallback (catch-all): it claims
            // every znode that no per-cluster agent owns. See `znode_controller::Mode`.
            let znode_ctx = Arc::new(znode_controller::Ctx {
                client: client.clone(),
                image_repository: operator_environment.image_repository.clone(),
                mode: Mode::OperatorFallback,
                platform_access_principal: None,
                platform_access_cert_dir: None,
                platform_access_server_ca_dir: None,
            });
            let znode_controller = znode_controller::run::run(
                client.clone(),
                &watch_namespace,
                znode_ctx,
                sigterm_watcher.handle(),
            );

            let delayed_zk_controller = async {
                signal::crd_established(&client, v1alpha1::ZookeeperCluster::crd_name(), None)
                    .await?;
                zk_controller.await
            };

            let delayed_znode_controller = async {
                signal::crd_established(&client, v1alpha1::ZookeeperZnode::crd_name(), None).await?;
                znode_controller.await;
                anyhow::Ok(())
            };

            // kube-runtime's Controller will tokio::spawn each reconciliation, so this only concerns the internal watch machinery
            futures::try_join!(
                delayed_zk_controller,
                delayed_znode_controller,
                eos_checker,
                webhook_server
            )?;
        }
        Command::Agent(AgentArguments {
            zookeeper_cluster_name,
            zookeeper_cluster_namespace,
            zookeeper_client_port,
            image_repository,
            platform_access_cert_dir,
            platform_access_server_ca_dir,
            platform_access_principal,
            common,
        }) => {
            let _tracing_guard =
                Tracing::pre_configured(built_info::PKG_NAME, common.telemetry).init()?;

            tracing::info!(
                built_info.pkg_version = built_info::PKG_VERSION,
                zookeeper.cluster.name = zookeeper_cluster_name,
                zookeeper.cluster.namespace = zookeeper_cluster_namespace,
                "Starting ZookeeperZnode agent"
            );
            if let Some(dir) = &platform_access_cert_dir {
                tracing::info!(?dir, "Platform-access credential directory");
            }
            match &platform_access_server_ca_dir {
                Some(dir) => tracing::info!(
                    ?dir,
                    "Platform-access server CA directory; server certs are verified against it \
                     (cross-CA mTLS)"
                ),
                None => tracing::debug!(
                    "No --platform-access-server-ca-dir set; the credential's own ca.crt is used to \
                     verify the server (single-CA mode)"
                ),
            }
            match &platform_access_principal {
                Some(principal) => tracing::info!(
                    %principal,
                    "Platform-access x509 principal; agent-created znodes are ACL'd to it"
                ),
                None => tracing::warn!(
                    "No --platform-access-principal set; agent-created znodes fall back to \
                     world:anyone (no access control)"
                ),
            }

            let sigterm_watcher = SignalWatcher::sigterm()?;

            let client = stackable_operator::client::initialize_operator(
                Some(OPERATOR_NAME.to_string()),
                &common.cluster_info,
            )
            .await?;

            // Renew the agent's liveness Lease alongside the controller, so the operator can
            // surface "agent not running" on the ZookeeperZnodes (spike step 4c).
            let holder = std::env::var("HOSTNAME")
                .unwrap_or_else(|_| format!("{zookeeper_cluster_name}-znode-agent"));
            let lease_task = znode_controller::lease::renew_forever(
                client.clone(),
                zookeeper_cluster_namespace.clone(),
                zookeeper_cluster_name.clone(),
                holder,
                sigterm_watcher.handle(),
            );

            let ctx = Arc::new(znode_controller::Ctx {
                client: client.clone(),
                image_repository,
                mode: Mode::Agent {
                    cluster_name: zookeeper_cluster_name,
                    namespace: zookeeper_cluster_namespace.clone(),
                    client_port: Port(zookeeper_client_port),
                },
                platform_access_principal,
                platform_access_cert_dir,
                platform_access_server_ca_dir,
            });

            // The agent watches only its own cluster's namespace.
            let watch_namespace = WatchNamespace::One(zookeeper_cluster_namespace);

            // NB: the agent must NOT call `signal::crd_established` (needs cluster-scoped CRD
            // list/watch and hard-fails after 5s) and must NOT create a conversion webhook
            // (`ConversionWebhook` owns CRD creation). It simply runs the znode controller (plus the
            // liveness lease renewal).
            let controller = znode_controller::run::run(
                client,
                &watch_namespace,
                ctx,
                sigterm_watcher.handle(),
            );
            futures::join!(lease_task, controller);
        }
    }

    Ok(())
}
