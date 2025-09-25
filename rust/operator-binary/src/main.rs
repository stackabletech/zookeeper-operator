// TODO: Look into how to properly resolve `clippy::result_large_err`.
// This will need changes in our and upstream error types.
#![allow(clippy::result_large_err)]
use std::sync::Arc;

use clap::Parser;
use crd::{
    APP_NAME, OPERATOR_NAME, ZookeeperCluster, ZookeeperClusterVersion, ZookeeperZnode,
    ZookeeperZnodeVersion, v1alpha1,
};
use futures::{StreamExt, pin_mut};
use stackable_operator::{
    YamlSchema,
    cli::{Command, RunArguments},
    eos::EndOfSupportChecker,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    kube::{
        Resource,
        core::DeserializeGuard,
        runtime::{
            Controller,
            events::{Recorder, Reporter},
            reflector::ObjectRef,
            watcher,
        },
    },
    logging::controller::report_controller_reconciled,
    shared::yaml::SerializeOptions,
    telemetry::Tracing,
};

use crate::{zk_controller::ZK_FULL_CONTROLLER_NAME, znode_controller::ZNODE_FULL_CONTROLLER_NAME};

mod command;
mod config;
pub mod crd;
mod discovery;
mod listener;
mod operations;
mod product_logging;
mod utils;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => {
            ZookeeperCluster::merged_crd(ZookeeperClusterVersion::V1Alpha1)?
                .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?;
            ZookeeperZnode::merged_crd(ZookeeperZnodeVersion::V1Alpha1)?
                .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?;
        }
        Command::Run(RunArguments {
            product_config,
            watch_namespace,
            operator_environment: _,
            maintenance,
            common,
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

            let eos_checker =
                EndOfSupportChecker::new(built_info::BUILT_TIME_UTC, maintenance.end_of_support)?
                    .run();

            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/zookeeper-operator/config-spec/properties.yaml",
            ])?;

            let client = stackable_operator::client::initialize_operator(
                Some(OPERATOR_NAME.to_string()),
                &common.cluster_info,
            )
            .await?;

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
                .shutdown_on_signal()
                .run(
                    zk_controller::reconcile_zk,
                    zk_controller::error_policy,
                    Arc::new(zk_controller::Ctx {
                        client: client.clone(),
                        product_config,
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
                );

            let znode_controller = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<v1alpha1::ZookeeperZnode>>(&client),
                watcher::Config::default(),
            );
            let znode_event_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: ZNODE_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                },
            ));

            let znode_store = znode_controller.store();
            let znode_controller = znode_controller
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
                    watcher::Config::default(),
                )
                .watches(
                    watch_namespace
                        .get_api::<DeserializeGuard<v1alpha1::ZookeeperCluster>>(&client),
                    watcher::Config::default(),
                    move |zk| {
                        znode_store
                            .state()
                            .into_iter()
                            .filter(move |znode| {
                                let Ok(znode) = &znode.0 else {
                                    return false;
                                };
                                let zk_meta = zk.meta();
                                zk_meta.namespace == znode.spec.cluster_ref.namespace
                                    && zk_meta.name == znode.spec.cluster_ref.name
                            })
                            .map(|znode| ObjectRef::from_obj(&*znode))
                    },
                )
                .shutdown_on_signal()
                .run(
                    znode_controller::reconcile_znode,
                    znode_controller::error_policy,
                    Arc::new(znode_controller::Ctx {
                        client: client.clone(),
                    }),
                )
                // We can let the reporting happen in the background
                .for_each_concurrent(
                    16, // concurrency limit
                    move |result| {
                        // The event_recorder needs to be shared across all invocations, so that
                        // events are correctly aggregated
                        let event_recorder = znode_event_recorder.clone();
                        async move {
                            report_controller_reconciled(
                                &event_recorder,
                                ZNODE_FULL_CONTROLLER_NAME,
                                &result,
                            )
                            .await;
                        }
                    },
                );

            pin_mut!(zk_controller, znode_controller);
            // kube-runtime's Controller will tokio::spawn each reconciliation, so this only concerns the internal watch machinery
            let controller_futures = futures::future::select(zk_controller, znode_controller);
            tokio::join!(controller_futures, eos_checker);
        }
    }

    Ok(())
}
