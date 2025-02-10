use std::sync::Arc;

use clap::{crate_description, crate_version, Parser};
use crd::{
    v1alpha1::{ZookeeperCluster, ZookeeperZnode},
    APP_NAME, OPERATOR_NAME,
};
use futures::{pin_mut, StreamExt};
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Endpoints, Service},
    },
    kube::{
        core::DeserializeGuard,
        runtime::{
            events::{Recorder, Reporter},
            reflector::ObjectRef,
            watcher, Controller,
        },
        Resource,
    },
    logging::controller::report_controller_reconciled,
    CustomResourceExt,
};

use crate::{zk_controller::ZK_FULL_CONTROLLER_NAME, znode_controller::ZNODE_FULL_CONTROLLER_NAME};

mod command;
pub mod crd;
mod discovery;
mod operations;
mod product_logging;
mod utils;
mod zk_controller;
mod znode_controller;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
    pub const TARGET_PLATFORM: Option<&str> = option_env!("TARGET");
    pub const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
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
            ZookeeperCluster::print_yaml_schema(built_info::CARGO_PKG_VERSION)?;
            ZookeeperZnode::print_yaml_schema(built_info::CARGO_PKG_VERSION)?;
        }
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
            tracing_target,
            cluster_info_opts,
        }) => {
            stackable_operator::logging::initialize_logging(
                "ZOOKEEPER_OPERATOR_LOG",
                APP_NAME,
                tracing_target,
            );
            stackable_operator::utils::print_startup_string(
                crate_description!(),
                crate_version!(),
                built_info::GIT_VERSION,
                built_info::TARGET_PLATFORM.unwrap_or("unknown target"),
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/zookeeper-operator/config-spec/properties.yaml",
            ])?;
            let client = stackable_operator::client::initialize_operator(
                Some(OPERATOR_NAME.to_string()),
                &cluster_info_opts,
            )
            .await?;

            let zk_controller = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<ZookeeperCluster>>(&client),
                watcher::Config::default(),
            );

            let zk_event_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: ZK_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                },
            ));
            let zk_store = zk_controller.store();
            let zk_controller = zk_controller
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<Service>>(&client),
                    watcher::Config::default(),
                )
                .watches(
                    watch_namespace.get_api::<DeserializeGuard<Endpoints>>(&client),
                    watcher::Config::default(),
                    move |endpoints| {
                        zk_store
                            .state()
                            .into_iter()
                            .filter(move |zk| {
                                let Ok(zk) = &zk.0 else {
                                    return false;
                                };
                                let endpoints_meta = endpoints.meta();
                                zk.metadata.namespace == endpoints_meta.namespace
                                    && zk.server_role_service_name() == endpoints_meta.name
                            })
                            .map(|zk| ObjectRef::from_obj(&*zk))
                    },
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
                watch_namespace.get_api::<DeserializeGuard<ZookeeperZnode>>(&client),
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
                    watch_namespace.get_api::<DeserializeGuard<ZookeeperCluster>>(&client),
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
            futures::future::select(zk_controller, znode_controller).await;
        }
    }

    Ok(())
}
