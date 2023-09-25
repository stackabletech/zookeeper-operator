mod command;
mod discovery;
mod operations;
mod product_logging;
mod utils;
mod zk_controller;
mod znode_controller;

use crate::zk_controller::ZK_CONTROLLER_NAME;
use crate::znode_controller::ZNODE_CONTROLLER_NAME;

use clap::{crate_description, crate_version, Parser};
use futures::StreamExt;
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Endpoints, Service},
    },
    kube::runtime::{reflector::ObjectRef, watcher, Controller},
    logging::controller::report_controller_reconciled,
    CustomResourceExt,
};
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperZnode, APP_NAME, OPERATOR_NAME};
use std::sync::Arc;

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
            ZookeeperCluster::print_yaml_schema()?;
            ZookeeperZnode::print_yaml_schema()?;
        }
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
            tracing_target,
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
            let client =
                stackable_operator::client::create_client(Some(OPERATOR_NAME.to_string())).await?;
            let zk_controller_builder = Controller::new(
                watch_namespace.get_api::<ZookeeperCluster>(&client),
                watcher::Config::default(),
            );

            let zk_store = zk_controller_builder.store();
            let zk_controller = zk_controller_builder
                .owns(
                    watch_namespace.get_api::<Service>(&client),
                    watcher::Config::default(),
                )
                .watches(
                    watch_namespace.get_api::<Endpoints>(&client),
                    watcher::Config::default(),
                    move |endpoints| {
                        zk_store
                            .state()
                            .into_iter()
                            .filter(move |zk| {
                                zk.metadata.namespace == endpoints.metadata.namespace
                                    && zk.server_role_service_name() == endpoints.metadata.name
                            })
                            .map(|zk| ObjectRef::from_obj(&*zk))
                    },
                )
                .owns(
                    watch_namespace.get_api::<StatefulSet>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<ConfigMap>(&client),
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
                .map(|res| {
                    report_controller_reconciled(
                        &client,
                        &format!("{ZK_CONTROLLER_NAME}.{OPERATOR_NAME}"),
                        &res,
                    );
                });
            let znode_controller_builder = Controller::new(
                watch_namespace.get_api::<ZookeeperZnode>(&client),
                watcher::Config::default(),
            );
            let znode_store = znode_controller_builder.store();
            let znode_controller = znode_controller_builder
                .owns(
                    watch_namespace.get_api::<ConfigMap>(&client),
                    watcher::Config::default(),
                )
                .watches(
                    watch_namespace.get_api::<ZookeeperCluster>(&client),
                    watcher::Config::default(),
                    move |zk| {
                        znode_store
                            .state()
                            .into_iter()
                            .filter(move |znode| {
                                zk.metadata.namespace == znode.spec.cluster_ref.namespace
                                    && zk.metadata.name == znode.spec.cluster_ref.name
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
                .map(|res| {
                    report_controller_reconciled(
                        &client,
                        &format!("{ZNODE_CONTROLLER_NAME}.{OPERATOR_NAME}"),
                        &res,
                    );
                });

            futures::stream::select(zk_controller, znode_controller)
                .collect::<()>()
                .await;
        }
    }

    Ok(())
}
