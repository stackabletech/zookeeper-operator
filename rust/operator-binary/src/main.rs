mod command;
mod discovery;
mod utils;
mod zk_controller;
mod znode_controller;

use crate::utils::Tokio01ExecutorExt;
use crate::zk_controller::ZK_CONTROLLER_NAME;
use crate::znode_controller::ZNODE_CONTROLLER_NAME;

use clap::Parser;
use futures::{compat::Future01CompatExt, StreamExt};
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Endpoints, Service},
    },
    kube::{
        api::ListParams,
        runtime::{reflector::ObjectRef, Controller},
    },
    logging::controller::report_controller_reconciled,
    CustomResourceExt,
};
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperZnode};
use std::sync::Arc;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

const APP_NAME: &str = "zookeeper";
const OPERATOR_NAME: &str = "zookeeper.stackable.tech";

#[derive(clap::Parser)]
#[clap(about = built_info::PKG_DESCRIPTION, author = stackable_operator::cli::AUTHOR)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // tokio-zookeeper depends on Tokio 0.1
    let tokio01_runtime = tokio01::runtime::Runtime::new()?;

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
                built_info::PKG_DESCRIPTION,
                built_info::PKG_VERSION,
                built_info::GIT_VERSION,
                built_info::TARGET,
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
                ListParams::default(),
            );

            let zk_store = zk_controller_builder.store();
            let zk_controller = zk_controller_builder
                .owns(
                    watch_namespace.get_api::<Service>(&client),
                    ListParams::default(),
                )
                .watches(
                    watch_namespace.get_api::<Endpoints>(&client),
                    ListParams::default(),
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
                    ListParams::default(),
                )
                .owns(
                    watch_namespace.get_api::<ConfigMap>(&client),
                    ListParams::default(),
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
                ListParams::default(),
            );
            let znode_store = znode_controller_builder.store();
            let znode_controller = znode_controller_builder
                .owns(
                    watch_namespace.get_api::<ConfigMap>(&client),
                    ListParams::default(),
                )
                .watches(
                    watch_namespace.get_api::<ZookeeperCluster>(&client),
                    ListParams::default(),
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
                    |znode, ctx| {
                        tokio01_runtime
                            .executor()
                            .run_in_ctx(znode_controller::reconcile_znode(znode, ctx))
                    },
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

    tokio01_runtime.shutdown_now().compat().await.unwrap();
    Ok(())
}
