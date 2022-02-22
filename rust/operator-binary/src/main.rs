mod discovery;
mod utils;
mod zk_controller;
mod znode_controller;

use crate::utils::Tokio01ExecutorExt;
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
        runtime::{controller::Context, reflector::ObjectRef, Controller},
        CustomResourceExt,
    },
    logging::controller::report_controller_reconciled,
};
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperZnode};

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub const APP_NAME: &str = "zookeeper";
pub const APP_PORT: u16 = 2181;

#[derive(clap::Parser)]
#[clap(about = built_info::PKG_DESCRIPTION, author = stackable_operator::cli::AUTHOR)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command<ZookeeperRun>,
}

#[derive(clap::Parser)]
struct ZookeeperRun {
    #[clap(long, env)]
    watch_namespace: Option<String>,
    #[clap(flatten)]
    common: ProductOperatorRun,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    stackable_operator::logging::initialize_logging("ZOOKEEPER_OPERATOR_LOG");
    // tokio-zookeeper depends on Tokio 0.1
    let tokio01_runtime = tokio01::runtime::Runtime::new()?;

    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => println!(
            "{}{}",
            serde_yaml::to_string(&ZookeeperCluster::crd())?,
            serde_yaml::to_string(&ZookeeperZnode::crd())?
        ),
        Command::Run(ZookeeperRun {
            watch_namespace,
            common: ProductOperatorRun { product_config },
        }) => {
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
            let client = stackable_operator::client::create_client(Some(
                "zookeeper.stackable.tech".to_string(),
            ))
            .await?;
            let zk_controller_builder = Controller::new(
                client.get_api::<ZookeeperCluster>(watch_namespace.as_deref()),
                ListParams::default(),
            );

            let zk_store = zk_controller_builder.store();
            let zk_controller = zk_controller_builder
                .owns(
                    client.get_api::<Service>(watch_namespace.as_deref()),
                    ListParams::default(),
                )
                .watches(
                    client.get_api::<Endpoints>(watch_namespace.as_deref()),
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
                    client.get_api::<StatefulSet>(watch_namespace.as_deref()),
                    ListParams::default(),
                )
                .owns(
                    client.get_api::<ConfigMap>(watch_namespace.as_deref()),
                    ListParams::default(),
                )
                .shutdown_on_signal()
                .run(
                    zk_controller::reconcile_zk,
                    zk_controller::error_policy,
                    Context::new(zk_controller::Ctx {
                        client: client.clone(),
                        product_config,
                    }),
                )
                .map(|res| {
                    report_controller_reconciled(
                        &client,
                        "zookeeperclusters.zookeeper.stackable.tech",
                        &res,
                    );
                });
            let znode_controller_builder = Controller::new(
                client.get_api::<ZookeeperZnode>(watch_namespace.as_deref()),
                ListParams::default(),
            );
            let znode_store = znode_controller_builder.store();
            let znode_controller = znode_controller_builder
                .owns(
                    client.get_api::<ConfigMap>(watch_namespace.as_deref()),
                    ListParams::default(),
                )
                .watches(
                    client.get_api::<ZookeeperCluster>(watch_namespace.as_deref()),
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
                    Context::new(znode_controller::Ctx {
                        client: client.clone(),
                    }),
                )
                .map(|res| {
                    report_controller_reconciled(
                        &client,
                        "zookeeperznode.zookeeper.stackable.tech",
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
