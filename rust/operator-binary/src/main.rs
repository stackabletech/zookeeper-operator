mod discovery;
mod utils;
mod zk_controller;
mod znode_controller;

use crate::utils::Tokio01ExecutorExt;
use clap::Parser;
use futures::{compat::Future01CompatExt, StreamExt, TryStreamExt};
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Endpoints, Service},
    },
    kube::{
        api::{DynamicObject, ListParams},
        runtime::{
            controller::{self, Context, ReconcilerAction},
            events::{Event, EventType, Recorder, Reporter},
            reflector::ObjectRef,
            Controller,
        },
        CustomResourceExt, Resource,
    },
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
    cmd: Command,
}

/// Erases the concrete types of the controller result, so that we can merge the streams of multiple controllers for different resources.
///
/// In particular, we convert `ObjectRef<K>` into `ObjectRef<DynamicObject>` (which carries `K`'s metadata at runtime instead), and
/// `E` into the trait object `anyhow::Error`.
fn erase_controller_result_type<K: Resource, E: std::error::Error + Send + Sync + 'static>(
    res: Result<(ObjectRef<K>, ReconcilerAction), E>,
) -> anyhow::Result<(ObjectRef<DynamicObject>, ReconcilerAction)> {
    let (obj_ref, action) = res?;
    Ok((obj_ref.erase(), action))
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
        Command::Run(ProductOperatorRun { product_config }) => {
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
                client.get_all_api::<ZookeeperCluster>(),
                ListParams::default(),
            );
            let zk_store = zk_controller_builder.store();
            let zk_controller = zk_controller_builder
                .owns(client.get_all_api::<Service>(), ListParams::default())
                .watches(
                    client.get_all_api::<Endpoints>(),
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
                .owns(client.get_all_api::<StatefulSet>(), ListParams::default())
                .owns(client.get_all_api::<ConfigMap>(), ListParams::default())
                .shutdown_on_signal()
                .run(
                    zk_controller::reconcile_zk,
                    zk_controller::error_policy,
                    Context::new(zk_controller::Ctx {
                        client: client.clone(),
                        product_config,
                    }),
                );
            let znode_controller_builder = Controller::new(
                client.get_all_api::<ZookeeperZnode>(),
                ListParams::default(),
            );
            let znode_store = znode_controller_builder.store();
            let znode_controller = znode_controller_builder
                .owns(client.get_all_api::<ConfigMap>(), ListParams::default())
                .watches(
                    client.get_all_api::<ZookeeperCluster>(),
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
                .or_else(|err| async {
                    if let controller::Error::ReconcilerFailed(err, znode) = &err {
                        let recorder = Recorder::new(
                            client.as_kube_client(),
                            Reporter {
                                controller: "zookeeperznode.zookeeper.stackable.tech".to_string(),
                                instance: None,
                            },
                            znode.clone().into(),
                        );
                        let _ = recorder
                            .publish(Event {
                                type_: EventType::Warning,
                                reason: "ReconcilerFailed".to_string(),
                                note: Some(err.to_string()),
                                action: "Reconcile".to_string(),
                                secondary: None,
                            })
                            .await;
                    }
                    Err(err)
                });

            futures::stream::select(
                zk_controller.map(erase_controller_result_type),
                znode_controller.map(erase_controller_result_type),
            )
            .for_each(|res| async {
                match res {
                    Ok((obj, _)) => tracing::info!(object = %obj, "Reconciled object"),
                    Err(err) => {
                        tracing::error!(
                            error = &*err as &dyn std::error::Error,
                            "Failed to reconcile object",
                        )
                    }
                }
            })
            .await;
        }
    }

    tokio01_runtime.shutdown_now().compat().await.unwrap();
    Ok(())
}
