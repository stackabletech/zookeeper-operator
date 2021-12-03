mod discovery;
mod utils;
mod zk_controller;
mod znode_controller;

use std::str::FromStr;

use crate::utils::Tokio01ExecutorExt;
use futures::{compat::Future01CompatExt, StreamExt};
use stackable_operator::{
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Endpoints, Service},
    },
    kube::{
        self,
        api::{DynamicObject, ListParams},
        runtime::{
            controller::{Context, ReconcilerAction},
            reflector::ObjectRef,
            Controller,
        },
        CustomResourceExt, Resource,
    },
    product_config::ProductConfigManager,
};
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperZnode};
use structopt::StructOpt;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub const APP_NAME: &str = "zookeeper";
pub const APP_PORT: u16 = 2181;

#[derive(StructOpt)]
#[structopt(about = built_info::PKG_DESCRIPTION, author = "Stackable GmbH - info@stackable.de")]
struct Opts {
    #[structopt(subcommand)]
    cmd: Cmd,
}

#[derive(StructOpt)]
enum Cmd {
    /// Print CRD objects
    Crd,
    /// Run operator
    Run {
        /// Provides the path to a product-config file
        #[structopt(long, short = "p", value_name = "FILE")]
        product_config: Option<String>,
    },
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

    let opts = Opts::from_args();
    match opts.cmd {
        Cmd::Crd => println!(
            "{}{}",
            serde_yaml::to_string(&ZookeeperCluster::crd())?,
            serde_yaml::to_string(&ZookeeperZnode::crd())?
        ),
        Cmd::Run { product_config } => {
            stackable_operator::utils::print_startup_string(
                built_info::PKG_DESCRIPTION,
                built_info::PKG_VERSION,
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = if let Some(product_config_path) = product_config {
                ProductConfigManager::from_yaml_file(&product_config_path)?
            } else {
                ProductConfigManager::from_str(include_str!(
                    "../../../deploy/config-spec/properties.yaml"
                ))?
            };
            let kube = kube::Client::try_default().await?;
            let zks = kube::Api::<ZookeeperCluster>::all(kube.clone());
            let znodes = kube::Api::<ZookeeperZnode>::all(kube.clone());
            let zk_controller_builder = Controller::new(zks.clone(), ListParams::default());
            let zk_store = zk_controller_builder.store();
            let zk_controller = zk_controller_builder
                .owns(
                    kube::Api::<Service>::all(kube.clone()),
                    ListParams::default(),
                )
                .watches(
                    kube::Api::<Endpoints>::all(kube.clone()),
                    ListParams::default(),
                    move |endpoints| {
                        zk_store
                            .state()
                            .into_iter()
                            .filter(move |zk| {
                                zk.metadata.namespace == endpoints.metadata.namespace
                                    && zk.server_role_service_name() == endpoints.metadata.name
                            })
                            .map(|zk| ObjectRef::from_obj(&zk))
                    },
                )
                .owns(
                    kube::Api::<StatefulSet>::all(kube.clone()),
                    ListParams::default(),
                )
                .owns(
                    kube::Api::<ConfigMap>::all(kube.clone()),
                    ListParams::default(),
                )
                .run(
                    zk_controller::reconcile_zk,
                    zk_controller::error_policy,
                    Context::new(zk_controller::Ctx {
                        kube: kube.clone(),
                        product_config,
                    }),
                );
            let znode_controller_builder = Controller::new(znodes, ListParams::default());
            let znode_store = znode_controller_builder.store();
            let znode_controller = znode_controller_builder
                .owns(
                    kube::Api::<ConfigMap>::all(kube.clone()),
                    ListParams::default(),
                )
                .watches(zks, ListParams::default(), move |zk| {
                    znode_store
                        .state()
                        .into_iter()
                        .filter(move |znode| {
                            zk.metadata.namespace == znode.spec.cluster_ref.namespace
                                && zk.metadata.name == znode.spec.cluster_ref.name
                        })
                        .map(|znode| ObjectRef::from_obj(&znode))
                })
                .run(
                    |znode, ctx| {
                        tokio01_runtime
                            .executor()
                            .run_in_ctx(znode_controller::reconcile_znode(znode, ctx))
                    },
                    znode_controller::error_policy,
                    Context::new(znode_controller::Ctx { kube }),
                );
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
