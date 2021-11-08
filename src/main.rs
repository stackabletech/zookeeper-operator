mod crd;
mod utils;
mod zk_controller;
mod znode_controller;

use crd::{ZookeeperCluster, ZookeeperZnode};
use futures::{compat::Future01CompatExt, StreamExt};
use stackable_operator::{
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
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
};
use structopt::StructOpt;

use crate::utils::Tokio01ExecutorExt;

#[derive(StructOpt)]
struct Opts {
    #[structopt(subcommand)]
    cmd: Cmd,
}

#[derive(StructOpt)]
enum Cmd {
    /// Print CRD objects
    Crd,
    Run,
}

fn erase_controller_result<K: Resource, E: std::error::Error + Send + Sync + 'static>(
    res: Result<(ObjectRef<K>, ReconcilerAction), E>,
) -> eyre::Result<(ObjectRef<DynamicObject>, ReconcilerAction)> {
    let (obj_ref, action) = res?;
    Ok((obj_ref.erase(), action))
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt().pretty().init();
    // tokio-zookeeper depends on Tokio 0.1
    let tokio01_runtime = tokio01::runtime::Runtime::new()?;

    let opts = Opts::from_args();
    match opts.cmd {
        Cmd::Crd => println!(
            "{}{}",
            serde_yaml::to_string(&ZookeeperCluster::crd())?,
            serde_yaml::to_string(&ZookeeperZnode::crd())?
        ),
        Cmd::Run => {
            let kube = kube::Client::try_default().await?;
            let zks = kube::Api::<ZookeeperCluster>::all(kube.clone());
            let znodes = kube::Api::<ZookeeperZnode>::all(kube.clone());
            let zk_controller = Controller::new(zks, ListParams::default())
                .owns(
                    kube::Api::<Service>::all(kube.clone()),
                    ListParams::default(),
                )
                .owns(
                    kube::Api::<StatefulSet>::all(kube.clone()),
                    ListParams::default(),
                )
                .run(
                    zk_controller::reconcile_zk,
                    zk_controller::error_policy,
                    Context::new(zk_controller::Ctx { kube: kube.clone() }),
                );
            let znode_controller = Controller::new(znodes, ListParams::default())
                .owns(
                    kube::Api::<ConfigMap>::all(kube.clone()),
                    ListParams::default(),
                )
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
                zk_controller.map(erase_controller_result),
                znode_controller.map(erase_controller_result),
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
