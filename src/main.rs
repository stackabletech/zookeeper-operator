mod controller;
mod crd;

use crd::ZookeeperCluster;
use futures::StreamExt;
use kube::{api::ListParams, CustomResourceExt};
use kube_runtime::{controller::Context, Controller};
use structopt::StructOpt;

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

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt().init();

    let opts = Opts::from_args();
    match opts.cmd {
        Cmd::Crd => println!("{}", serde_yaml::to_string(&ZookeeperCluster::crd())?),
        Cmd::Run => {
            let kube = kube::Client::try_default().await?;
            let zks = kube::Api::<ZookeeperCluster>::all(kube.clone());
            Controller::new(zks, ListParams::default())
                .run(
                    controller::reconcile_zk,
                    controller::error_policy,
                    Context::new(controller::Ctx { kube }),
                )
                .for_each(|res| async {
                    match res {
                        Ok((obj, _)) => tracing::info!(object = %obj, "Reconciled object"),
                        Err(err) => {
                            tracing::error!(
                                error = &err as &dyn std::error::Error,
                                "Failed to reconcile object",
                            )
                        }
                    }
                })
                .await;
        }
    }
    Ok(())
}
