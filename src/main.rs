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
    let opts = Opts::from_args();
    match opts.cmd {
        Cmd::Crd => println!("{}", serde_yaml::to_string(&ZookeeperCluster::crd())?),
        Cmd::Run => {
            let kube = kube::Client::try_default().await?;
            let zks = kube::Api::<ZookeeperCluster>::all(kube);
            Controller::new(zks, ListParams::default())
                .run(
                    controller::reconcile_zk,
                    controller::error_policy,
                    Context::new(controller::Ctx {}),
                )
                .for_each(|res| async {
                    if let Err(err) = res {
                        eprintln!("Error: {}", err)
                    }
                })
                .await;
        }
    }
    Ok(())
}
