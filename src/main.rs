mod crd;

use crd::ZookeeperCluster;
use kube::CustomResourceExt;
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
}

fn main() -> eyre::Result<()> {
    let opts = Opts::from_args();
    match opts.cmd {
        Cmd::Crd => println!("{}", serde_yaml::to_string(&ZookeeperCluster::crd())?),
    }
    Ok(())
}
