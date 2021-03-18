use serde_yaml;
use stackable_zookeeper_crd::ZooKeeperCluster;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("{}", serde_yaml::to_string(&ZooKeeperCluster::crd())?);
    Ok(())
}
