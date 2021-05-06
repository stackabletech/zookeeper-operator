use stackable_zookeeper_crd::ZookeeperCluster;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("{}", serde_yaml::to_string(&ZookeeperCluster::crd())?);
    Ok(())
}
