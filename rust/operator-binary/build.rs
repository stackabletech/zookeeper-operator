use stackable_operator::crd::CustomResourceExt;
use stackable_zookeeper_crd::commands::{Restart, Start, Stop};
use stackable_zookeeper_crd::ZookeeperCluster;

fn main() -> Result<(), stackable_operator::error::Error> {
    built::write_built_file().expect("Failed to acquire build-time information");

    ZookeeperCluster::write_yaml_schema("../../deploy/crd/zookeepercluster.crd.yaml")?;
    Restart::write_yaml_schema("../../deploy/crd/restart.crd.yaml")?;
    Start::write_yaml_schema("../../deploy/crd/start.crd.yaml")?;
    Stop::write_yaml_schema("../../deploy/crd/stop.crd.yaml")?;

    Ok(())
}
