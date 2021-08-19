use stackable_operator::crd::CustomResourceExt;
use stackable_zookeeper_crd::ZookeeperCluster;

fn main() -> Result<(), stackable_operator::error::Error> {
    built::write_built_file().expect("Failed to acquire build-time information");

    ZookeeperCluster::write_yaml_schema("../deploy/crd/zookeepercluster.crd.yaml")?;

    Ok(())
}
