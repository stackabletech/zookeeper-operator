use stackable_operator::CustomResourceExt;
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperZnode};

fn main() -> eyre::Result<()> {
    built::write_built_file().expect("Failed to acquire build-time information");

    ZookeeperCluster::write_yaml_schema("../../deploy/crd/zookeepercluster.crd.yaml")?;
    ZookeeperZnode::write_yaml_schema("../../deploy/crd/zookeeperznode.crd.yaml")?;

    Ok(())
}
