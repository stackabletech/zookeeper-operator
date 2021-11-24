use stackable_operator::CustomResourceExt;
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperZnode};

fn main() {
    built::write_built_file().expect("Failed to acquire build-time information");

    ZookeeperCluster::write_yaml_schema("../../deploy/crd/zookeepercluster.crd.yaml").unwrap();
    ZookeeperZnode::write_yaml_schema("../../deploy/crd/zookeeperznode.crd.yaml").unwrap();
}
