use stackable_operator::crd::CustomResourceExt;
use stackable_zookeeper_crd::ZookeeperCluster;

fn main() {
    let target_file = "deploy/crd/zookeepercluster.crd.yaml";
    ZookeeperCluster::write_yaml_schema(target_file).unwrap();
    println!("Wrote CRD to [{}]", target_file);
}
