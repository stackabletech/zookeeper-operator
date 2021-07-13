use stackable_operator::crd::CustomResourceExt;
use stackable_zookeeper_crd::ZookeeperCluster;

fn main() {
    let target_file = "deploy/crd/zookeepercluster.crd.yaml";
    match ZookeeperCluster::write_yaml_schema(target_file) {
        Ok(_) => println!("Wrote CRD to [{}]", target_file),
        Err(err) => println!("Could not write CRD to [{}]: {:?}", target_file, err),
    }
}
