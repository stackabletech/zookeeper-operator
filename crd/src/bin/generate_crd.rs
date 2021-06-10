use stackable_zookeeper_crd::ZookeeperCluster;
use std::fs;

fn main() {
    let target_file = "deploy/crd/zookeepercluster.crd.yaml";
    let schema = ZookeeperCluster::crd();
    let string_schema = match serde_yaml::to_string(&schema) {
        Ok(schema) => schema,
        Err(err) => panic!("Failed to retrieve CRD: [{}]", err),
    };
    match fs::write(target_file, string_schema) {
        Ok(()) => println!("Successfully wrote CRD to file."),
        Err(err) => println!("Failed to write file: [{}]", err),
    }
}
