use serde::de::DeserializeOwned;
use stackable_operator::commons::authentication::AuthenticationClass;
use stackable_operator::k8s_openapi::api::core::v1::ConfigMap;
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperZnode};
use std::fs;
use std::fs::File;

fn read<T: DeserializeOwned>(file_path: &str) -> T {
    let file = File::open(file_path).expect("Could not open file");
    serde_yaml::from_reader(file).expect("Could not read file content")
}

// Required to deserialize the complex enum.
// Otherwise error: "invalid type: map, expected a YAML tag starting with '!'"
fn read_auth_class(file_path: &str) -> AuthenticationClass {
    let file_content = fs::read_to_string(file_path).expect("Could not open file");
    let deserializer = serde_yaml::Deserializer::from_str(&file_content);

    serde_yaml::with::singleton_map_recursive::deserialize(deserializer)
        .expect("Could not read file content")
}

const USAGE_GUIDE_EXAMPLE_PATH: &str = "../../docs/modules/zookeeper/examples/usage_guide";

#[test]
fn test_doc_usage_guide_examples() {
    read::<ZookeeperCluster>(&format!(
        "{USAGE_GUIDE_EXAMPLE_PATH}/example-cluster-tls-authentication.yaml"
    ));

    read_auth_class(&format!(
        "{USAGE_GUIDE_EXAMPLE_PATH}/example-cluster-tls-authentication-class.yaml"
    ));

    // TODO: `SecretClass` currently resides in Secret Operator and we have no access here.
    // read::<SecretClass>(&format!(
    //     "{USAGE_GUIDE_EXAMPLE_PATH}/example-cluster-tls-authentication-secret.yaml"
    // ));

    read::<ZookeeperCluster>(&format!(
        "{USAGE_GUIDE_EXAMPLE_PATH}/example-cluster-tls-encryption.yaml"
    ));

    // TODO: `SecretClass` currently resides in Secret Operator and we have no access here.
    // read::<SecretCluster>(&format!(
    //    "{USAGE_GUIDE_EXAMPLE_PATH}/example-secret-operator-tls-secret.yaml"
    // ));
}

const USAGE_GUIDE_EXAMPLE_ZNODE_PATH: &str =
    "../../docs/modules/zookeeper/examples/usage_guide/znode";

#[test]
fn test_doc_usage_guide_znode_examples() {
    read::<ConfigMap>(&format!(
        "{USAGE_GUIDE_EXAMPLE_ZNODE_PATH}/example-znode-discovery.yaml"
    ));

    read::<ZookeeperZnode>(&format!(
        "{USAGE_GUIDE_EXAMPLE_ZNODE_PATH}/example-znode-druid.yaml"
    ));

    read::<ZookeeperZnode>(&format!(
        "{USAGE_GUIDE_EXAMPLE_ZNODE_PATH}/example-znode-kafka.yaml"
    ));
}

const EXAMPLE_TLS_PATH: &str = "../../examples/tls";

#[test]
fn test_example_tls_cluster() {
    read::<ZookeeperCluster>(&format!(
        "{EXAMPLE_TLS_PATH}/simple-zookeeper-tls-cluster.yaml"
    ));

    read::<ZookeeperZnode>(&format!(
        "{EXAMPLE_TLS_PATH}/simple-zookeeper-tls-znode.yaml"
    ));

    read_auth_class(&format!(
        "{EXAMPLE_TLS_PATH}/simple-zookeeper-tls-auth-class.yaml"
    ));

    // TODO: `SecretClass` currently resides in Secret Operator and we have no access here.
    // read::<SecretClass>(&format!(
    //    "{EXAMPLE_TLS_PATH}/simple-zookeeper-tls-secret-class.yaml"
    // ))
    // .unwrap();
}

const GETTING_STARTED_EXAMPLES_PATH: &str =
    "../../docs/modules/zookeeper/examples/getting_started/code";

#[test]
fn test_getting_started_examples() {
    read::<ZookeeperCluster>(&format!("{GETTING_STARTED_EXAMPLES_PATH}/zookeeper.yaml"));
    read::<ZookeeperZnode>(&format!("{GETTING_STARTED_EXAMPLES_PATH}/znode.yaml"));
}
