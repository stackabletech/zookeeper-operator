use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperClusterSpec};

pub fn build_init_container_args(zk: &ZookeeperCluster) -> Vec<String> {
    //let mut args = vec![];
    let spec: &ZookeeperClusterSpec = &zk.spec;

    vec![
        "chown stackable:stackable /stackable/data",
        "chmod a=,u=rwX /stackable/data",
        "expr $MYID_OFFSET + $(echo $POD_NAME | sed 's/.*-//') > /stackable/data/myid",
    ]
    .join(" && ");

    vec!["test".to_string()]
}
