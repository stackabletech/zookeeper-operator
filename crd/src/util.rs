use crate::{ZooKeeperCluster, ZooKeeperClusterSpec, APP_NAME, MANAGED_BY};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::Api;
use stackable_operator::client::Client;
use stackable_operator::error::Error::{InvalidName, KubeError, MissingObjectKey};
use stackable_operator::error::OperatorResult;
use stackable_operator::labels::{
    APP_INSTANCE_LABEL, APP_MANAGED_BY_LABEL, APP_NAME_LABEL, APP_ROLE_GROUP_LABEL,
};
use stackable_operator::pod_utils::is_pod_running_and_ready;
use std::collections::BTreeMap;
use tracing::{debug, warn};

pub struct ZookeeperConnectionInformation {
    pub connection_string: String,
}

pub async fn get_zk_connection_info(
    client: &Client,
    zookeeper_reference: ZooKeeperCluster,
) -> OperatorResult<ZookeeperConnectionInformation> {
    let zk_name = zookeeper_reference.metadata.name.unwrap();
    let zk_namespace = zookeeper_reference.metadata.namespace.unwrap();

    let zk_cluster = check_zookeeper_reference(client, &zk_name, &zk_namespace).await?;

    let zk_pods = client
        .list_with_label_selector(None, &get_match_labels(&zk_name))
        .await?;

    let connection_string = get_zk_connection_string_from_pods(zk_cluster.spec, zk_pods, None)?;

    Ok(ZookeeperConnectionInformation { connection_string })
}

fn get_match_labels(name: &str) -> LabelSelector {
    let mut zk_pod_matchlabels = BTreeMap::new();
    // TODO: retrieve these from the zookeeper crd crate - which means moving them there first :)
    zk_pod_matchlabels.insert(String::from(APP_NAME_LABEL), String::from(APP_NAME));
    zk_pod_matchlabels.insert(String::from(APP_MANAGED_BY_LABEL), String::from(MANAGED_BY));

    zk_pod_matchlabels.insert(String::from(APP_INSTANCE_LABEL), name.to_string());
    zk_pod_matchlabels.insert(String::from(APP_NAME_LABEL), String::from("zookeeper"));

    LabelSelector {
        match_labels: Some(zk_pod_matchlabels),
        ..Default::default()
    }
}

//
async fn check_zookeeper_reference(
    client: &Client,
    zk_name: &str,
    zk_namespace: &str,
) -> OperatorResult<ZooKeeperCluster> {
    debug!(
        "Checking ZookeeperReference if [{}] exists in namespace [{}].",
        zk_name, zk_namespace
    );
    let api: Api<ZooKeeperCluster> = client.get_namespaced_api(zk_namespace);

    let zk_cluster = api.get(zk_name).await;
    // TODO: We need to watch the ZooKeeper resource and do _something_ when it goes down or when its nodes are changed

    zk_cluster.map_err(|err| {
        warn!(?err,
                        "Referencing a ZooKeeper cluster that does not exist (or some other error while fetching it): [{}/{}], we will requeue and check again",
                        zk_namespace,
                        zk_name
                    );
                    KubeError {source: err}})
}

fn get_zk_connection_string_from_pods(
    zookeeper_spec: ZooKeeperClusterSpec,
    zk_pods: Vec<Pod>,
    chroot: Option<&str>,
) -> OperatorResult<String> {
    let mut server_and_port_list = Vec::new();

    for pod in zk_pods {
        if !is_pod_running_and_ready(&pod) {
            debug!(
                "Pod [{:?}] is assigned to this cluster but not ready, aborting.. ",
                pod.metadata.name
            );
            return Err(InvalidName {
                errors: vec![String::from("pod not ready")],
            });
        }
        let node_name = pod.spec.unwrap().node_name.unwrap();
        let role_group = match pod
            .metadata
            .labels
            .unwrap_or_default()
            .get(APP_ROLE_GROUP_LABEL)
        {
            None => {
                return Err(MissingObjectKey {
                    key: APP_ROLE_GROUP_LABEL,
                })
            }
            Some(role_group) => role_group.to_owned(),
        };

        server_and_port_list.push((node_name, get_zk_port(&zookeeper_spec, &role_group)?));
    }

    let conn_string = server_and_port_list
        .iter()
        .map(|(host, port)| format!("{}:{}", host, port))
        .collect::<Vec<_>>()
        .join(",");

    if let Some(chroot_content) = chroot {
        Ok(format!("{}/{}", conn_string, chroot_content))
    } else {
        Ok(conn_string)
    }
}

fn get_zk_port(_zk_cluster: &ZooKeeperClusterSpec, _role_group: &str) -> OperatorResult<u16> {
    Ok(2181)
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use rstest::rstest;

    #[test]
    fn get_labels_from_name() {
        let test_name = "testcluster";
        let selector = get_match_labels(test_name);

        assert!(selector.match_expressions.is_none());

        let generated_labels_selector = selector.match_labels.expect("labels were None");
        assert!(generated_labels_selector.len() == 3);
        assert_eq!(
            generated_labels_selector
                .get("app.kubernetes.io/name")
                .unwrap(),
            "zookeeper"
        );
        assert_eq!(
            generated_labels_selector
                .get("app.kubernetes.io/instance")
                .unwrap(),
            "testcluster"
        );
        assert_eq!(
            generated_labels_selector
                .get("app.kubernetes.io/managed-by")
                .unwrap(),
            "stackable-zookeeper"
        );
    }

    #[rstest]
    #[case(indoc! {"
      version: 3.4.14
      servers:
        - node_name: debian
    "},
        indoc! {"
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/name: zookeeper
              app.kubernetes.io/role-group: default
              app.kubernetes.io/instance: test
          spec:
            nodeName: debian
            containers: []
          status:
            phase: Running
            conditions:
              - type: Ready
                status: True
        "},
        None,
        "debian:2181"
)]
    #[case(indoc! {"
      version: 3.4.14
      servers:
        - node_name: worker-1.stackable.tech
    "},
    indoc! {"
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/name: zookeeper
              app.kubernetes.io/role-group: default
              app.kubernetes.io/instance: test
          spec:
            nodeName: worker-1.stackable.tech
            containers: []
          status:
            phase: Running
            conditions:
              - type: Ready
                status: True
        "},
    Some("dev"),
    "worker-1.stackable.tech:2181/dev"
    )]
    #[case(indoc! {"
      version: 3.4.14
      servers:
        - node_name: debian
    "},
    indoc! {"
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/name: zookeeper
              app.kubernetes.io/role-group: default
              app.kubernetes.io/instance: test
          spec:
            nodeName: worker-1.stackable.demo
            containers: []
          status:
            phase: Running
            conditions:
              - type: Ready
                status: True
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/name: zookeeper
              app.kubernetes.io/role-group: default
              app.kubernetes.io/instance: test
          spec:
            nodeName: worker-2.stackable.demo
            containers: []
          status:
            phase: Running
            conditions:
              - type: Ready
                status: True
        "},
    Some("prod"),
    "worker-1.stackable.demo:2181,worker-2.stackable.demo:2181/prod"
    )]
    fn get_connection_string(
        #[case] zookeeper_spec: &str,
        #[case] zk_pods: &str,
        #[case] chroot: Option<&str>,
        #[case] expected_result: &str,
    ) {
        let pods = parse_pod_list_from_yaml(zk_pods);
        let zk = parse_zk_from_yaml(zookeeper_spec);

        let conn_string =
            get_zk_connection_string_from_pods(zk.clone(), pods.clone(), chroot.clone())
                .expect("should not fail");
        assert_eq!(expected_result, conn_string);
    }

    #[rstest]
    #[case(indoc! {"
      version: 3.4.14
      servers:
        - node_name: debian
    "},
    indoc! {"
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/name: zookeeper
              app.kubernetes.io/role-group: default
              app.kubernetes.io/instance: test
          spec:
            nodeName: worker-1.stackable.demo
            containers: []
          status:
            phase: Running
            conditions:
              - type: Ready
                status: True
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/name: zookeeper
              app.kubernetes.io/role-group: default
              app.kubernetes.io/instance: test
          spec:
            nodeName: worker-2.stackable.demo
            containers: []
          status:
            phase: Pending
            conditions:
              - type: Ready
                status: True
        "},
    Some("prod"),
    )]
    #[case(indoc! {"
      version: 3.4.14
      servers:
        - node_name: debian
    "},
    indoc! {"
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/name: zookeeper
              app.kubernetes.io/role-group: default
              app.kubernetes.io/instance: test
          spec:
            nodeName: worker-1.stackable.demo
            containers: []
          status:
            phase: Running
        "},
    Some("prod"),
    )]
    #[case(indoc! {"
      version: 3.4.14
      servers:
        - node_name: debian
    "},
    indoc! {"
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/role-group: default
              app.kubernetes.io/instance: test
          spec:
            nodeName: worker-1.stackable.demo
            containers: []
          status:
            phase: Running
        "},
    Some("prod"),
    )]

    fn get_connection_string_should_fail(
        #[case] zookeeper_spec: &str,
        #[case] zk_pods: &str,
        #[case] chroot: Option<&str>,
    ) {
        let pods = parse_pod_list_from_yaml(zk_pods);
        let zk = parse_zk_from_yaml(zookeeper_spec);

        let conn_string =
            get_zk_connection_string_from_pods(zk.clone(), pods.clone(), chroot.clone());

        assert!(conn_string.is_err())
    }

    fn parse_pod_list_from_yaml(pod_config: &str) -> Vec<Pod> {
        let kube_pods: Vec<k8s_openapi::api::core::v1::Pod> =
            serde_yaml::from_str(pod_config).unwrap();
        kube_pods
            .iter()
            .map(|pod| Pod::from(pod.to_owned()))
            .collect()
    }

    fn parse_zk_from_yaml(zk_config: &str) -> ZooKeeperClusterSpec {
        serde_yaml::from_str(zk_config).unwrap()
    }
}
