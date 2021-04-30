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

/// Contains all necessary information to configure a connection with this
/// Zookeeper instance
#[allow(dead_code)]
pub struct ZookeeperConnectionInformation {
    pub connection_string: String,
}

/// Returns connection information for a ZookeeperCluster custom resource
///
/// # Arguments
///
/// * `client` - A [`stackable_operator::client::Client`] used to access the Kubernetes cluster
/// * `zk_name` - The name of the ZookeeperCluster cr
/// * `zk_namespace` - The namespace this ZookeeperCluster cr is in, if not provided will
///                     default to all namespaces
/// * `chroot` - If provided, this will be appended as chroot to the connection string
///
#[allow(dead_code)]
pub async fn get_zk_connection_info(
    client: &Client,
    zk_name: &str,
    zk_namespace: Option<&str>,
    chroot: Option<&str>,
) -> OperatorResult<ZookeeperConnectionInformation> {
    let zk_cluster = check_zookeeper_reference(client, zk_name, zk_namespace).await?;

    let zk_pods = client
        .list_with_label_selector(None, &get_match_labels(&zk_name))
        .await?;

    let connection_string = get_zk_connection_string_from_pods(zk_cluster.spec, zk_pods, chroot)?;

    Ok(ZookeeperConnectionInformation { connection_string })
}

// Build a Labelselector that applies only to pods belonging to the cluster instance referenced
// by `name`
fn get_match_labels(name: &str) -> LabelSelector {
    let mut zk_pod_matchlabels = BTreeMap::new();
    zk_pod_matchlabels.insert(String::from(APP_NAME_LABEL), String::from(APP_NAME));
    zk_pod_matchlabels.insert(String::from(APP_MANAGED_BY_LABEL), String::from(MANAGED_BY));
    zk_pod_matchlabels.insert(String::from(APP_INSTANCE_LABEL), name.to_string());

    LabelSelector {
        match_labels: Some(zk_pod_matchlabels),
        ..Default::default()
    }
}

// Check in kubernetes, whether the zookeeper object referenced by `zk_name` and `zk_namespace`
// exists.
// If it exists the object will be returned
async fn check_zookeeper_reference(
    client: &Client,
    zk_name: &str,
    zk_namespace: Option<&str>,
) -> OperatorResult<ZooKeeperCluster> {
    debug!(
        "Checking ZookeeperReference if [{}] exists in namespace [{}].",
        zk_name,
        zk_namespace.unwrap_or("<>")
    );
    let api: Api<ZooKeeperCluster> = client.get_api(zk_namespace);

    let zk_cluster = api.get(zk_name).await;

    // TODO: We need to watch the ZooKeeper resource and do _something_ when it goes down or when its nodes are changed

    zk_cluster.map_err(|err| {
        warn!(?err,
                        "Referencing a ZooKeeper cluster that does not exist (or some other error while fetching it): [{}/{}], we will requeue and check again",
                        zk_namespace.unwrap_or("<>"),
                        zk_name
                    );
                    KubeError {source: err}})
}

// Builds the actual connection string after all necessary information has been retrieved.
// Takes a list of pods belonging to this cluster from which the hostnames are retrieved and
// the cluster spec itself, from which the port per host will be retrieved (unimplemented at
// this time)
fn get_zk_connection_string_from_pods(
    zookeeper_spec: ZooKeeperClusterSpec,
    zk_pods: Vec<Pod>,
    chroot: Option<&str>,
) -> OperatorResult<String> {
    let mut server_and_port_list = Vec::new();

    for pod in zk_pods {
        if !is_pod_running_and_ready(&pod) {
            // TODO: Do we really want to fail in this case?
            //  The current handling would require other operators using this
            //  To requeue on error and wait until the cluster is fully functioning before
            //  retrieving the connect string.
            let err_message = format!(
                "Pod [{:?}] is assigned to this cluster but not ready, aborting.. ",
                pod.metadata.name
            );
            debug!("{}", &err_message);
            // TODO: This is not the correct error type, need to add one in operator-rs
            return Err(InvalidName {
                errors: vec![err_message],
            });
        }

        let node_name = match pod.spec.and_then(|spec| spec.node_name) {
            None => {
                let err_message = format!( "Pod [{:?}] is does not have node_name set, might not be scheduled yet, aborting.. ",
                                           pod.metadata.name);
                debug!("{}", &err_message);
                // TODO: This is not the correct error type, need to add one in operator-rs
                return Err(InvalidName {
                    errors: vec![err_message],
                });
            }
            Some(node_name) => node_name,
        };

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

    // Sort list by hostname to make resulting connection strings predictable
    // Shouldn't matter for connectivity but makes testing easier and avoids unnecessary
    // changes to the infrastructure
    server_and_port_list.sort_by(|(host1, _), (host2, _)| host1.cmp(host2));

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

// Retrieve the port for the specified rolegroup from the cluster spec
// TODO: Currently hard coded to 2181 as we do not support this setting yet.
//  Depends on https://github.com/stackabletech/zookeeper-operator/pull/71
//  Depends on https://github.com/stackabletech/zookeeper-operator/issues/85
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
    #[case::single_pod_no_chroot(
      indoc! {"
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
    #[case::single_pod_with_chroot(
      indoc! {"
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
    #[case::multiple_pods_wrong_order(
      indoc! {"
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
            nodeName: worker-2.stackable.demo
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
            nodeName: worker-1.stackable.demo
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
    #[case::pending_pod(
      indoc! {"
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
    #[case::missing_ready_condition(
      indoc! {"
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
    #[case::missing_mandatory_label(
      indoc! {"
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
              app.kubernetes.io/instance: test
          spec:
            nodeName: worker-1.stackable.demo
            containers: []
          status:
            phase: Running
            conditions:
              - type: Ready
                status: True
      "},
      Some("prod"),
    )]
    #[case::missing_hostname(
      indoc! {"
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
            containers: []
          status:
            phase: Running
            conditions:
              - type: Ready
                status: True
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
