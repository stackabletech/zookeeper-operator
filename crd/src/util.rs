use crate::error::Error::{
    IllegalChroot, ObjectWithoutName, OperatorFrameworkError, PodMissingLabels, PodWithoutHostname,
};
use crate::error::ZookeeperOperatorResult;
use crate::util::TicketReferences::ErrZkPodWithoutName;
use crate::{ZooKeeperCluster, ZooKeeperClusterSpec, APP_NAME, MANAGED_BY};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use stackable_operator::client::Client;
use stackable_operator::error::OperatorResult;
use stackable_operator::labels::{
    APP_INSTANCE_LABEL, APP_MANAGED_BY_LABEL, APP_NAME_LABEL, APP_ROLE_GROUP_LABEL,
};
use std::collections::{BTreeMap, HashSet};
use std::string::ToString;
use strum_macros::Display;
use tracing::{debug, warn};

const RESERVED_WORDS: [&str; 3] = [".", "..", "zookeeper"];

#[derive(Display)]
pub enum TicketReferences {
    ErrZkPodWithoutName,
}

/// Contains all necessary information to configure a connection with a
/// ZooKeeper ensemble
#[allow(dead_code)]
pub struct ZookeeperConnectionInformation {
    // A connection string as defined by ZooKeeper
    // https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#ch_zkSessions
    // This has the form `host:port[,host:port,...][/chroot]`
    // For example:
    //  - server1:2181,server2:2181
    //  - server1:2181,server2:2181/application
    pub connection_string: String,
}

/// Returns connection information for a ZookeeperCluster custom resource
///
/// # Arguments
///
/// * `client` - A [`stackable_operator::client::Client`] used to access the Kubernetes cluster
/// * `zk_name` - The name of the ZookeeperCluster custom resource
/// * `zk_namespace` - The namespace this ZookeeperCluster custom resource is in
/// * `chroot` - If provided, this will be appended as chroot to the connection string
#[allow(dead_code)]
pub async fn get_zk_connection_info(
    client: &Client,
    zk_name: &str,
    zk_namespace: &str,
    chroot: Option<&str>,
) -> ZookeeperOperatorResult<ZookeeperConnectionInformation> {
    let clean_chroot = is_valid_chroot(chroot)?;

    let zk_cluster = check_zookeeper_reference(client, zk_name, zk_namespace).await?;

    let zk_pods = client
        .list_with_label_selector(None, &get_match_labels(&zk_name))
        .await?;

    let connection_string =
        get_zk_connection_string_from_pods(zk_cluster.spec, zk_pods, clean_chroot)?;

    Ok(ZookeeperConnectionInformation { connection_string })
}

/// Check if a string is a valid chroot string to use in a ZooKeeper connection
/// string
///
/// Leading and trailing slashes are ignored, as we strip these before using the
/// string in a ZooKeeper chroot string.
///
/// Slashes elsewhere in the String are allowed as this signifies a valid path.
///
/// # Arguments
///
/// * `chroot` - The chroot string which should be checked against ZooKeeper's naming rules
///
/// # Errors
///
/// * [`IllegalChroot`] if the name violates any of ZooKeeper's naming rules
pub fn is_valid_chroot(chroot: Option<&str>) -> ZookeeperOperatorResult<Option<&str>> {
    let chroot = match chroot {
        None => return Ok(None),
        Some(chroot) => chroot,
    };

    let chroot = chroot.trim_start_matches('/').trim_end_matches('/');

    if chroot.is_empty() {
        return Err(IllegalChroot {
            chroot: chroot.to_string(),
            message: "chroot was provided but empty".to_string(),
        });
    }

    is_valid_node(chroot)?;

    Ok(Some(chroot))
}

/// Check if the name is a valid name for a znode in ZooKeeper
/// This does not currently fail on presence of '/' as this would simply be a nested
/// path (like a subdirectory in a folder structure)
/// We may want to revisit this later depending on how exactly this code is used
///
/// # Arguments
///
/// * `node_name` - The name which should be checked against ZooKeepers naming rules
///
/// # Errors
///
/// * [`IllegalChroot`] if the name violates any of ZooKeepers naming rules
pub fn is_valid_node(node_name: &str) -> ZookeeperOperatorResult<()> {
    if RESERVED_WORDS.contains(&node_name) {
        return Err(IllegalChroot {
            chroot: node_name.to_string(),
            message: format!("{} is a reserved word and cannot be used", node_name),
        });
    }

    let illegal_chars = contains_illegal_character(node_name);

    if illegal_chars.is_empty() {
        Ok(())
    } else {
        Err(IllegalChroot {
            chroot: node_name.to_string(),
            message: format!(
                "string contained prohibited unicode characters: [{:?}]",
                illegal_chars
            ),
        })
    }
}

// Checks the string for any illegal characters according to ZooKeeper's rules and returns
// a HashSet containing all illegal characters
fn contains_illegal_character(node_name: &str) -> HashSet<char> {
    // The value E000 in the list below does not exactly match what is written in the Zookeeper
    // docs, this is because rust `char`s cannot represent surrogates, so for D800 we moved this
    // to the next _legal_ value of E000
    node_name
        .chars()
        .filter(|character| {
            matches!(character, '\u{0000}'
        | '\u{0001}'..='\u{001F}'
        | '\u{007F}'
        | '\u{009F}'
        | '\u{E000}'..='\u{F8FF}'
        | '\u{FFF0}'..='\u{FFFF}' )
        })
        .collect()
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
    zk_namespace: &str,
) -> ZookeeperOperatorResult<ZooKeeperCluster> {
    debug!(
        "Checking ZookeeperReference if [{}] exists in namespace [{}].",
        zk_name, zk_namespace
    );
    let zk_cluster: OperatorResult<ZooKeeperCluster> =
        client.get(zk_name, Some(zk_namespace)).await;

    zk_cluster.map_err(|err| {
        warn!(?err,
                        "Referencing a ZooKeeper cluster that does not exist (or some other error while fetching it): [{}/{}], we will requeue and check again",
                        zk_namespace,
                        zk_name
                    );
                    OperatorFrameworkError {source: err}})
}

// Builds the actual connection string after all necessary information has been retrieved.
// Takes a list of pods belonging to this cluster from which the hostnames are retrieved and
// the cluster spec itself, from which the port per host will be retrieved (unimplemented at
// this time)
fn get_zk_connection_string_from_pods(
    zookeeper_spec: ZooKeeperClusterSpec,
    zk_pods: Vec<Pod>,
    chroot: Option<&str>,
) -> ZookeeperOperatorResult<String> {
    let mut server_and_port_list = Vec::new();

    for pod in zk_pods {
        let pod_name = match pod.metadata.name {
            None => {
                return Err(ObjectWithoutName {
                    reference: ErrZkPodWithoutName.to_string(),
                })
            }
            Some(pod_name) => pod_name,
        };

        let node_name = match pod.spec.and_then(|spec| spec.node_name) {
            None => {
                debug!("Pod [{:?}] is does not have node_name set, might not be scheduled yet, aborting.. ",
                       pod_name);
                return Err(PodWithoutHostname { pod: pod_name });
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
                return Err(PodMissingLabels {
                    labels: vec![String::from(APP_ROLE_GROUP_LABEL)],
                    pod: pod_name,
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
fn get_zk_port(
    _zk_cluster: &ZooKeeperClusterSpec,
    _role_group: &str,
) -> ZookeeperOperatorResult<u16> {
    Ok(2181)
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use rstest::rstest;
    use std::iter::FromIterator;

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
    #[case::trim_leading_slash("/test", "test")]
    #[case::trim_trailing_slash("test/", "test")]
    #[case::trim_leading_and_trailing_slash("/test/", "test")]
    #[case::no_trim("test", "test")]
    #[case::unicode_characters("Spade: ♠", "Spade: ♠")]
    #[case::unicode_characters_leading_slash("/Heart: ♥", "Heart: ♥")]
    #[case::unicode_characters_trailing_slash("Diamond: ♦/", "Diamond: ♦")]
    #[case::unicode_characters_both_slashes("/Club: ♣/", "Club: ♣")]
    fn valid_chroot(#[case] input: &str, #[case] expected_output: &str) {
        let output = is_valid_chroot(Some(input))
            .expect("Valid chroots shouldn't return an error.")
            .expect("Chroot should not be none");
        assert_eq!(output, expected_output);
    }

    #[rstest]
    // Legal strings
    #[case("test ",vec![])]
    #[case("testtest",vec![])]
    #[case("QFGNEtrd(}ſ‘‚gf(rdtn",vec![])]
    #[case("blbaG—\"tirane\u{0020}",vec![])]
    #[case("\u{009E}",vec![])]
    #[case("\u{00A0}",vec![])]
    #[case("uiaeuaie\u{00A0}uiaeuaie\u{009E}udtairneaiu",vec![])]
    #[case("/zook\u{FFEF}eeper/",vec![])]
    // Strictly speaking illegal, but contain only valid characters
    #[case("zookeeper",vec![])]
    #[case(".",vec![])]
    #[case("..",vec![])]
    // Illegal Strings
    #[case("/te\u{0000}st",vec!['\u{0000}'])]
    #[case("/te\u{0001}st",vec!['\u{0001}'])]
    #[case("/te\u{007F}s♥t",vec!['\u{007F}'])]
    #[case("/te\u{009F}st",vec!['\u{009F}'])]
    #[case("/te\u{009F}st\u{0001}",vec!['\u{009F}','\u{0001}'])]
    #[case("eiuatreunaieä&‚‘’<)(te\u{E000}st",vec!['\u{E000}'])]
    #[case("\u{0001}\u{001F}\u{007F}\u{009F}\u{E000}\u{F8FF}\u{FFF0}\u{FFFF}",vec!['\u{F8FF}','\u{0001}','\u{001F}','\u{FFFF}','\u{007F}','\u{009F}','\u{E000}','\u{FFF0}'])]
    #[case("/te\u{E000}st",vec!['\u{E000}'])]
    #[case("/te\u{FFF0}st",vec!['\u{FFF0}'])]
    fn identify_illegal_characters(#[case] input: &str, #[case] expected_illegal_chars: Vec<char>) {
        // Convert vec to set because we do not care about the order of illegal characters
        // that are identified
        let expected_illegal_chars: HashSet<char> =
            HashSet::from_iter(expected_illegal_chars.into_iter());
        let illegal_chars = contains_illegal_character(input);
        assert_eq!(expected_illegal_chars, illegal_chars);
    }

    #[rstest]
    #[case::reserved_word("zookeeper")]
    #[case::reserved_word_with_leading_slash("/..")]
    #[case::reserved_word_with_trailing_slash("./")]
    #[case::empty("")]
    #[case::forbidden_nullcharacter("\u{0000}")]
    #[case::forbidden_character_from_range("hello\u{E000}test")]
    #[case::forbidden_character_leading_slash("/_\u{FFFF}_h_el.lo_")]
    fn invalid_chroot(#[case] input: &str) {
        let output = is_valid_chroot(Some(input));
        assert!(output.is_err());
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
