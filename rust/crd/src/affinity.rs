use stackable_operator::{
    commons::affinity::{affinity_between_role_pods, StackableAffinityFragment},
    k8s_openapi::api::core::v1::PodAntiAffinity,
};

use crate::{ZookeeperRole, APP_NAME};

pub fn get_affinity(cluster_name: &str, role: &ZookeeperRole) -> StackableAffinityFragment {
    let affinity_between_role_pods =
        affinity_between_role_pods(APP_NAME, cluster_name, &role.to_string(), 70);

    StackableAffinityFragment {
        pod_affinity: None,
        pod_anti_affinity: Some(PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                affinity_between_role_pods,
            ]),
            required_during_scheduling_ignored_during_execution: None,
        }),
        node_affinity: None,
        node_selector: None,
    }
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeMap;

    use stackable_operator::{
        commons::affinity::{StackableAffinity, StackableNodeSelector},
        k8s_openapi::{
            api::core::v1::{
                NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
                PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm,
            },
            apimachinery::pkg::apis::meta::v1::LabelSelector,
        },
        kube::runtime::reflector::ObjectRef,
        role_utils::RoleGroupRef,
    };

    use crate::{ZookeeperCluster, ZookeeperRole};

    #[test]
    fn test_affinity_defaults() {
        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zk
        spec:
          image:
            productVersion: 3.8.2
          clusterConfig:
            authentication:
              - authenticationClass: zk-client-tls
            tls:
              serverSecretClass: tls
              quorumSecretClass: tls
          servers:
            roleGroups:
              default:
                replicas: 3
        "#;
        let zk: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");

        let rolegroup_ref = RoleGroupRef {
            cluster: ObjectRef::from_obj(&zk),
            role: ZookeeperRole::Server.to_string(),
            role_group: "default".to_string(),
        };

        let expected: StackableAffinity = StackableAffinity {
            pod_affinity: None,
            pod_anti_affinity: Some(PodAntiAffinity {
                required_during_scheduling_ignored_during_execution: None,
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    WeightedPodAffinityTerm {
                        pod_affinity_term: PodAffinityTerm {
                            label_selector: Some(LabelSelector {
                                match_expressions: None,
                                match_labels: Some(BTreeMap::from([
                                    (
                                        "app.kubernetes.io/name".to_string(),
                                        "zookeeper".to_string(),
                                    ),
                                    (
                                        "app.kubernetes.io/instance".to_string(),
                                        "simple-zk".to_string(),
                                    ),
                                    (
                                        "app.kubernetes.io/component".to_string(),
                                        "server".to_string(),
                                    ),
                                ])),
                            }),
                            namespace_selector: None,
                            namespaces: None,
                            topology_key: "kubernetes.io/hostname".to_string(),
                        },
                        weight: 70,
                    },
                ]),
            }),

            node_affinity: None,
            node_selector: None,
        };

        let affinity = zk
            .merged_config(&ZookeeperRole::Server, &rolegroup_ref)
            .unwrap()
            .affinity;

        assert_eq!(affinity, expected);
    }

    #[test]
    fn test_affinity_legacy_node_selector() {
        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zk
        spec:
          image:
            productVersion: 3.8.2
          clusterConfig:
            authentication:
              - authenticationClass: zk-client-tls
            tls:
              serverSecretClass: tls
              quorumSecretClass: tls
          servers:
            roleGroups:
              default:
                replicas: 3
                selector:
                  matchLabels:
                    disktype: ssd
                  matchExpressions:
                    - key: topology.kubernetes.io/zone
                      operator: In
                      values:
                        - antarctica-east1
                        - antarctica-west1
        "#;

        let zk: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");

        let expected: StackableAffinity = StackableAffinity {
            node_affinity: Some(NodeAffinity {
                preferred_during_scheduling_ignored_during_execution: None,
                required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                    node_selector_terms: vec![NodeSelectorTerm {
                        match_expressions: Some(vec![NodeSelectorRequirement {
                            key: "topology.kubernetes.io/zone".to_string(),
                            operator: "In".to_string(),
                            values: Some(vec![
                                "antarctica-east1".to_string(),
                                "antarctica-west1".to_string(),
                            ]),
                        }]),
                        match_fields: None,
                    }],
                }),
            }),
            node_selector: Some(StackableNodeSelector {
                node_selector: BTreeMap::from([("disktype".to_string(), "ssd".to_string())]),
            }),
            pod_affinity: None,
            pod_anti_affinity: Some(PodAntiAffinity {
                required_during_scheduling_ignored_during_execution: None,
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    WeightedPodAffinityTerm {
                        pod_affinity_term: PodAffinityTerm {
                            label_selector: Some(LabelSelector {
                                match_expressions: None,
                                match_labels: Some(BTreeMap::from([
                                    (
                                        "app.kubernetes.io/name".to_string(),
                                        "zookeeper".to_string(),
                                    ),
                                    (
                                        "app.kubernetes.io/instance".to_string(),
                                        "simple-zk".to_string(),
                                    ),
                                    (
                                        "app.kubernetes.io/component".to_string(),
                                        "server".to_string(),
                                    ),
                                ])),
                            }),
                            namespace_selector: None,
                            namespaces: None,
                            topology_key: "kubernetes.io/hostname".to_string(),
                        },
                        weight: 70,
                    },
                ]),
            }),
        };

        let rolegroup_ref = RoleGroupRef {
            cluster: ObjectRef::from_obj(&zk),
            role: ZookeeperRole::Server.to_string(),
            role_group: "default".to_string(),
        };

        let affinity = zk
            .merged_config(&ZookeeperRole::Server, &rolegroup_ref)
            .unwrap()
            .affinity;

        assert_eq!(affinity, expected);
    }
}
