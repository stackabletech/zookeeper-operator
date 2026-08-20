//! Builds the RBAC resources (ServiceAccount + RoleBinding) shared by all role groups.

use stackable_operator::{
    k8s_openapi::api::{core::v1::ServiceAccount, rbac::v1::RoleBinding},
    v2::rbac,
};

use crate::zk_controller::{
    build::recommended_labels_for_cluster_resources, validate::ValidatedCluster,
};

/// Builds the [`ServiceAccount`] that the role-group Pods run under.
pub fn build_service_account(cluster: &ValidatedCluster) -> ServiceAccount {
    rbac::build_service_account(
        cluster,
        &cluster.cluster_resource_names(),
        recommended_labels_for_cluster_resources(cluster),
    )
}

/// Builds the [`RoleBinding`] that binds the [`ServiceAccount`] from [`build_service_account`] to
/// the operator-deployed ClusterRole.
pub fn build_role_binding(cluster: &ValidatedCluster) -> RoleBinding {
    rbac::build_role_binding(
        cluster,
        &cluster.cluster_resource_names(),
        recommended_labels_for_cluster_resources(cluster),
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::zk_controller::test_support::{app_version_label, minimal_zk, validated_cluster};

    // `simple-zookeeper` vs `zookeeper`: see the swap-guard note on `minimal_zk`.
    fn cluster() -> ValidatedCluster {
        let zookeeper = minimal_zk(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: simple-zookeeper
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleGroups:
                  default:
                    replicas: 1
            "#,
        );
        validated_cluster(&zookeeper)
    }

    #[test]
    fn test_service_account() {
        let service_account = build_service_account(&cluster());

        assert_eq!(
            json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {
                    // The RBAC resources are cluster-shared, so they carry no component or
                    // role-group label.
                    "labels": {
                        "app.kubernetes.io/instance": "simple-zookeeper",
                        "app.kubernetes.io/managed-by": "zookeeper.stackable.tech_zookeepercluster",
                        "app.kubernetes.io/name": "zookeeper",
                        "app.kubernetes.io/version": app_version_label("3.9.5"),
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "simple-zookeeper-serviceaccount",
                    "namespace": "default",
                    "ownerReferences": [
                        {
                            "apiVersion": "zookeeper.stackable.tech/v1alpha1",
                            "controller": true,
                            "kind": "ZookeeperCluster",
                            "name": "simple-zookeeper",
                            "uid": "c27b3971-ca72-42c1-80a4-abdfc1db0ddd"
                        }
                    ]
                }
            }),
            serde_json::to_value(service_account).expect("must be serializable")
        );
    }

    #[test]
    fn test_role_binding() {
        let role_binding = build_role_binding(&cluster());

        assert_eq!(
            json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/instance": "simple-zookeeper",
                        "app.kubernetes.io/managed-by": "zookeeper.stackable.tech_zookeepercluster",
                        "app.kubernetes.io/name": "zookeeper",
                        "app.kubernetes.io/version": app_version_label("3.9.5"),
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "simple-zookeeper-rolebinding",
                    "namespace": "default",
                    "ownerReferences": [
                        {
                            "apiVersion": "zookeeper.stackable.tech/v1alpha1",
                            "controller": true,
                            "kind": "ZookeeperCluster",
                            "name": "simple-zookeeper",
                            "uid": "c27b3971-ca72-42c1-80a4-abdfc1db0ddd"
                        }
                    ]
                },
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "ClusterRole",
                    "name": "zookeeper-clusterrole"
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "name": "simple-zookeeper-serviceaccount",
                        "namespace": "default"
                    }
                ]
            }),
            serde_json::to_value(role_binding).expect("must be serializable")
        );
    }
}
