//! Build steps for the ZookeeperCluster controller.
//!
//! Each submodule turns the [`ValidatedCluster`]
//! into a Kubernetes resource. The [`v1alpha1::ZookeeperCluster`](crate::crd::v1alpha1::ZookeeperCluster)
//! is only used for the owner reference and object metadata, never for
//! configuration.
//!
//! Builders that emit a complete cluster resource live under [`resource`]; the
//! remaining submodules ([`command`], [`graceful_shutdown`], [`jvm`],
//! [`properties`]) produce fragments that those resource builders assemble.

use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::types::operator::RoleGroupName,
};

use crate::{
    crd::ZookeeperRole,
    zk_controller::{
        KubernetesResources,
        build::resource::{
            config_map,
            listener::build_role_listener,
            pdb::build_pdb,
            rbac::{build_role_binding, build_service_account},
            service::{
                build_server_rolegroup_headless_service, build_server_rolegroup_metrics_service,
            },
            statefulset::{self, build_server_rolegroup_statefulset},
        },
        validate::ValidatedCluster,
    },
};

// Placeholder role-group name used for the recommended labels of the role-level discovery
// `ConfigMap` (which is not tied to a single role group).
stackable_operator::constant!(pub(crate) PLACEHOLDER_DISCOVERY_ROLE_GROUP: RoleGroupName = "discovery");

// Placeholder role-group name used for the recommended labels of the role-level `Listener`
// (which is not tied to a single role group).
stackable_operator::constant!(pub(crate) NONE_ROLE_GROUP_NAME: RoleGroupName = "none");

pub mod command;
pub mod graceful_shutdown;
pub mod jvm;
pub mod properties;
pub mod resource;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap for role group {rolegroup}"))]
    ConfigMap {
        source: config_map::Error,
        rolegroup: RoleGroupName,
    },

    #[snafu(display("failed to build StatefulSet for role group {rolegroup}"))]
    StatefulSet {
        source: statefulset::Error,
        rolegroup: RoleGroupName,
    },
}

/// Builds every Kubernetes resource for the given validated cluster.
///
/// Does not need a Kubernetes client: every reference to another Kubernetes resource is already
/// dereferenced and validated by this point, so the errors returned here are resource-assembly
/// failures only. `cluster_info` is static cluster metadata (not a client call), consumed by the
/// role-group ConfigMap builder.
///
/// The discovery `ConfigMap` is deliberately absent: it is built from the *applied* role
/// [`Listener`](stackable_operator::crd::listener::v1alpha1::Listener)'s ingress addresses, so it
/// is assembled in the reconcile step after the Listener has been applied, not here.
pub fn build(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> Result<KubernetesResources, Error> {
    let mut stateful_sets = vec![];
    let mut services = vec![];
    let mut config_maps = vec![];
    let mut pod_disruption_budgets = vec![];

    let zk_role = ZookeeperRole::Server;
    let server_role_group_configs = cluster
        .role_group_configs
        .get(&zk_role)
        .into_iter()
        .flatten();
    for (rolegroup_name, rolegroup_config) in server_role_group_configs {
        // Resource naming, labels and owner references are derived from the `ValidatedCluster` and
        // the type-safe `RoleGroupName`.
        services.push(build_server_rolegroup_headless_service(
            cluster,
            rolegroup_name,
        ));
        services.push(build_server_rolegroup_metrics_service(
            cluster,
            rolegroup_name,
            rolegroup_config,
        ));
        config_maps.push(
            config_map::build_server_rolegroup_config_map(
                cluster,
                cluster_info,
                rolegroup_name,
                rolegroup_config,
            )
            .context(ConfigMapSnafu {
                rolegroup: rolegroup_name.clone(),
            })?,
        );
        stateful_sets.push(
            build_server_rolegroup_statefulset(cluster, rolegroup_name, rolegroup_config)
                .with_context(|_| StatefulSetSnafu {
                    rolegroup: rolegroup_name.clone(),
                })?,
        );
    }

    if let Some(role_config) = &cluster.role_config
        && let Some(pdb) = build_pdb(&role_config.pdb, cluster, &zk_role)
    {
        pod_disruption_budgets.push(pdb);
    }

    let listeners = vec![build_role_listener(cluster, &zk_role)];

    Ok(KubernetesResources {
        stateful_sets,
        services,
        listeners,
        config_maps,
        pod_disruption_budgets,
        service_accounts: vec![build_service_account(cluster)],
        role_bindings: vec![build_role_binding(cluster)],
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use stackable_operator::kube::Resource;

    use super::build;
    use crate::zk_controller::test_support::{cluster_info, minimal_zk, validated_cluster};

    /// Collects the `.metadata.name`s of the given resources, sorted for stable comparison.
    fn sorted_names(resources: &[impl Resource]) -> Vec<&str> {
        let mut names: Vec<&str> = resources
            .iter()
            .filter_map(|resource| resource.meta().name.as_deref())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn build_produces_expected_resource_names() {
        let zookeeper_yaml = r#"
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
                replicas: 3
              secondary:
                replicas: 1
        "#;
        let zookeeper = minimal_zk(zookeeper_yaml);
        let cluster = validated_cluster(&zookeeper);

        let resources = build(&cluster, &cluster_info()).expect("build succeeds");

        // One StatefulSet per role group.
        assert_eq!(
            sorted_names(&resources.stateful_sets),
            [
                "simple-zookeeper-server-default",
                "simple-zookeeper-server-secondary",
            ]
        );
        // One headless and one metrics Service per role group.
        assert_eq!(
            sorted_names(&resources.services),
            [
                "simple-zookeeper-server-default-headless",
                "simple-zookeeper-server-default-metrics",
                "simple-zookeeper-server-secondary-headless",
                "simple-zookeeper-server-secondary-metrics",
            ]
        );
        // One ConfigMap per role group; the discovery ConfigMap is absent — see `build()`.
        assert_eq!(
            sorted_names(&resources.config_maps),
            [
                "simple-zookeeper-server-default",
                "simple-zookeeper-server-secondary",
            ]
        );
        // The single role-level Listener for the one ZooKeeper role (`server`).
        assert_eq!(
            sorted_names(&resources.listeners),
            ["simple-zookeeper-server"]
        );
        // The default PodDisruptionBudget for the `server` role.
        assert_eq!(
            sorted_names(&resources.pod_disruption_budgets),
            ["simple-zookeeper-server"]
        );
    }

    /// Locks the RBAC resource names, the roleRef, and the recommended label set against
    /// accidental drift. The fixture's cluster name deliberately differs from the product name so
    /// that swapped `name`/`instance` label values cannot pass unnoticed.
    #[test]
    fn build_produces_rbac() {
        let zookeeper_yaml = r#"
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
        "#;
        let zookeeper = minimal_zk(zookeeper_yaml);
        let cluster = validated_cluster(&zookeeper);

        let resources = build(&cluster, &cluster_info()).expect("build succeeds");

        assert_eq!(
            sorted_names(&resources.service_accounts),
            ["simple-zookeeper-serviceaccount"]
        );
        assert_eq!(
            sorted_names(&resources.role_bindings),
            ["simple-zookeeper-rolebinding"]
        );

        let expected_labels = BTreeMap::from(
            [
                ("app.kubernetes.io/component", "none"),
                ("app.kubernetes.io/instance", "simple-zookeeper"),
                (
                    "app.kubernetes.io/managed-by",
                    "zookeeper.stackable.tech_zookeepercluster",
                ),
                ("app.kubernetes.io/name", "zookeeper"),
                ("app.kubernetes.io/role-group", "none"),
                ("app.kubernetes.io/version", "3.9.5-stackable0.0.0-dev"),
                ("stackable.tech/vendor", "Stackable"),
            ]
            .map(|(key, value)| (key.to_string(), value.to_string())),
        );
        let service_account = resources
            .service_accounts
            .first()
            .expect("a ServiceAccount is built");
        assert_eq!(
            service_account.metadata.labels,
            Some(expected_labels.clone())
        );

        let role_binding = resources
            .role_bindings
            .first()
            .expect("a RoleBinding is built");
        assert_eq!(role_binding.metadata.labels, Some(expected_labels));
        assert_eq!(role_binding.role_ref.name, "zookeeper-clusterrole");
    }
}
