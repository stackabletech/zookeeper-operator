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

use std::{marker::PhantomData, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{builder::meta::ownerreference_from_resource, types::operator::RoleGroupName},
};

use crate::{
    crd::ZookeeperRole,
    discovery,
    zk_controller::{
        KubernetesResources, Prepared, ZK_CONTROLLER_NAME,
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

    #[snafu(display("failed to build the discovery ConfigMap"))]
    DiscoveryConfigMap { source: discovery::Error },
}

/// Builds every Kubernetes resource for the given validated cluster.
///
/// Does not need a Kubernetes client: every reference to another Kubernetes resource is already
/// dereferenced and validated by this point, so the errors returned here are resource-assembly
/// failures only. `cluster_info` is static cluster metadata (not a client call), consumed by the
/// role-group ConfigMap builder.
///
/// The discovery `ConfigMap` is only built once the role
/// [`Listener`](stackable_operator::crd::listener::v1alpha1::Listener) publishes ingress addresses.
/// Those are dereferenced and validated into
/// [`ValidatedCluster::discovery_addresses`](ValidatedCluster#structfield.discovery_addresses)
/// before this step runs, so the ConfigMap is absent during the reconciliation that first creates
/// the Listener, and built by the one that the Listener watch triggers afterwards.
pub fn build(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> Result<KubernetesResources<Prepared>, Error> {
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

    if let Some(pdb) = build_pdb(&cluster.role_config.pdb, cluster, &zk_role) {
        pod_disruption_budgets.push(pdb);
    }

    let listeners = vec![build_role_listener(cluster, &zk_role)];

    let maybe_discovery_config_map = cluster
        .discovery_addresses
        .as_ref()
        .map(|listener_addresses| {
            discovery::build_discovery_configmap(cluster, ZK_CONTROLLER_NAME, listener_addresses)
        })
        .transpose()
        .context(DiscoveryConfigMapSnafu)?;

    Ok(KubernetesResources {
        stateful_sets,
        services,
        listeners,
        config_maps,
        maybe_discovery_config_map,
        pod_disruption_budgets,
        service_accounts: vec![build_service_account(cluster)],
        role_bindings: vec![build_role_binding(cluster)],
        status: PhantomData,
    })
}

/// Returns an [`ObjectMetaBuilder`] pre-filled with the namespace, an owner reference back to
/// the cluster, and the recommended labels for a resource named `name` in `role_group_name`.
///
/// Consolidates the metadata chain repeated by the child-resource builders. Call sites that
/// need extra labels/annotations chain them onto the returned builder.
pub(crate) fn object_meta(
    cluster: &ValidatedCluster,
    name: impl Into<String>,
    role_group_name: &RoleGroupName,
) -> ObjectMetaBuilder {
    let mut builder = ObjectMetaBuilder::new();
    builder
        .name_and_namespace(cluster)
        .name(name)
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(cluster.recommended_labels(role_group_name));
    builder
}

#[cfg(test)]
mod tests {
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
        // One ConfigMap per role group.
        assert_eq!(
            sorted_names(&resources.config_maps),
            [
                "simple-zookeeper-server-default",
                "simple-zookeeper-server-secondary",
            ]
        );
        // The fixture has no role Listener yet, so the discovery ConfigMap is absent (see `build()`).
        assert!(resources.maybe_discovery_config_map.is_none());
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
        // The cluster-shared RBAC pair.
        assert_eq!(
            sorted_names(&resources.service_accounts),
            ["simple-zookeeper-serviceaccount"]
        );
        assert_eq!(
            sorted_names(&resources.role_bindings),
            ["simple-zookeeper-rolebinding"]
        );
    }
}
