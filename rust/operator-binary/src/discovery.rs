//! Builders for the discovery ConfigMaps, which advertise how to connect to a ZooKeeper ensemble.
//!
//! Shared by the build steps of both controllers: the ZookeeperCluster controller publishes the
//! whole ensemble, the ZookeeperZnode controller publishes the same ensemble narrowed to a chroot.

use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{Resource, api::ObjectMeta},
    kvp::{Label, Labels},
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::meta::ownerreference_from_resource,
        kvp::label,
        types::{kubernetes::ConfigMapName, operator::ClusterName},
    },
};

use crate::{
    crd::{OPERATOR_NAME, PRODUCT_NAME, ZookeeperRole, security::ZookeeperSecurity},
    listener_addresses::ListenerAddresses,
    zk_controller::{build::recommended_labels_for_role_resources, validate::ValidatedCluster},
    znode_controller::{self, validate::ValidatedZnode},
};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("chroot path {} was relative (must be absolute)", chroot))]
    RelativeChroot { chroot: String },

    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },
}

/// The name of the cluster controller's discovery [`ConfigMap`], which is the cluster name.
///
/// Takes the bare cluster name (not [`ValidatedCluster`]) so that the dereference step, which runs
/// before validation, fetches the same ConfigMap that the build step writes.
pub fn discovery_config_map_name(cluster_name: &ClusterName) -> ConfigMapName {
    const _: () = assert!(
        ClusterName::MAX_LENGTH <= ConfigMapName::MAX_LENGTH,
        "The string `<cluster_name>` must not exceed the limit of ConfigMap names."
    );
    let _ = ClusterName::IS_RFC_1123_SUBDOMAIN_NAME;

    ConfigMapName::from_str(cluster_name.as_ref())
        .expect("a valid cluster name is a valid ConfigMap name")
}

/// Build the discovery [`ConfigMap`] for the cluster controller from the [`ValidatedCluster`].
///
/// The connection details come from the addresses published by the role Listener
/// ([`ValidatedCluster::discovery_addresses`]). While there is no usable address (around the
/// first reconciliation, after the Listener was re-created, or while its status is stale), no
/// fresh ConfigMap can be built. The stored one is then re-emitted unchanged, so that it stays
/// tracked by the apply step (an untracked ConfigMap would be deleted as an orphan) without
/// changing its content: an unchanged ConfigMap gets no new resource version, so consumers are
/// not restarted. Only the keys in [`DISCOVERY_CONFIG_MAP_KEYS`] are re-emitted.
///
/// `None` is returned while no discovery ConfigMap (owned by this cluster) is stored, e.g. before
/// the first successful build, or after it was deleted while the Listener had no usable address.
/// It is also returned while reconciliation is paused and none is stored yet: the paused apply
/// step only fetches resources, and fetching a ConfigMap that does not exist would fail every run.
/// The Listener watch triggers a new run once addresses are published.
pub fn build_discovery_configmap(
    validated_cluster: &ValidatedCluster,
    zk_role: &ZookeeperRole,
) -> Result<Option<ConfigMap>> {
    let labels = recommended_labels_for_role_resources(validated_cluster, zk_role);

    match (
        &validated_cluster.discovery_addresses,
        &validated_cluster.existing_discovery_config_map,
    ) {
        (Some(_), None) if validated_cluster.cluster_operation.reconciliation_paused => {
            tracing::debug!(
                "reconciliation is paused and no discovery ConfigMap is stored yet, skipping it"
            );
            Ok(None)
        }
        (Some(addresses), _) => build_discovery_configmap_for_owner(
            validated_cluster,
            &validated_cluster.namespace,
            labels,
            addresses,
            None,
            &validated_cluster.cluster_config.zookeeper_security,
        )
        .map(Some),
        (None, Some(existing)) => {
            tracing::debug!(
                "the role Listener has no usable address, re-emitting the stored discovery ConfigMap"
            );
            Ok(Some(ConfigMap {
                metadata: discovery_config_map_metadata(
                    validated_cluster,
                    &validated_cluster.namespace,
                    labels,
                ),
                data: existing.data.as_ref().map(|data| {
                    data.iter()
                        .filter(|(key, _)| DISCOVERY_CONFIG_MAP_KEYS.contains(&key.as_str()))
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect()
                }),
                ..ConfigMap::default()
            }))
        }
        (None, _) => {
            tracing::debug!(
                "the role Listener has no usable address and no discovery ConfigMap of this cluster \
                is stored, skipping it"
            );
            Ok(None)
        }
    }
}

/// Build the discovery [`ConfigMap`] for the znode controller.
///
/// The ConfigMap is owned by, and placed in the namespace of, the
/// [`ValidatedZnode`]. The product version and `zookeeper_security` originate from the referenced
/// cluster (via the validated znode), while `chroot` isolates the znode within the shared ZooKeeper
/// ensemble.
pub fn build_znode_discovery_configmap(
    validated_znode: &ValidatedZnode,
    listener_addresses: &ListenerAddresses,
    chroot: &str,
) -> Result<ConfigMap> {
    build_discovery_configmap_for_owner(
        validated_znode,
        &validated_znode.namespace,
        znode_discovery_labels(validated_znode),
        listener_addresses,
        Some(chroot),
        &validated_znode.zookeeper_security,
    )
}

/// The recommended labels for the znode's discovery [`ConfigMap`].
///
/// The znode controller's discovery ConfigMap cannot use the label functions from
/// [`stackable_operator::v2::kvp::label`], because its `app.kubernetes.io/instance` value is the
/// name of the owning [`ZookeeperZnode`](crate::crd::v1alpha1::ZookeeperZnode), not a
/// [`ClusterName`]. The label set matches the role-level recommended labels of the
/// cluster controller's discovery ConfigMap otherwise.
fn znode_discovery_labels(validated_znode: &ValidatedZnode) -> Labels {
    Labels::from_iter([
        Label::instance(&validated_znode.to_label_value()).expect(
            "the value implements NameIsValidLabelValue and is therefore a valid label value",
        ),
        label::label_app_kubernetes_io_name(&PRODUCT_NAME),
        label::label_app_kubernetes_io_version(&validated_znode.product_version),
        label::label_app_kubernetes_io_component(&ZookeeperRole::Server),
        label::label_app_kubernetes_io_managed_by(
            &OPERATOR_NAME,
            &znode_controller::CONTROLLER_NAME,
        ),
        label::label_stackable_tech_vendor(),
    ])
}

/// The keys of a discovery [`ConfigMap`], as written by [`build_discovery_configmap_for_owner`].
///
/// Only these are re-emitted, so that a key added by an objectOverride is not kept alive after the
/// override was removed.
const DISCOVERY_CONFIG_MAP_KEYS: [&str; 4] = [
    "ZOOKEEPER",
    "ZOOKEEPER_HOSTS",
    "ZOOKEEPER_CLIENT_PORT",
    "ZOOKEEPER_CHROOT",
];

/// Build a discovery [`ConfigMap`] containing ZooKeeper connection details from the
/// [`ListenerAddresses`] published by the role Listener.
///
/// `owner` owns the ConfigMap (the [`ZookeeperCluster`](crate::crd::v1alpha1::ZookeeperCluster) for the cluster
/// controller, or the [`ZookeeperZnode`](crate::crd::v1alpha1::ZookeeperZnode) for the znode controller) and
/// `namespace` is where the ConfigMap is placed.
fn build_discovery_configmap_for_owner(
    owner: &(impl Resource<DynamicType = ()> + HasName + HasUid + NameIsValidLabelValue),
    namespace: impl Into<String>,
    labels: Labels,
    listener_addresses: &ListenerAddresses,
    chroot: Option<&str>,
    zookeeper_security: &ZookeeperSecurity,
) -> Result<ConfigMap> {
    // Write a connection string of the format that Java ZooKeeper client expects:
    // "{host1}:{port1},{host2:port2},.../{chroot}"
    // See https://zookeeper.apache.org/doc/current/apidocs/zookeeper-server/org/apache/zookeeper/ZooKeeper.html#ZooKeeper-java.lang.String-int-org.apache.zookeeper.Watcher-
    let listener_addresses = listener_addresses.to_connection_string();
    let mut conn_str = listener_addresses.clone();
    if let Some(chroot) = chroot {
        if !chroot.starts_with('/') {
            return RelativeChrootSnafu { chroot }.fail();
        }
        conn_str.push_str(chroot);
    }
    ConfigMapBuilder::new()
        .metadata(discovery_config_map_metadata(owner, namespace, labels))
        .add_data("ZOOKEEPER", conn_str)
        // Some clients don't support ZooKeeper's merged `hosts/chroot` format, so export them separately for these clients
        .add_data("ZOOKEEPER_HOSTS", listener_addresses)
        .add_data(
            "ZOOKEEPER_CLIENT_PORT",
            zookeeper_security.client_port().to_string(),
        )
        .add_data("ZOOKEEPER_CHROOT", chroot.unwrap_or("/"))
        .build()
        .context(BuildConfigMapSnafu)
}

/// The metadata of a discovery [`ConfigMap`], shared by freshly built and re-emitted ConfigMaps
/// so that the two cannot drift apart.
fn discovery_config_map_metadata(
    owner: &(impl Resource<DynamicType = ()> + HasName + HasUid + NameIsValidLabelValue),
    namespace: impl Into<String>,
    labels: Labels,
) -> ObjectMeta {
    ObjectMetaBuilder::new()
        .name(owner.to_name())
        .namespace(namespace)
        .ownerreference(ownerreference_from_resource(owner, None, Some(true)))
        .with_labels(labels)
        .build()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use stackable_operator::{
        crd::listener, k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference,
    };

    use super::*;
    use crate::{
        crd::ZOOKEEPER_SERVER_PORT_NAME,
        listener_addresses::test_support::{ingress_address, role_listener},
        zk_controller::test_support::{
            CLUSTER_UID, app_version_label, minimal_zk, try_validate_with_role_listener,
        },
        znode_controller::test_support::{minimal_znode, validated_znode},
    };

    const ZK_YAML: &str = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.6"
          servers:
            roleGroups:
              default:
                replicas: 3
        "#;

    const ZNODE_YAML: &str = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperZnode
        metadata:
          name: simple-znode
        spec:
          clusterRef:
            name: simple-zookeeper
        "#;

    /// The fixture cluster with reconciliation paused.
    const PAUSED_ZK_YAML: &str = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.6"
          clusterOperation:
            reconciliationPaused: true
          servers:
            roleGroups:
              default:
                replicas: 3
        "#;

    /// The znode path that the znode controller derives from the fixture's UID.
    const ZNODE_PATH: &str = "/znode-e5dbf9c2-d8b0-4c1e-9f4a-1d2e3f4a5b6c";

    /// The value of `key` in the given discovery ConfigMap.
    fn data(config_map: &ConfigMap, key: &str) -> String {
        config_map
            .data
            .as_ref()
            .expect("the discovery ConfigMap should carry data")
            .get(key)
            .unwrap_or_else(|| panic!("the discovery ConfigMap should carry {key}"))
            .clone()
    }

    /// A role Listener publishing the given addresses under the ZooKeeper server port name.
    fn listener_with(addresses: &[(&str, u16)]) -> listener::v1alpha1::Listener {
        role_listener(Some(
            addresses
                .iter()
                .map(|(address, port)| {
                    ingress_address(address, ZOOKEEPER_SERVER_PORT_NAME, i32::from(*port))
                })
                .collect(),
        ))
    }

    /// A discovery ConfigMap as the API server returns it: with server-populated metadata.
    fn stored_config_map(zookeeper: &str) -> ConfigMap {
        ConfigMap {
            metadata: ObjectMeta {
                name: Some("simple-zookeeper".to_owned()),
                namespace: Some("default".to_owned()),
                resource_version: Some("42".to_owned()),
                uid: Some("0b5c7b8e-0000-0000-0000-000000000000".to_owned()),
                managed_fields: Some(vec![]),
                owner_references: Some(vec![OwnerReference {
                    api_version: "zookeeper.stackable.tech/v1alpha1".to_owned(),
                    kind: "ZookeeperCluster".to_owned(),
                    name: "simple-zookeeper".to_owned(),
                    uid: CLUSTER_UID.to_owned(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..ObjectMeta::default()
            },
            data: Some(
                [
                    ("ZOOKEEPER".to_owned(), zookeeper.to_owned()),
                    ("ZOOKEEPER_HOSTS".to_owned(), zookeeper.to_owned()),
                    ("ZOOKEEPER_CLIENT_PORT".to_owned(), "2282".to_owned()),
                    ("ZOOKEEPER_CHROOT".to_owned(), "/".to_owned()),
                ]
                .into(),
            ),
            ..ConfigMap::default()
        }
    }

    /// The cluster controller advertises the whole ensemble, rooted at `/`. The fixture keeps TLS
    /// enabled, so the client port is the secure one.
    #[test]
    fn cluster_discovery_config_map_advertises_the_published_addresses() {
        let cluster = try_validate_with_role_listener(
            &minimal_zk(ZK_YAML),
            Some(listener_with(&[("node-0", 2282), ("node-1", 2282)])),
            None,
        )
        .expect("validate should succeed for the test fixture");

        let config_map = build_discovery_configmap(&cluster, &ZookeeperRole::Server)
            .expect("build")
            .expect("a fresh ConfigMap is built when addresses exist");

        assert_eq!(
            config_map.metadata.name.as_deref(),
            Some("simple-zookeeper")
        );
        assert_eq!(config_map.metadata.namespace.as_deref(), Some("default"));
        assert_eq!(data(&config_map, "ZOOKEEPER"), "node-0:2282,node-1:2282");
        assert_eq!(
            data(&config_map, "ZOOKEEPER_HOSTS"),
            "node-0:2282,node-1:2282"
        );
        assert_eq!(data(&config_map, "ZOOKEEPER_CLIENT_PORT"), "2282");
        assert_eq!(data(&config_map, "ZOOKEEPER_CHROOT"), "/");
    }

    /// The znode controller advertises the same ensemble, narrowed to the znode's chroot. Only
    /// `ZOOKEEPER` carries the chroot, because some clients cannot parse the merged format.
    #[test]
    fn znode_discovery_config_map_narrows_the_ensemble_to_the_chroot() {
        let znode = validated_znode(&minimal_znode(ZNODE_YAML));

        let config_map =
            build_znode_discovery_configmap(&znode, &znode.discovery_addresses, ZNODE_PATH)
                .expect("build");

        // The ConfigMap is named after the znode, not after the referenced cluster.
        assert_eq!(config_map.metadata.name.as_deref(), Some("simple-znode"));
        assert_eq!(config_map.metadata.namespace.as_deref(), Some("default"));
        assert_eq!(
            data(&config_map, "ZOOKEEPER"),
            format!("node-0:2282{ZNODE_PATH}")
        );
        assert_eq!(data(&config_map, "ZOOKEEPER_HOSTS"), "node-0:2282");
        assert_eq!(data(&config_map, "ZOOKEEPER_CLIENT_PORT"), "2282");
        assert_eq!(data(&config_map, "ZOOKEEPER_CHROOT"), ZNODE_PATH);
    }

    /// A relative chroot would silently produce a connection string pointing at the ensemble root.
    #[test]
    fn relative_chroot_is_rejected() {
        let znode = validated_znode(&minimal_znode(ZNODE_YAML));

        assert!(matches!(
            build_znode_discovery_configmap(
                &znode,
                &znode.discovery_addresses,
                "znode-without-a-leading-slash",
            ),
            Err(Error::RelativeChroot { .. })
        ));
    }

    /// The znode discovery ConfigMap's labels are hand-composed (see [`znode_discovery_labels`]),
    /// so lock the whole set: `instance` is the znode name, the rest matches the role-level
    /// recommended labels of the cluster controller's discovery ConfigMap.
    #[test]
    fn znode_discovery_config_map_carries_the_expected_labels() {
        let znode = validated_znode(&minimal_znode(ZNODE_YAML));

        let config_map =
            build_znode_discovery_configmap(&znode, &znode.discovery_addresses, ZNODE_PATH)
                .expect("build");

        let expected_labels = BTreeMap::from(
            [
                ("app.kubernetes.io/component", "server".to_owned()),
                ("app.kubernetes.io/instance", "simple-znode".to_owned()),
                (
                    "app.kubernetes.io/managed-by",
                    "zookeeper.stackable.tech_znode".to_owned(),
                ),
                ("app.kubernetes.io/name", "zookeeper".to_owned()),
                ("app.kubernetes.io/version", app_version_label("3.9.6")),
                ("stackable.tech/vendor", "Stackable".to_owned()),
            ]
            .map(|(key, value)| (key.to_owned(), value)),
        );
        assert_eq!(config_map.metadata.labels, Some(expected_labels));
    }

    /// Before the first successful build there is nothing to re-emit, so no ConfigMap is built.
    #[test]
    fn skips_cluster_discovery_config_map_without_addresses_and_without_stored_one() {
        let cluster = try_validate_with_role_listener(&minimal_zk(ZK_YAML), None, None)
            .expect("validate should succeed for the test fixture");

        let config_map =
            build_discovery_configmap(&cluster, &ZookeeperRole::Server).expect("build");

        assert!(config_map.is_none());
    }

    /// While the Listener has no usable address, the stored ConfigMap is re-emitted unchanged, with
    /// fresh metadata that carries no server-populated fields.
    #[test]
    fn reemits_stored_cluster_discovery_config_map_without_addresses() {
        let stored = stored_config_map("node-0:2282");
        let cluster =
            try_validate_with_role_listener(&minimal_zk(ZK_YAML), None, Some(stored.clone()))
                .expect("validate should succeed for the test fixture");

        let config_map = build_discovery_configmap(&cluster, &ZookeeperRole::Server)
            .expect("build")
            .expect("the stored ConfigMap is re-emitted");

        assert_eq!(config_map.data, stored.data);
        assert_eq!(config_map.metadata.resource_version, None);
        assert_eq!(config_map.metadata.uid, None);
        assert_eq!(config_map.metadata.managed_fields, None);

        // Same metadata as a freshly built ConfigMap for this cluster.
        let fresh_cluster = try_validate_with_role_listener(
            &minimal_zk(ZK_YAML),
            Some(listener_with(&[("node-0", 2282)])),
            None,
        )
        .expect("validate should succeed for the test fixture");
        let fresh = build_discovery_configmap(&fresh_cluster, &ZookeeperRole::Server)
            .expect("build")
            .expect("fresh");
        assert_eq!(config_map.metadata, fresh.metadata);
    }

    /// Once addresses exist again, they replace whatever was stored.
    #[test]
    fn fresh_addresses_replace_stale_stored_data() {
        let cluster = try_validate_with_role_listener(
            &minimal_zk(ZK_YAML),
            Some(listener_with(&[("node-0", 2282)])),
            Some(stored_config_map("old:2282")),
        )
        .expect("validate should succeed for the test fixture");

        let config_map = build_discovery_configmap(&cluster, &ZookeeperRole::Server)
            .expect("build")
            .expect("fresh");

        assert_eq!(data(&config_map, "ZOOKEEPER"), "node-0:2282");
    }

    /// The dereference step fetches the stored ConfigMap by [`discovery_config_map_name`], so it
    /// must be the name the build step writes; otherwise nothing would ever be re-emitted.
    #[test]
    fn discovery_config_map_name_matches_the_built_config_map() {
        let cluster = try_validate_with_role_listener(
            &minimal_zk(ZK_YAML),
            Some(listener_with(&[("node-0", 2282)])),
            None,
        )
        .expect("validate should succeed for the test fixture");

        let config_map = build_discovery_configmap(&cluster, &ZookeeperRole::Server)
            .expect("build")
            .expect("fresh");

        assert_eq!(
            config_map.metadata.name.as_deref(),
            Some(discovery_config_map_name(&cluster.name).as_ref())
        );
    }

    /// [`DISCOVERY_CONFIG_MAP_KEYS`] must list exactly the keys a fresh build writes, or the
    /// re-emit would drop or keep the wrong ones.
    #[test]
    fn discovery_config_map_keys_match_a_fresh_build() {
        let cluster = try_validate_with_role_listener(
            &minimal_zk(ZK_YAML),
            Some(listener_with(&[("node-0", 2282)])),
            None,
        )
        .expect("validate should succeed for the test fixture");

        let config_map = build_discovery_configmap(&cluster, &ZookeeperRole::Server)
            .expect("build")
            .expect("fresh");

        let mut expected_keys = DISCOVERY_CONFIG_MAP_KEYS.to_vec();
        expected_keys.sort_unstable();
        assert_eq!(
            config_map
                .data
                .expect("data")
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            expected_keys
        );
    }

    /// Keys that the operator does not write (e.g. added by a since removed objectOverride) are
    /// not re-emitted.
    #[test]
    fn reemit_keeps_only_the_discovery_keys() {
        let mut stored = stored_config_map("node-0:2282");
        stored
            .data
            .as_mut()
            .expect("data")
            .insert("EXTRA".to_owned(), "x".to_owned());
        let cluster = try_validate_with_role_listener(&minimal_zk(ZK_YAML), None, Some(stored))
            .expect("validate should succeed for the test fixture");

        let config_map = build_discovery_configmap(&cluster, &ZookeeperRole::Server)
            .expect("build")
            .expect("the stored ConfigMap is re-emitted");

        assert_eq!(config_map.data, stored_config_map("node-0:2282").data);
    }

    /// While reconciliation is paused, the apply step only fetches resources. A ConfigMap that was
    /// never stored is therefore not built, even once addresses exist, or every paused run would
    /// fail fetching it.
    #[test]
    fn skips_cluster_discovery_config_map_while_paused_and_none_is_stored() {
        let cluster = try_validate_with_role_listener(
            &minimal_zk(PAUSED_ZK_YAML),
            Some(listener_with(&[("node-0", 2282)])),
            None,
        )
        .expect("validate should succeed for the test fixture");

        let config_map =
            build_discovery_configmap(&cluster, &ZookeeperRole::Server).expect("build");

        assert!(config_map.is_none());
    }

    /// While reconciliation is paused, a stored ConfigMap still goes through the apply step, which
    /// fetches it, so the status keeps its hash.
    #[test]
    fn builds_cluster_discovery_config_map_while_paused_when_one_is_stored() {
        let cluster = try_validate_with_role_listener(
            &minimal_zk(PAUSED_ZK_YAML),
            Some(listener_with(&[("node-0", 2282)])),
            Some(stored_config_map("node-0:2282")),
        )
        .expect("validate should succeed for the test fixture");

        let config_map =
            build_discovery_configmap(&cluster, &ZookeeperRole::Server).expect("build");

        assert!(config_map.is_some());
    }
}
