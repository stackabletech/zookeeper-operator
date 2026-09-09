//! Builders for the discovery ConfigMaps, which advertise how to connect to a ZooKeeper ensemble.
//!
//! Shared by the build steps of both controllers: the ZookeeperCluster controller publishes the
//! whole ensemble, the ZookeeperZnode controller publishes the same ensemble narrowed to a chroot.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    kube::Resource,
    kvp::{Label, Labels},
    v2::{
        HasName, HasUid, NameIsValidLabelValue, builder::meta::ownerreference_from_resource,
        kvp::label,
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

/// Build the discovery [`ConfigMap`] for the cluster controller from the
/// [`ValidatedCluster`].
///
/// The ConfigMap is owned by, and placed in the namespace of, the cluster. The image and security
/// settings are taken from the [`ValidatedCluster`] rather than being passed in separately.
///
/// The connection details are read from the addresses published by the role Listener (carried on
/// [`ValidatedCluster::discovery_addresses`](ValidatedCluster#structfield.discovery_addresses),
/// fetched in the dereference step), which only the listener operator writes. While no address
/// exists around the first reconciliations, which create the Listener in the first place, the
/// ConfigMap is still written, with an empty `ZOOKEEPER` value: omitting it instead would let the
/// apply step delete an existing discovery ConfigMap as an orphan, breaking consumers that mount
/// it. The Listener watch triggers a new run that fills in the value once the addresses are
/// published.
pub fn build_discovery_configmap(
    validated_cluster: &ValidatedCluster,
    zk_role: &ZookeeperRole,
    listener_addresses: &ListenerAddresses,
) -> Result<ConfigMap> {
    build_discovery_configmap_for_owner(
        validated_cluster,
        &validated_cluster.namespace,
        recommended_labels_for_role_resources(validated_cluster, zk_role),
        listener_addresses,
        None,
        &validated_cluster.cluster_config.zookeeper_security,
    )
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
/// [`ClusterName`](stackable_operator::v2::types::operator::ClusterName). The label set matches
/// the role-level recommended labels of the cluster controller's discovery ConfigMap otherwise.
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
    let name = owner.to_name();

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
        .metadata(
            ObjectMetaBuilder::new()
                .name(name)
                .namespace(namespace)
                .ownerreference(ownerreference_from_resource(owner, None, Some(true)))
                .with_labels(labels)
                .build(),
        )
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{
        crd::ZOOKEEPER_SERVER_PORT_NAME,
        listener_addresses::{
            listener_addresses,
            test_support::{ingress_address, role_listener},
        },
        zk_controller::test_support::{
            app_version_label, minimal_zk, try_validate_with_role_listener,
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
            productVersion: "3.9.5"
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

    /// Addresses published under the ZooKeeper server port name, built through the real reader so
    /// the fixtures cannot drift from what the validate step produces.
    fn published_addresses(addresses: &[(&str, u16)]) -> ListenerAddresses {
        let listener = role_listener(Some(
            addresses
                .iter()
                .map(|(address, port)| {
                    ingress_address(address, ZOOKEEPER_SERVER_PORT_NAME, i32::from(*port))
                })
                .collect(),
        ));

        listener_addresses(&listener, ZOOKEEPER_SERVER_PORT_NAME)
            .expect("the fixture publishes addresses under the server port name")
            .expect("the fixture publishes addresses")
    }

    /// The cluster controller advertises the whole ensemble, rooted at `/`. The fixture keeps TLS
    /// enabled, so the client port is the secure one.
    #[test]
    fn cluster_discovery_config_map_advertises_the_published_addresses() {
        let cluster = try_validate_with_role_listener(&minimal_zk(ZK_YAML), None)
            .expect("validate should succeed for the test fixture");
        let addresses = published_addresses(&[("node-0", 2282), ("node-1", 2282)]);

        let config_map =
            build_discovery_configmap(&cluster, &ZookeeperRole::Server, &addresses).expect("build");

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

    /// While the role Listener publishes no addresses the ConfigMap is still written, with an
    /// empty connection string, so that the apply step does not delete the published one as an
    /// orphan.
    #[test]
    fn cluster_discovery_config_map_without_addresses_is_still_written() {
        let cluster = try_validate_with_role_listener(&minimal_zk(ZK_YAML), None)
            .expect("validate should succeed for the test fixture");

        let config_map = build_discovery_configmap(
            &cluster,
            &ZookeeperRole::Server,
            &ListenerAddresses::default(),
        )
        .expect("build");

        assert_eq!(
            config_map.metadata.name.as_deref(),
            Some("simple-zookeeper")
        );
        assert_eq!(data(&config_map, "ZOOKEEPER"), "");
        assert_eq!(data(&config_map, "ZOOKEEPER_HOSTS"), "");
        // The port and chroot do not depend on the addresses, so they stay populated.
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
                ("app.kubernetes.io/version", app_version_label("3.9.5")),
                ("stackable.tech/vendor", "Stackable".to_owned()),
            ]
            .map(|(key, value)| (key.to_owned(), value)),
        );
        assert_eq!(config_map.metadata.labels, Some(expected_labels));
    }
}
