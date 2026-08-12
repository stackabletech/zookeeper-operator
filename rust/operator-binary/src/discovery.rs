//! Builders for the discovery ConfigMaps, which advertise how to connect to a ZooKeeper ensemble.
//!
//! Shared by the build steps of both controllers: the ZookeeperCluster controller publishes the
//! whole ensemble, the ZookeeperZnode controller publishes the same ensemble narrowed to a chroot.

use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    kube::Resource,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::meta::ownerreference_from_resource,
        kvp::label::recommended_labels,
        types::operator::{ControllerName, ProductVersion, RoleGroupName},
    },
};

use crate::{
    crd::{ZookeeperRole, security::ZookeeperSecurity},
    listener_addresses::ListenerAddresses,
    zk_controller::validate::{ValidatedCluster, operator_name, product_name},
    znode_controller::validate::ValidatedZnode,
};

// Placeholder role-group name used for the recommended labels of the role-level discovery
// `ConfigMap` (which is not tied to a single role group).
stackable_operator::constant!(PLACEHOLDER_DISCOVERY_ROLE_GROUP: RoleGroupName = "discovery");

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
    controller_name: &str,
    listener_addresses: &ListenerAddresses,
) -> Result<ConfigMap> {
    build_discovery_configmap_for_owner(
        validated_cluster,
        &validated_cluster.namespace,
        controller_name,
        &validated_cluster.product_version,
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
    controller_name: &str,
    listener_addresses: &ListenerAddresses,
    chroot: &str,
) -> Result<ConfigMap> {
    build_discovery_configmap_for_owner(
        validated_znode,
        &validated_znode.namespace,
        controller_name,
        &validated_znode.product_version,
        listener_addresses,
        Some(chroot),
        &validated_znode.zookeeper_security,
    )
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
    controller_name: &str,
    product_version: &ProductVersion,
    listener_addresses: &ListenerAddresses,
    chroot: Option<&str>,
    zookeeper_security: &ZookeeperSecurity,
) -> Result<ConfigMap> {
    let name = owner.to_name();

    // The discovery ConfigMap is a role-level resource of the `server` role, conventionally
    // labelled with the `discovery` role group. The controller name differs between the cluster and
    // znode controllers, so it is passed in and validated into the type-safe newtype here.
    let controller_name = ControllerName::from_str(controller_name)
        .expect("the controller name is a valid label value");
    let role_group_name = PLACEHOLDER_DISCOVERY_ROLE_GROUP.clone();

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
                .with_labels(recommended_labels(
                    owner,
                    &product_name(),
                    product_version,
                    &operator_name(),
                    &controller_name,
                    &ZookeeperRole::Server.into(),
                    &role_group_name,
                ))
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
    use super::*;
    use crate::{
        crd::ZOOKEEPER_SERVER_PORT_NAME,
        listener_addresses::{
            listener_addresses,
            test_support::{ingress_address, role_listener},
        },
        zk_controller::{
            ZK_CONTROLLER_NAME,
            test_support::{minimal_zk, try_validate_with_role_listener},
        },
        znode_controller::{
            ZNODE_CONTROLLER_NAME,
            test_support::{minimal_znode, validated_znode},
        },
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
            build_discovery_configmap(&cluster, ZK_CONTROLLER_NAME, &addresses).expect("build");

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

        let config_map =
            build_discovery_configmap(&cluster, ZK_CONTROLLER_NAME, &ListenerAddresses::default())
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

        let config_map = build_znode_discovery_configmap(
            &znode,
            ZNODE_CONTROLLER_NAME,
            &znode.discovery_addresses,
            ZNODE_PATH,
        )
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
                ZNODE_CONTROLLER_NAME,
                &znode.discovery_addresses,
                "znode-without-a-leading-slash",
            ),
            Err(Error::RelativeChroot { .. })
        ));
    }
}
