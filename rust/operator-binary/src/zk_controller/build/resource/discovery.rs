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
        types::operator::{ControllerName, ProductVersion},
    },
};

use crate::{
    crd::{ZookeeperRole, security::ZookeeperSecurity},
    listener_addresses::ListenerAddresses,
    zk_controller::{
        build::PLACEHOLDER_DISCOVERY_ROLE_GROUP,
        validate::{ValidatedCluster, operator_name, product_name},
    },
    znode_controller::validate::ValidatedZnode,
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

/// Build a discovery [`ConfigMap`] containing ZooKeeper connection details from a
/// [`listener::v1alpha1::Listener`].
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
