use std::{collections::BTreeSet, num::TryFromIntError};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    commons::product_image_selection::ResolvedProductImage,
    crd::listener,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{Resource, ResourceExt, runtime::reflector::ObjectRef},
};

use crate::{
    crd::{ZookeeperRole, security::ZookeeperSecurity, v1alpha1},
    utils::build_recommended_labels,
};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object {} is missing metadata to build owner reference", zk))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        zk: ObjectRef<v1alpha1::ZookeeperCluster>,
    },

    #[snafu(display("chroot path {} was relative (must be absolute)", chroot))]
    RelativeChroot { chroot: String },

    #[snafu(display("object has no namespace associated"))]
    NoNamespace,

    #[snafu(display("failed to list expected pods"))]
    ExpectedPods { source: crate::crd::Error },

    #[snafu(display("{listener} does not have a port with the name {port_name:?}"))]
    PortNotFound {
        port_name: String,
        listener: ObjectRef<listener::v1alpha1::Listener>,
    },

    #[snafu(display("expected an unsigned 16-bit port, got {port_number}"))]
    InvalidPort {
        source: TryFromIntError,
        port_number: i32,
    },

    #[snafu(display("{listener} has no ingress addresses"))]
    NoListenerIngressAddresses {
        listener: ObjectRef<listener::v1alpha1::Listener>,
    },

    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },
}

/// Build a discovery [`ConfigMap`] containing connection details for a [`v1alpha1::ZookeeperCluster`] from a [`listener::v1alpha1::Listener`].
///
/// `hosts` will usually come from either [`pod_hosts`] or [`nodeport_hosts`].
#[allow(clippy::too_many_arguments)]
pub fn build_discovery_configmap(
    zk: &v1alpha1::ZookeeperCluster,
    owner: &impl Resource<DynamicType = ()>,
    controller_name: &str,
    listener: listener::v1alpha1::Listener,
    chroot: Option<&str>,
    resolved_product_image: &ResolvedProductImage,
    zookeeper_security: &ZookeeperSecurity,
) -> Result<ConfigMap> {
    let name = owner.name_unchecked();
    let namespace = owner.namespace().context(NoNamespaceSnafu)?;

    let listener_addresses = listener_addresses(&listener, "zk")?;

    // Write a connection string of the format that Java ZooKeeper client expects:
    // "{host1}:{port1},{host2:port2},.../{chroot}"
    // See https://zookeeper.apache.org/doc/current/apidocs/zookeeper-server/org/apache/zookeeper/ZooKeeper.html#ZooKeeper-java.lang.String-int-org.apache.zookeeper.Watcher-
    let listener_addresses = listener_addresses
        .into_iter()
        .map(|(host, port)| format!("{host}:{port}"))
        .collect::<Vec<_>>()
        .join(",");
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
                .ownerreference_from_resource(owner, None, Some(true))
                .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
                    zk: ObjectRef::from_obj(zk),
                })?
                .with_recommended_labels(build_recommended_labels(
                    owner,
                    controller_name,
                    &resolved_product_image.app_version_label,
                    &ZookeeperRole::Server.to_string(),
                    "discovery",
                ))
                .context(ObjectMetaSnafu)?
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

// /// Lists all Pods FQDNs expected to host the [`v1alpha1::ZookeeperCluster`]
// fn pod_hosts<'a>(
//     zk: &'a v1alpha1::ZookeeperCluster,
//     zookeeper_security: &'a ZookeeperSecurity,
//     cluster_info: &'a KubernetesClusterInfo,
// ) -> Result<impl IntoIterator<Item = (String, u16)> + 'a> {
//     Ok(zk
//         .pods()
//         .context(ExpectedPodsSnafu)?
//         .map(|pod_ref| (pod_ref.fqdn(cluster_info), zookeeper_security.client_port())))
// }

/// Lists all listener address and port number pairs for a given `port_name` for Pods participating in the [`Listener`][1]
///
/// This returns pairs of `(Address, Port)`, where address could be a hostname or IP address of a node, clusterIP or external
/// load balancer depending on the Service type.
///
/// ## Errors
///
/// An error will be returned if there is no address found for the `port_name`.
///
/// [1]: listener::v1alpha1::Listener
// TODO (@NickLarsenNZ): Move this to stackable-operator, so it can be used as listener.addresses_for_port(port_name)
fn listener_addresses(
    listener: &listener::v1alpha1::Listener,
    port_name: &str,
) -> Result<impl IntoIterator<Item = (String, u16)>> {
    // Get addresses port pairs for addresses that have a port with the name that matches the one we are interested in
    let address_port_pairs = listener
        .status
        .as_ref()
        .and_then(|listener_status| listener_status.ingress_addresses.as_ref())
        .context(NoListenerIngressAddressesSnafu { listener })?
        .iter()
        // Filter the addresses that have the port we are interested in (they likely all have it though)
        .filter_map(|listener_ingress| {
            Some(listener_ingress.address.clone()).zip(listener_ingress.ports.get(port_name))
        })
        // Convert the port from i32 to u16
        .map(|(listener_address, &port_number)| {
            let port_number: u16 = port_number
                .try_into()
                .context(InvalidPortSnafu { port_number })?;
            Ok((listener_address, port_number))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;

    // An empty list is considered an error
    match address_port_pairs.is_empty() {
        true => PortNotFoundSnafu {
            port_name,
            listener,
        }
        .fail(),
        false => Ok(address_port_pairs),
    }
}

// TODO (@NickLarsenNZ): Implement this directly on RoleGroupRef, ie:
// RoleGroupRef<K: Resource>::metrics_service_name(&self) to restrict what _name_ can be.
pub fn build_role_group_headless_service_name(name: String) -> String {
    format!("{name}-headless")
}

// TODO (@NickLarsenNZ): Implement this directly on RoleGroupRef, ie:
// RoleGroupRef<K: Resource>::metrics_service_name(&self) to restrict what _name_ can be.
pub fn build_role_group_metrics_service_name(name: String) -> String {
    format!("{name}-metrics")
}
