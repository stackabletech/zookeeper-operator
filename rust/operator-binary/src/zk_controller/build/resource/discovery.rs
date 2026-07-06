use std::{collections::BTreeSet, num::TryFromIntError, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    crd::listener,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{Resource, runtime::reflector::ObjectRef},
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::meta::ownerreference_from_resource,
        kvp::label::recommended_labels,
        types::operator::{ControllerName, ProductVersion},
    },
};

use crate::{
    crd::{ZOOKEEPER_SERVER_PORT_NAME, security::ZookeeperSecurity},
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
}

/// Build the discovery [`ConfigMap`] for the cluster controller from the
/// [`ValidatedCluster`].
///
/// The ConfigMap is owned by, and placed in the namespace of, the cluster. The image and security
/// settings are taken from the [`ValidatedCluster`] rather than being passed in separately.
pub fn build_discovery_configmap(
    validated_cluster: &ValidatedCluster,
    controller_name: &str,
    listener: listener::v1alpha1::Listener,
) -> Result<ConfigMap> {
    build_discovery_configmap_for_owner(
        validated_cluster,
        &validated_cluster.namespace,
        controller_name,
        &validated_cluster.product_version,
        listener,
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
    listener: listener::v1alpha1::Listener,
    chroot: &str,
) -> Result<ConfigMap> {
    build_discovery_configmap_for_owner(
        validated_znode,
        &validated_znode.namespace,
        controller_name,
        &validated_znode.product_version,
        listener,
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
    listener: listener::v1alpha1::Listener,
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

    let listener_addresses = listener_addresses(&listener, ZOOKEEPER_SERVER_PORT_NAME)?;

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
                .ownerreference(ownerreference_from_resource(owner, None, Some(true)))
                .with_labels(recommended_labels(
                    owner,
                    &product_name(),
                    product_version,
                    &operator_name(),
                    &controller_name,
                    &ValidatedCluster::role_name(),
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
) -> Result<impl IntoIterator<Item = (String, u16)> + use<>> {
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use stackable_operator::{
        crd::listener::v1alpha1::{
            AddressType, Listener, ListenerIngress, ListenerSpec, ListenerStatus,
        },
        k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };

    use super::*;

    fn listener(ingress_addresses: Option<Vec<ListenerIngress>>) -> Listener {
        Listener {
            metadata: ObjectMeta {
                name: Some("test-listener".to_owned()),
                ..ObjectMeta::default()
            },
            spec: ListenerSpec::default(),
            status: Some(ListenerStatus {
                service_name: None,
                ingress_addresses,
                node_ports: None,
            }),
        }
    }

    fn ingress(port: i32) -> ListenerIngress {
        ListenerIngress {
            address: "node-0".to_owned(),
            address_type: AddressType::Hostname,
            ports: BTreeMap::from([(ZOOKEEPER_SERVER_PORT_NAME.to_owned(), port)]),
        }
    }

    #[test]
    fn listener_addresses_returns_host_port_pairs() {
        let listener = listener(Some(vec![ingress(2181)]));
        let pairs: Vec<_> = listener_addresses(&listener, ZOOKEEPER_SERVER_PORT_NAME)
            .expect("addresses")
            .into_iter()
            .collect();
        assert_eq!(pairs, vec![("node-0".to_owned(), 2181u16)]);
    }

    #[test]
    fn listener_addresses_without_ingress_is_error() {
        assert!(matches!(
            listener_addresses(&listener(None), ZOOKEEPER_SERVER_PORT_NAME),
            Err(Error::NoListenerIngressAddresses { .. })
        ));
    }

    #[test]
    fn listener_addresses_missing_port_name_is_error() {
        let listener = listener(Some(vec![ingress(2181)]));
        assert!(matches!(
            listener_addresses(&listener, "does-not-exist"),
            Err(Error::PortNotFound { .. })
        ));
    }

    #[test]
    fn listener_addresses_port_out_of_u16_range_is_error() {
        // A port number that does not fit into a u16 must be rejected.
        let listener = listener(Some(vec![ingress(70_000)]));
        assert!(matches!(
            listener_addresses(&listener, ZOOKEEPER_SERVER_PORT_NAME),
            Err(Error::InvalidPort { .. })
        ));
    }
}
