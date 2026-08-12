//! Reading client connection addresses from a [`Listener`](listener::v1alpha1::Listener).
//!
//! Shared by both controllers, which turn the dereferenced ZooKeeper role Listener into the
//! addresses advertised by their discovery ConfigMaps.

use std::{collections::BTreeSet, num::TryFromIntError};

use snafu::{ResultExt, Snafu};
use stackable_operator::{crd::listener, kube::runtime::reflector::ObjectRef};

#[derive(Snafu, Debug)]
pub enum Error {
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
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The address and port pairs published by a [`Listener`](listener::v1alpha1::Listener) for a
/// single named port, sorted and deduplicated.
///
/// An address is a hostname or IP address of a node, a cluster IP or an external load balancer,
/// depending on the Service type behind the Listener.
///
/// The [`Default`] value is empty, which renders as an empty connection string, see
/// [`build_discovery_configmap`](crate::discovery::build_discovery_configmap).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListenerAddresses(BTreeSet<(String, u16)>);

impl ListenerAddresses {
    /// Renders the addresses as the comma separated `host1:port1,host2:port2` list that ZooKeeper
    /// clients expect.
    pub fn to_connection_string(&self) -> String {
        self.0
            .iter()
            .map(|(address, port)| format!("{address}:{port}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Reads the addresses that `listener` publishes for `port_name`.
///
/// Returns `Ok(None)` while the Listener carries no ingress addresses at all, which is the normal
/// state between creating the Listener and the listener operator publishing its addresses. A
/// Listener that does publish addresses, but none for `port_name`, is an error.
// TODO (@NickLarsenNZ): Move this to stackable-operator, so it can be used as
// listener.addresses_for_port(port_name)
pub fn listener_addresses(
    listener: &listener::v1alpha1::Listener,
    port_name: &str,
) -> Result<Option<ListenerAddresses>> {
    let Some(ingress_addresses) = listener
        .status
        .as_ref()
        .and_then(|listener_status| listener_status.ingress_addresses.as_ref())
    else {
        return Ok(None);
    };

    let address_port_pairs = ingress_addresses
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

    match address_port_pairs.is_empty() {
        true => PortNotFoundSnafu {
            port_name,
            listener,
        }
        .fail(),
        false => Ok(Some(ListenerAddresses(address_port_pairs))),
    }
}

/// Shared helpers for building role [`Listener`](listener::v1alpha1::Listener) fixtures.
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::BTreeMap;

    use stackable_operator::{
        crd::listener::v1alpha1::{
            AddressType, Listener, ListenerIngress, ListenerSpec, ListenerStatus,
        },
        k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };

    /// A role Listener publishing `ingress_addresses`. `None` is a Listener that the listener
    /// operator has not written a status for yet.
    pub fn role_listener(ingress_addresses: Option<Vec<ListenerIngress>>) -> Listener {
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

    /// A single ingress address of a role Listener, publishing `port` under `port_name`.
    pub fn ingress_address(address: &str, port_name: &str, port: i32) -> ListenerIngress {
        ListenerIngress {
            address: address.to_owned(),
            address_type: AddressType::Hostname,
            ports: BTreeMap::from([(port_name.to_owned(), port)]),
        }
    }
}

#[cfg(test)]
mod tests {
    use stackable_operator::crd::listener::v1alpha1::ListenerIngress;

    use super::{
        test_support::{ingress_address, role_listener},
        *,
    };
    use crate::crd::ZOOKEEPER_SERVER_PORT_NAME;

    fn ingress(port: i32) -> ListenerIngress {
        ingress_address("node-0", ZOOKEEPER_SERVER_PORT_NAME, port)
    }

    #[test]
    fn listener_addresses_returns_host_port_pairs() {
        let listener = role_listener(Some(vec![ingress(2181)]));
        let addresses = listener_addresses(&listener, ZOOKEEPER_SERVER_PORT_NAME)
            .expect("addresses")
            .expect("the listener publishes addresses");
        assert_eq!(addresses.to_connection_string(), "node-0:2181");
    }

    #[test]
    fn listener_addresses_without_ingress_is_not_ready_yet() {
        assert_eq!(
            listener_addresses(&role_listener(None), ZOOKEEPER_SERVER_PORT_NAME)
                .expect("addresses"),
            None
        );
    }

    #[test]
    fn listener_addresses_missing_port_name_is_error() {
        let listener = role_listener(Some(vec![ingress(2181)]));
        assert!(matches!(
            listener_addresses(&listener, "does-not-exist"),
            Err(Error::PortNotFound { .. })
        ));
    }

    #[test]
    fn listener_addresses_port_out_of_u16_range_is_error() {
        // A port number that does not fit into a u16 must be rejected.
        let listener = role_listener(Some(vec![ingress(70_000)]));
        assert!(matches!(
            listener_addresses(&listener, ZOOKEEPER_SERVER_PORT_NAME),
            Err(Error::InvalidPort { .. })
        ));
    }
}
