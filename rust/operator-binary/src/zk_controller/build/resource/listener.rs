//! Types and functions for exposing product endpoints via [listener::v1alpha1::Listener].

use stackable_operator::crd::listener;

use crate::{
    crd::{
        ZOOKEEPER_SERVER_PORT_NAME, ZookeeperRole, role_listener_name, security::ZookeeperSecurity,
    },
    zk_controller::{build::PLACEHOLDER_LISTENER_ROLE_GROUP, validate::ValidatedCluster},
};

/// Builds the role-level [`Listener`](listener::v1alpha1::Listener) exposing the ZooKeeper servers.
///
/// The listener is owned by, labelled and named from the [`ValidatedCluster`]; the ListenerClass
/// and security settings are taken from its validated cluster config.
pub fn build_role_listener(
    cluster: &ValidatedCluster,
    zk_role: &ZookeeperRole,
) -> listener::v1alpha1::Listener {
    listener::v1alpha1::Listener {
        metadata: cluster
            .object_meta(
                role_listener_name(cluster.name.as_ref(), zk_role),
                &PLACEHOLDER_LISTENER_ROLE_GROUP,
            )
            .build(),
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(cluster.cluster_config.listener_class.to_string()),
            ports: Some(listener_ports(&cluster.cluster_config.zookeeper_security)),
            ..listener::v1alpha1::ListenerSpec::default()
        },
        status: None,
    }
}

// We only use the server port here and intentionally omit the metrics one.
fn listener_ports(zookeeper_security: &ZookeeperSecurity) -> Vec<listener::v1alpha1::ListenerPort> {
    vec![listener::v1alpha1::ListenerPort {
        name: ZOOKEEPER_SERVER_PORT_NAME.to_string(),
        port: zookeeper_security.client_port().into(),
        protocol: Some("TCP".to_string()),
    }]
}
