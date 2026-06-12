//! Types and functions for exposing product endpoints via [listener::v1alpha1::Listener].

use std::str::FromStr;

use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    crd::listener,
    v2::{builder::meta::ownerreference_from_resource, types::operator::RoleGroupName},
};

use crate::{
    crd::{ZOOKEEPER_SERVER_PORT_NAME, ZookeeperRole, security::ZookeeperSecurity},
    zk_controller::validate::ValidatedCluster,
};

/// Builds the role-level [`Listener`](listener::v1alpha1::Listener) exposing the ZooKeeper servers.
///
/// The listener is owned by, labelled and named from the [`ValidatedCluster`]; the ListenerClass
/// and security settings are taken from its validated cluster config.
pub fn build_role_listener(
    cluster: &ValidatedCluster,
    zk_role: &ZookeeperRole,
) -> listener::v1alpha1::Listener {
    // The listener is a role-level resource, so it has no role group. The recommended labels
    // require a role-group value, so a constant "none" is used (matching the previous behaviour).
    let role_group_name =
        RoleGroupName::from_str("none").expect("'none' is a valid role group name");

    listener::v1alpha1::Listener {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(role_listener_name(cluster.name.as_ref(), zk_role))
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            .with_labels(cluster.recommended_labels(&role_group_name))
            .build(),
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(cluster.cluster_config.listener_class.clone()),
            ports: Some(listener_ports(&cluster.cluster_config.zookeeper_security)),
            ..listener::v1alpha1::ListenerSpec::default()
        },
        status: None,
    }
}

pub fn role_listener_name(cluster_name: &str, zk_role: &ZookeeperRole) -> String {
    format!("{cluster_name}-{zk_role}")
}

// We only use the server port here and intentionally omit the metrics one.
fn listener_ports(zookeeper_security: &ZookeeperSecurity) -> Vec<listener::v1alpha1::ListenerPort> {
    vec![listener::v1alpha1::ListenerPort {
        name: ZOOKEEPER_SERVER_PORT_NAME.to_string(),
        port: zookeeper_security.client_port().into(),
        protocol: Some("TCP".to_string()),
    }]
}
