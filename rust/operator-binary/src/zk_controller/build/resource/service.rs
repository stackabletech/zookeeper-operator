use stackable_operator::{
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    v2::{
        builder::service::{Scheme, Scraping, prometheus_annotations, prometheus_labels},
        types::operator::RoleGroupName,
    },
};

use crate::{
    crd::{
        JMX_METRICS_PORT, JMX_METRICS_PORT_NAME, METRICS_PROVIDER_HTTP_PORT_NAME,
        ZOOKEEPER_ELECTION_PORT, ZOOKEEPER_ELECTION_PORT_NAME, ZOOKEEPER_LEADER_PORT,
        ZOOKEEPER_LEADER_PORT_NAME,
    },
    zk_controller::validate::{ValidatedCluster, ZookeeperRoleGroupConfig},
};

/// The rolegroup [`Service`] is a headless service that allows internal access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
pub(crate) fn build_server_rolegroup_headless_service(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
) -> Service {
    let metadata = cluster
        .object_meta(
            cluster
                .role_group_resource_names(role_group_name)
                .headless_service_name()
                .to_string(),
            role_group_name,
        )
        .build();

    let service_spec = ServiceSpec {
        // Internal communication does not need to be exposed
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        ports: Some(vec![
            ServicePort {
                name: Some(ZOOKEEPER_LEADER_PORT_NAME.to_string()),
                port: ZOOKEEPER_LEADER_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            },
            ServicePort {
                name: Some(ZOOKEEPER_ELECTION_PORT_NAME.to_string()),
                port: ZOOKEEPER_ELECTION_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            },
        ]),
        selector: Some(cluster.role_group_selector(role_group_name).into()),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Service {
        metadata,
        spec: Some(service_spec),
        status: None,
    }
}

/// The rolegroup [`Service`] for exposing metrics
pub(crate) fn build_server_rolegroup_metrics_service(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    rolegroup_config: &ZookeeperRoleGroupConfig,
) -> Service {
    let metrics_port = cluster.metrics_http_port(rolegroup_config);
    let metadata = cluster
        .object_meta(
            cluster
                .role_group_resource_names(role_group_name)
                .metrics_service_name(),
            role_group_name,
        )
        .with_labels(prometheus_labels(&Scraping::Enabled))
        .with_annotations(prometheus_annotations(
            &Scraping::Enabled,
            &Scheme::Http,
            "/metrics",
            &metrics_port,
        ))
        .build();

    let service_spec = ServiceSpec {
        // Internal communication does not need to be exposed
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        ports: Some(vec![
            // We keep this for legacy compatibility
            ServicePort {
                name: Some(JMX_METRICS_PORT_NAME.to_string()),
                port: JMX_METRICS_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            },
            ServicePort {
                name: Some(METRICS_PROVIDER_HTTP_PORT_NAME.to_string()),
                port: metrics_port.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            },
        ]),
        selector: Some(cluster.role_group_selector(role_group_name).into()),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Service {
        metadata,
        spec: Some(service_spec),
        status: None,
    }
}
