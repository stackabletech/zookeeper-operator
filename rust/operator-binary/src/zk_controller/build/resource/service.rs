use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    kvp::{Annotations, Labels},
    v2::{builder::meta::ownerreference_from_resource, types::operator::RoleGroupName},
};

use crate::{
    crd::{
        JMX_METRICS_PORT, JMX_METRICS_PORT_NAME, METRICS_PROVIDER_HTTP_PORT_NAME,
        ZOOKEEPER_ELECTION_PORT, ZOOKEEPER_ELECTION_PORT_NAME, ZOOKEEPER_LEADER_PORT,
        ZOOKEEPER_LEADER_PORT_NAME,
    },
    zk_controller::validate::{ValidatedCluster, ValidatedRoleGroupConfig},
};

/// The rolegroup [`Service`] is a headless service that allows internal access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
pub(crate) fn build_server_rolegroup_headless_service(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
) -> Service {
    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(cluster)
        .name(
            cluster
                .resource_names(role_group_name)
                .headless_service_name()
                .to_string(),
        )
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(cluster.recommended_labels(role_group_name))
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
    rolegroup_config: &ValidatedRoleGroupConfig,
) -> Service {
    let metrics_port = cluster.metrics_http_port(rolegroup_config);
    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(cluster)
        .name(
            cluster
                .resource_names(role_group_name)
                .metrics_service_name(),
        )
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(cluster.recommended_labels(role_group_name))
        .with_labels(prometheus_labels())
        .with_annotations(prometheus_annotations(metrics_port))
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

/// Common labels for Prometheus
fn prometheus_labels() -> Labels {
    Labels::try_from([("prometheus.io/scrape", "true")]).expect("should be a valid label")
}

/// Common annotations for Prometheus
///
/// These annotations can be used in a ServiceMonitor.
///
/// see also <https://github.com/prometheus-community/helm-charts/blob/prometheus-27.32.0/charts/prometheus/values.yaml#L983-L1036>
fn prometheus_annotations(metrics_port: u16) -> Annotations {
    Annotations::try_from([
        ("prometheus.io/path".to_owned(), "/metrics".to_owned()),
        ("prometheus.io/port".to_owned(), metrics_port.to_string()),
        ("prometheus.io/scheme".to_owned(), "http".to_owned()),
        ("prometheus.io/scrape".to_owned(), "true".to_owned()),
    ])
    .expect("should be valid annotations")
}
