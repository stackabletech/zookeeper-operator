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
                .resource_names(role_group_name)
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
                .resource_names(role_group_name)
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::{
        crd::ZookeeperRole,
        zk_controller::test_support::{minimal_zk, validated_cluster},
    };

    const DEFAULT_ZK: &str = r#"
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

    fn port(service: &Service, name: &str) -> i32 {
        service
            .spec
            .as_ref()
            .unwrap()
            .ports
            .as_ref()
            .unwrap()
            .iter()
            .find(|p| p.name.as_deref() == Some(name))
            .unwrap_or_else(|| panic!("missing service port {name}"))
            .port
    }

    #[test]
    fn headless_service_shape() {
        let validated = validated_cluster(&minimal_zk(DEFAULT_ZK));
        let rg = RoleGroupName::from_str("default").expect("valid role group name");
        let service = build_server_rolegroup_headless_service(&validated, &rg);

        assert_eq!(
            service.metadata.name.as_deref(),
            Some("simple-zookeeper-server-default-headless")
        );

        let spec = service.spec.as_ref().unwrap();
        assert_eq!(spec.type_.as_deref(), Some("ClusterIP"));
        assert_eq!(spec.cluster_ip.as_deref(), Some("None"));
        assert_eq!(spec.publish_not_ready_addresses, Some(true));

        // Only the leader/election ports (independent of the client TLS matrix).
        assert_eq!(port(&service, ZOOKEEPER_LEADER_PORT_NAME), 2888);
        assert_eq!(port(&service, ZOOKEEPER_ELECTION_PORT_NAME), 3888);
        assert_eq!(spec.ports.as_ref().unwrap().len(), 2);

        let selector = spec.selector.as_ref().unwrap();
        assert_eq!(
            selector
                .get("app.kubernetes.io/component")
                .map(String::as_str),
            Some("server")
        );
        assert_eq!(
            selector
                .get("app.kubernetes.io/role-group")
                .map(String::as_str),
            Some("default")
        );
    }

    #[test]
    fn metrics_service_shape_and_prometheus_annotations() {
        let validated = validated_cluster(&minimal_zk(DEFAULT_ZK));
        let rg = RoleGroupName::from_str("default").expect("valid role group name");
        let rg_config = validated.role_group_configs[&ZookeeperRole::Server][&rg].clone();
        let service = build_server_rolegroup_metrics_service(&validated, &rg, &rg_config);

        assert_eq!(
            service.metadata.name.as_deref(),
            Some("simple-zookeeper-server-default-metrics")
        );

        // Prometheus scrape annotations.
        let annotations = service.metadata.annotations.as_ref().unwrap();
        assert_eq!(
            annotations.get("prometheus.io/path").map(String::as_str),
            Some("/metrics")
        );
        assert_eq!(
            annotations.get("prometheus.io/port").map(String::as_str),
            Some("7000")
        );
        assert_eq!(
            annotations.get("prometheus.io/scheme").map(String::as_str),
            Some("http")
        );
        assert_eq!(
            annotations.get("prometheus.io/scrape").map(String::as_str),
            Some("true")
        );
        // ... and the scrape label.
        assert_eq!(
            service
                .metadata
                .labels
                .as_ref()
                .unwrap()
                .get("prometheus.io/scrape")
                .map(String::as_str),
            Some("true")
        );

        // Legacy JMX port plus the Prometheus http port.
        assert_eq!(port(&service, JMX_METRICS_PORT_NAME), 9505);
        assert_eq!(port(&service, METRICS_PROVIDER_HTTP_PORT_NAME), 7000);
    }
}
