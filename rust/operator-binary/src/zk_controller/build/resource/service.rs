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
    zk_controller::{
        build::object_meta,
        validate::{ValidatedCluster, ZookeeperRoleGroupConfig},
    },
};

/// The rolegroup [`Service`] is a headless service that allows internal access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
pub(crate) fn build_server_rolegroup_headless_service(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
) -> Service {
    let metadata = object_meta(
        cluster,
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
    let metadata = object_meta(
        cluster,
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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stackable_operator::v2::types::operator::RoleGroupName;

    use super::*;
    use crate::{
        crd::ZookeeperRole,
        zk_controller::test_support::{app_version_label, minimal_zk, validated_cluster},
    };

    /// Every metrics Service must carry the Prometheus scrape label and the
    /// `prometheus.io/path|port|scheme|scrape` annotations, or Prometheus stops discovering the
    /// endpoints.
    #[test]
    fn test_rolegroup_metrics_service() {
        let zookeeper = minimal_zk(
            r#"
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
                    replicas: 1
            "#,
        );
        let cluster = validated_cluster(&zookeeper);
        let role_group_name: RoleGroupName = "default".parse().expect("valid role group name");
        let rolegroup_config =
            &cluster.role_group_configs[&ZookeeperRole::Server][&role_group_name];

        let service =
            build_server_rolegroup_metrics_service(&cluster, &role_group_name, rolegroup_config);

        assert_eq!(
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "annotations": {
                        "prometheus.io/path": "/metrics",
                        "prometheus.io/port": "7000",
                        "prometheus.io/scheme": "http",
                        "prometheus.io/scrape": "true"
                    },
                    "labels": {
                        "app.kubernetes.io/component": "server",
                        "app.kubernetes.io/instance": "simple-zookeeper",
                        "app.kubernetes.io/managed-by": "zookeeper.stackable.tech_zookeepercluster",
                        "app.kubernetes.io/name": "zookeeper",
                        "app.kubernetes.io/role-group": "default",
                        "app.kubernetes.io/version": app_version_label("3.9.5"),
                        "prometheus.io/scrape": "true",
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "simple-zookeeper-server-default-metrics",
                    "namespace": "default",
                    "ownerReferences": [
                        {
                            "apiVersion": "zookeeper.stackable.tech/v1alpha1",
                            "controller": true,
                            "kind": "ZookeeperCluster",
                            "name": "simple-zookeeper",
                            "uid": "c27b3971-ca72-42c1-80a4-abdfc1db0ddd"
                        }
                    ]
                },
                "spec": {
                    "clusterIP": "None",
                    "ports": [
                        {
                            "name": "jmx-metrics",
                            "port": 9505,
                            "protocol": "TCP"
                        },
                        {
                            "name": "metrics",
                            "port": 7000,
                            "protocol": "TCP"
                        }
                    ],
                    "publishNotReadyAddresses": true,
                    "selector": {
                        "app.kubernetes.io/component": "server",
                        "app.kubernetes.io/instance": "simple-zookeeper",
                        "app.kubernetes.io/name": "zookeeper",
                        "app.kubernetes.io/role-group": "default"
                    },
                    "type": "ClusterIP"
                }
            }),
            serde_json::to_value(service).expect("must be serializable")
        );
    }
}
