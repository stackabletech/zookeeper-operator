use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use product_config::types::PropertyNameKind;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    kvp::{Annotations, Labels},
    role_utils::RoleGroupRef,
};

use crate::{
    crd::{
        APP_NAME, JMX_METRICS_PORT, JMX_METRICS_PORT_NAME, METRICS_PROVIDER_HTTP_PORT,
        METRICS_PROVIDER_HTTP_PORT_KEY, METRICS_PROVIDER_HTTP_PORT_NAME, ZOOKEEPER_ELECTION_PORT,
        ZOOKEEPER_ELECTION_PORT_NAME, ZOOKEEPER_LEADER_PORT, ZOOKEEPER_LEADER_PORT_NAME,
        ZOOKEEPER_PROPERTIES_FILE, v1alpha1,
    },
    utils::build_recommended_labels,
    zk_controller::ZK_CONTROLLER_NAME,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build Metadata"))]
    BuildMetadata {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build Labels"))]
    BuildLabel {
        source: stackable_operator::kvp::LabelError,
    },
}

/// The rolegroup [`Service`] is a headless service that allows internal access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
pub(crate) fn build_server_rolegroup_headless_service(
    zk: &v1alpha1::ZookeeperCluster,
    rolegroup: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Service, Error> {
    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(zk)
        .name(rolegroup.rolegroup_headless_service_name())
        .ownerreference_from_resource(zk, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(build_recommended_labels(
            zk,
            ZK_CONTROLLER_NAME,
            &resolved_product_image.app_version_label_value,
            &rolegroup.role,
            &rolegroup.role_group,
        ))
        .context(BuildMetadataSnafu)?
        .build();

    let service_selector_labels =
        Labels::role_group_selector(zk, APP_NAME, &rolegroup.role, &rolegroup.role_group)
            .context(BuildLabelSnafu)?;

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
        selector: Some(service_selector_labels.into()),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata,
        spec: Some(service_spec),
        status: None,
    })
}

/// The rolegroup [`Service`] for exposing metrics
pub(crate) fn build_server_rolegroup_metrics_service(
    zk: &v1alpha1::ZookeeperCluster,
    rolegroup: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<Service, Error> {
    let metrics_port = metrics_port_from_rolegroup_config(rolegroup_config);

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(zk)
        .name(rolegroup.rolegroup_metrics_service_name())
        .ownerreference_from_resource(zk, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(build_recommended_labels(
            zk,
            ZK_CONTROLLER_NAME,
            &resolved_product_image.app_version_label_value,
            &rolegroup.role,
            &rolegroup.role_group,
        ))
        .context(BuildMetadataSnafu)?
        .with_labels(prometheus_labels())
        .with_annotations(prometheus_annotations(metrics_port))
        .build();

    let service_selector_labels =
        Labels::role_group_selector(zk, APP_NAME, &rolegroup.role, &rolegroup.role_group)
            .context(BuildLabelSnafu)?;

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
        selector: Some(service_selector_labels.into()),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata,
        spec: Some(service_spec),
        status: None,
    })
}

pub(crate) fn metrics_port_from_rolegroup_config(
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> u16 {
    let metrics_port = rolegroup_config
        .get(&PropertyNameKind::File(
            ZOOKEEPER_PROPERTIES_FILE.to_string(),
        ))
        .expect("{ZOOKEEPER_PROPERTIES_FILE} is present")
        .get(METRICS_PROVIDER_HTTP_PORT_KEY)
        .expect("{METRICS_PROVIDER_HTTP_PORT_KEY} is set");

    match u16::from_str(metrics_port) {
        Ok(port) => port,
        Err(err) => {
            tracing::error!("{err}");
            tracing::info!("Defaulting to using {METRICS_PROVIDER_HTTP_PORT} as metrics port.");
            METRICS_PROVIDER_HTTP_PORT
        }
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
