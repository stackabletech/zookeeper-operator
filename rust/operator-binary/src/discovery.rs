use crate::utils::build_recommended_labels;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::{ConfigMap, Endpoints, Service},
    kube::{runtime::reflector::ObjectRef, Resource, ResourceExt},
};
use stackable_zookeeper_crd::security::ZookeeperSecurity;
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperRole};
use std::{collections::BTreeSet, num::TryFromIntError};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object {} is missing metadata to build owner reference", zk))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        zk: ObjectRef<ZookeeperCluster>,
    },
    #[snafu(display("chroot path {} was relative (must be absolute)", chroot))]
    RelativeChroot { chroot: String },
    #[snafu(display("object has no name associated"))]
    NoName,
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
    #[snafu(display("failed to list expected pods"))]
    ExpectedPods {
        source: stackable_zookeeper_crd::Error,
    },
    #[snafu(display("could not find service port with name {}", port_name))]
    NoServicePort { port_name: String },
    #[snafu(display("service port with name {} does not have a nodePort", port_name))]
    NoNodePort { port_name: String },
    #[snafu(display("could not find Endpoints for {}", svc))]
    FindEndpoints {
        source: stackable_operator::error::Error,
        svc: ObjectRef<Service>,
    },
    #[snafu(display("nodePort was out of range"))]
    InvalidNodePort { source: TryFromIntError },
    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::error::Error,
    },
}

/// Builds discovery [`ConfigMap`]s for connecting to a [`ZookeeperCluster`] for all expected scenarios
#[allow(clippy::too_many_arguments)]
pub async fn build_discovery_configmaps(
    zk: &ZookeeperCluster,
    owner: &impl Resource<DynamicType = ()>,
    client: &stackable_operator::client::Client,
    controller_name: &str,
    svc: &Service,
    chroot: Option<&str>,
    resolved_product_image: &ResolvedProductImage,
    zookeeper_security: &ZookeeperSecurity,
) -> Result<Vec<ConfigMap>, Error> {
    let name = owner.name_unchecked();
    let namespace = owner.namespace().context(NoNamespaceSnafu)?;

    let mut discovery_configmaps = vec![build_discovery_configmap(
        zk,
        owner,
        zookeeper_security,
        name.as_str(),
        &namespace,
        controller_name,
        chroot,
        pod_hosts(zk, zookeeper_security)?,
        resolved_product_image,
    )?];
    if zk.spec.cluster_config.listener_class
        == stackable_zookeeper_crd::CurrentlySupportedListenerClasses::ExternalUnstable
    {
        discovery_configmaps.push(build_discovery_configmap(
            zk,
            owner,
            zookeeper_security,
            &format!("{}-nodeport", name),
            &namespace,
            controller_name,
            chroot,
            nodeport_hosts(client, svc, "zk").await?,
            resolved_product_image,
        )?);
    }

    Ok(discovery_configmaps)
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain [`ZookeeperCluster`]
///
/// `hosts` will usually come from either [`pod_hosts`] or [`nodeport_hosts`].
#[allow(clippy::too_many_arguments)]
fn build_discovery_configmap(
    zk: &ZookeeperCluster,
    owner: &impl Resource<DynamicType = ()>,
    zookeeper_security: &ZookeeperSecurity,
    name: &str,
    namespace: &str,
    controller_name: &str,
    chroot: Option<&str>,
    hosts: impl IntoIterator<Item = (impl Into<String>, u16)>,
    resolved_product_image: &ResolvedProductImage,
) -> Result<ConfigMap, Error> {
    // Write a connection string of the format that Java ZooKeeper client expects:
    // "{host1}:{port1},{host2:port2},.../{chroot}"
    // See https://zookeeper.apache.org/doc/current/apidocs/zookeeper-server/org/apache/zookeeper/ZooKeeper.html#ZooKeeper-java.lang.String-int-org.apache.zookeeper.Watcher-
    let hosts = hosts
        .into_iter()
        .map(|(host, port)| format!("{}:{}", host.into(), port))
        .collect::<Vec<_>>()
        .join(",");
    let mut conn_str = hosts.clone();
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
                .build(),
        )
        .add_data("ZOOKEEPER", conn_str)
        // Some clients don't support ZooKeeper's merged `hosts/chroot` format, so export them separately for these clients
        .add_data("ZOOKEEPER_HOSTS", hosts)
        .add_data(
            "ZOOKEEPER_CLIENT_PORT",
            zookeeper_security.client_port().to_string(),
        )
        .add_data("ZOOKEEPER_CHROOT", chroot.unwrap_or("/"))
        .build()
        .context(BuildConfigMapSnafu)
}

/// Lists all Pods FQDNs expected to host the [`ZookeeperCluster`]
fn pod_hosts<'a>(
    zk: &'a ZookeeperCluster,
    zookeeper_security: &'a ZookeeperSecurity,
) -> Result<impl IntoIterator<Item = (String, u16)> + 'a, Error> {
    Ok(zk
        .pods()
        .context(ExpectedPodsSnafu)?
        .map(|pod_ref| (pod_ref.fqdn(), zookeeper_security.client_port())))
}

/// Lists all nodes currently hosting Pods participating in the [`Service`]
async fn nodeport_hosts(
    client: &stackable_operator::client::Client,
    svc: &Service,
    port_name: &str,
) -> Result<impl IntoIterator<Item = (String, u16)>, Error> {
    let svc_port = svc
        .spec
        .as_ref()
        .and_then(|svc_spec| {
            svc_spec
                .ports
                .as_ref()?
                .iter()
                .find(|port| port.name.as_deref() == Some(port_name))
        })
        .context(NoServicePortSnafu { port_name })?;
    let node_port = svc_port.node_port.context(NoNodePortSnafu { port_name })?;
    let endpoints = client
        .get::<Endpoints>(
            svc.metadata.name.as_deref().context(NoNameSnafu)?,
            svc.metadata
                .namespace
                .as_deref()
                .context(NoNamespaceSnafu)?,
        )
        .await
        .with_context(|_| FindEndpointsSnafu {
            svc: ObjectRef::from_obj(svc),
        })?;
    let nodes = endpoints
        .subsets
        .into_iter()
        .flatten()
        .flat_map(|subset| subset.addresses)
        .flatten()
        .flat_map(|addr| addr.node_name);
    let addrs = nodes
        .map(|node| Ok((node, node_port.try_into().context(InvalidNodePortSnafu)?)))
        .collect::<Result<BTreeSet<_>, _>>()?;
    Ok(addrs)
}
