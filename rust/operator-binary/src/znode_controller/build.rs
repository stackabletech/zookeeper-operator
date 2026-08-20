//! The build step in the ZookeeperZnode controller.

use snafu::{ResultExt, Snafu};

use crate::{
    discovery::{self, build_znode_discovery_configmap},
    znode_controller::{KubernetesResources, validate::ValidatedZnode},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build the discovery ConfigMap"))]
    DiscoveryConfigMap { source: discovery::Error },
}

/// Builds every Kubernetes resource for the given validated znode.
///
/// Does not need a Kubernetes client: the referenced cluster and the addresses published by its
/// role Listener are already dereferenced and validated by this point. The znode itself (a path
/// inside the ZooKeeper ensemble, not a Kubernetes object) is created by the apply step.
pub fn build(znode: &ValidatedZnode, znode_path: &str) -> Result<KubernetesResources, Error> {
    let discovery_config_map =
        build_znode_discovery_configmap(znode, &znode.discovery_addresses, znode_path)
            .context(DiscoveryConfigMapSnafu)?;

    Ok(KubernetesResources {
        discovery_config_maps: vec![discovery_config_map],
    })
}
