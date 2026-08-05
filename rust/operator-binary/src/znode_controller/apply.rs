//! The apply step in the ZookeeperZnode controller.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    cluster_resources::{ClusterResource, ClusterResourceApplyStrategy, ClusterResources},
    deep_merger::ObjectOverrides,
    kube::Resource,
};

use crate::{
    APP_NAME, OPERATOR_NAME,
    znode_controller::{
        KubernetesResources, ZNODE_CONTROLLER_NAME, validate::ValidatedZnode, znode_mgmt,
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to apply Kubernetes resource"))]
    ApplyResource {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to ensure that ZNode {znode_path:?} exists in {zk_mgmt_addr}"))]
    EnsureZnode {
        source: znode_mgmt::Error,
        zk_mgmt_addr: String,
        znode_path: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Applier for the Kubernetes resource specifications produced by this controller.
///
/// Unlike the ZookeeperCluster controller's applier, the resources are not marked as prepared or
/// applied: the ZookeeperZnode has no cluster conditions, so there is no status step that could
/// derive the status from resources that were never applied.
pub struct Applier<'a> {
    client: &'a Client,
    cluster_resources: ClusterResources<'a>,
}

impl<'a> Applier<'a> {
    pub fn new(
        client: &'a Client,
        znode: &ValidatedZnode,
        apply_strategy: ClusterResourceApplyStrategy,
        object_overrides: &'a ObjectOverrides,
    ) -> Applier<'a> {
        // Infallible: `ValidatedZnode`'s object reference always contains name, namespace and uid
        // (set unconditionally during the validate step), which is all `ClusterResources::new`
        // requires.
        let cluster_resources = ClusterResources::new(
            APP_NAME,
            OPERATOR_NAME,
            ZNODE_CONTROLLER_NAME,
            &znode.object_ref(&()),
            apply_strategy,
            object_overrides,
        )
        .expect(
            "ClusterResources should be created because the ValidatedZnode's object reference \
             always contains name, namespace and uid",
        );

        Applier {
            client,
            cluster_resources,
        }
    }

    /// Applies the given Kubernetes resources.
    pub async fn apply(mut self, resources: KubernetesResources) -> Result<KubernetesResources> {
        // Destructured without `..`, so adding a field to [`KubernetesResources`] fails to
        // compile here instead of silently never being applied.
        let KubernetesResources {
            discovery_config_maps,
        } = resources;

        let discovery_config_maps = self.add_resources(discovery_config_maps).await?;

        self.cluster_resources
            .delete_orphaned_resources(self.client)
            .await
            .context(DeleteOrphanedResourcesSnafu)?;

        Ok(KubernetesResources {
            discovery_config_maps,
        })
    }

    async fn add_resources<T: ClusterResource + Sync>(
        &mut self,
        resources: Vec<T>,
    ) -> Result<Vec<T>> {
        let mut applied_resources = vec![];

        for resource in resources {
            applied_resources.push(
                self.cluster_resources
                    .add(self.client, resource)
                    .await
                    .context(ApplyResourceSnafu)?,
            );
        }

        Ok(applied_resources)
    }
}

/// Ensures that the znode exists in the ZooKeeper ensemble reachable at `zk_mgmt_addr`.
///
/// The znode is a path inside ZooKeeper rather than a Kubernetes object, so it cannot be part of
/// the client-free `build()` step, and it is not tracked in
/// [`ClusterResources`](stackable_operator::cluster_resources::ClusterResources) either. It must
/// exist before the discovery ConfigMap advertises it to clients.
pub async fn ensure_znode_exists(zk_mgmt_addr: &str, znode_path: &str) -> Result<()> {
    znode_mgmt::ensure_znode_exists(zk_mgmt_addr, znode_path)
        .await
        .with_context(|_| EnsureZnodeSnafu {
            zk_mgmt_addr,
            znode_path,
        })
}
