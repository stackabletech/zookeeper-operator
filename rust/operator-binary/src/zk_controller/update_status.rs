//! The update_status step in the ZookeeperCluster controller.

use std::hash::Hasher;

use fnv::FnvHasher;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    k8s_openapi::api::core::v1::ConfigMap,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    ZOOKEEPER_OPERATOR_NAME,
    crd::v1alpha1,
    zk_controller::{Applied, KubernetesResources},
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Computes the cluster status from the applied resources and patches it onto the
/// [`v1alpha1::ZookeeperCluster`].
///
/// Takes [`KubernetesResources<Applied>`] so the type system proves that the status derives from
/// applied resources, not merely built ones.
pub async fn update_status(
    client: &Client,
    zk: &v1alpha1::ZookeeperCluster,
    applied: &KubernetesResources<Applied>,
) -> Result<()> {
    let mut stateful_set_condition_builder = StatefulSetConditionBuilder::default();
    for stateful_set in &applied.stateful_sets {
        stateful_set_condition_builder.add(stateful_set.clone());
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&zk.spec.cluster_operation);

    let status = v1alpha1::ZookeeperClusterStatus {
        discovery_hash: Some(discovery_hash(&applied.discovery_config_map)),
        conditions: compute_conditions(
            zk,
            &[
                &stateful_set_condition_builder,
                &cluster_operation_cond_builder,
            ],
        ),
    };

    client
        .apply_patch_status(ZOOKEEPER_OPERATOR_NAME, zk, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(())
}

/// Hashes the resource version of the applied discovery ConfigMap, so that clients can tell when
/// the published connection details changed.
///
/// A ConfigMap that was never applied carries no resource version, in which case the hash covers
/// nothing.
fn discovery_hash(discovery_config_map: &ConfigMap) -> String {
    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    let mut discovery_hash = FnvHasher::with_key(0);

    if let Some(resource_version) = &discovery_config_map.metadata.resource_version {
        discovery_hash.write(resource_version.as_bytes())
    }

    // Serialize as a string to discourage users from trying to parse the value,
    // and to keep things flexible if we end up changing the hasher at some point.
    discovery_hash.finish().to_string()
}

#[cfg(test)]
mod tests {
    use stackable_operator::kube::api::ObjectMeta;

    use super::{ConfigMap, discovery_hash};

    fn discovery_config_map(resource_version: Option<&str>) -> ConfigMap {
        ConfigMap {
            metadata: ObjectMeta {
                name: Some("simple-zookeeper".to_owned()),
                resource_version: resource_version.map(ToOwned::to_owned),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        }
    }

    /// The hash exists so that clients can tell when the published connection details changed, so
    /// it must follow the discovery ConfigMap's resource version.
    #[test]
    fn discovery_hash_tracks_the_resource_version() {
        let hash = discovery_hash(&discovery_config_map(Some("42")));

        assert_ne!(hash, discovery_hash(&discovery_config_map(Some("43"))));
        assert_eq!(hash, discovery_hash(&discovery_config_map(Some("42"))));

        // A ConfigMap that was never applied carries no resource version, in which case the hash
        // covers nothing and stays at the hasher's initial state.
        assert_ne!(hash, discovery_hash(&discovery_config_map(None)));
    }
}
