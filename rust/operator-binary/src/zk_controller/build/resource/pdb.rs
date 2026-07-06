use stackable_operator::{
    commons::pdb::PdbConfig, k8s_openapi::api::policy::v1::PodDisruptionBudget,
    v2::builder::pdb::pod_disruption_budget_builder_with_role,
};

use crate::{
    crd::ZookeeperRole,
    zk_controller::validate::{ValidatedCluster, controller_name, operator_name, product_name},
};

/// Builds the [`PodDisruptionBudget`] for the given `role`, or `None` if PDBs are disabled.
pub fn build_pdb(
    pdb: &PdbConfig,
    cluster: &ValidatedCluster,
    role: &ZookeeperRole,
) -> Option<PodDisruptionBudget> {
    if !pdb.enabled {
        return None;
    }
    let max_unavailable = pdb.max_unavailable.unwrap_or(match role {
        ZookeeperRole::Server => max_unavailable_servers(),
    });

    let pdb = pod_disruption_budget_builder_with_role(
        cluster,
        &product_name(),
        &ValidatedCluster::role_name(),
        &operator_name(),
        &controller_name(),
    )
    .with_max_unavailable(max_unavailable)
    .build();

    Some(pdb)
}

fn max_unavailable_servers() -> u16 {
    1
}
