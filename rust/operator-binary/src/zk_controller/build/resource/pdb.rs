use std::str::FromStr;

use stackable_operator::{
    commons::pdb::PdbConfig,
    k8s_openapi::api::policy::v1::PodDisruptionBudget,
    v2::{
        builder::pdb::pod_disruption_budget_builder_with_role,
        types::operator::{ControllerName, OperatorName, ProductName, RoleName},
    },
};

use crate::{
    crd::{APP_NAME, OPERATOR_NAME, ZookeeperRole},
    zk_controller::{ZK_CONTROLLER_NAME, validate::ValidatedCluster},
};

/// Builds the [`PodDisruptionBudget`] for the given `role`, or `None` if PDBs are disabled.
pub fn build_pdb(
    pdb: &PdbConfig,
    validated_cluster: &ValidatedCluster,
    role: &ZookeeperRole,
) -> Option<PodDisruptionBudget> {
    if !pdb.enabled {
        return None;
    }
    let max_unavailable = pdb.max_unavailable.unwrap_or(match role {
        ZookeeperRole::Server => max_unavailable_servers(),
    });

    // These names are derived from compile-time constants and a validated role enum, so they are
    // guaranteed to be valid and we use the infallible v2 builder.
    let product_name =
        ProductName::from_str(APP_NAME).expect("APP_NAME should be a valid product name");
    let operator_name = OperatorName::from_str(OPERATOR_NAME)
        .expect("OPERATOR_NAME should be a valid operator name");
    let controller_name = ControllerName::from_str(ZK_CONTROLLER_NAME)
        .expect("ZK_CONTROLLER_NAME should be a valid controller name");
    let role_name =
        RoleName::from_str(&role.to_string()).expect("role name should be a valid role name");

    let pdb = pod_disruption_budget_builder_with_role(
        validated_cluster,
        &product_name,
        &role_name,
        &operator_name,
        &controller_name,
    )
    .with_max_unavailable(max_unavailable)
    .build();

    Some(pdb)
}

fn max_unavailable_servers() -> u16 {
    1
}
