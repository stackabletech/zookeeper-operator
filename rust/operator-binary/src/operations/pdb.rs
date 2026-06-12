use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    cluster_resources::ClusterResources,
    commons::pdb::PdbConfig,
    kube::ResourceExt,
    v2::{
        builder::pdb::pod_disruption_budget_builder_with_role,
        types::operator::{ControllerName, OperatorName, ProductName, RoleName},
    },
};

use crate::{
    crd::{APP_NAME, OPERATOR_NAME, ZookeeperRole},
    zk_controller::{ZK_CONTROLLER_NAME, validate::ValidatedCluster},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Cannot apply PodDisruptionBudget [{name}]"))]
    ApplyPdb {
        source: stackable_operator::cluster_resources::Error,
        name: String,
    },
}

pub async fn add_pdbs(
    pdb: &PdbConfig,
    validated_cluster: &ValidatedCluster,
    role: &ZookeeperRole,
    client: &Client,
    cluster_resources: &mut ClusterResources<'_>,
) -> Result<(), Error> {
    if !pdb.enabled {
        return Ok(());
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
    let pdb_name = pdb.name_any();
    cluster_resources
        .add(client, pdb)
        .await
        .with_context(|_| ApplyPdbSnafu { name: pdb_name })?;

    Ok(())
}

fn max_unavailable_servers() -> u16 {
    1
}
