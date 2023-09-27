use std::cmp::max;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pdb::PodDisruptionBudgetBuilder, client::Client, cluster_resources::ClusterResources,
    commons::pdb::PdbConfig, kube::ResourceExt,
};
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperRole, APP_NAME, OPERATOR_NAME};

use crate::zk_controller::ZK_CONTROLLER_NAME;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Cannot create PodDisruptionBudget for role [{role}]"))]
    CreatePdb {
        source: stackable_operator::error::Error,
        role: String,
    },
    #[snafu(display("Cannot apply role group PodDisruptionBudget [{name}]"))]
    ApplyPdb {
        source: stackable_operator::error::Error,
        name: String,
    },
}

pub async fn add_pdbs(
    pdb: &PdbConfig,
    zookeeper: &ZookeeperCluster,
    role: &ZookeeperRole,
    client: &Client,
    cluster_resources: &mut ClusterResources,
) -> Result<(), Error> {
    if !pdb.enabled {
        return Ok(());
    }
    let max_unavailable = pdb.max_unavailable.unwrap_or(match role {
        ZookeeperRole::Server => max_unavailable_servers(zookeeper.num_servers()),
    });
    let pdb = PodDisruptionBudgetBuilder::new_with_role(
        zookeeper,
        APP_NAME,
        &role.to_string(),
        OPERATOR_NAME,
        ZK_CONTROLLER_NAME,
    )
    .with_context(|_| CreatePdbSnafu {
        role: role.to_string(),
    })?
    .with_max_unavailable(max_unavailable)
    .build();
    let pdb_name = pdb.name_any();
    cluster_resources
        .add(client, pdb)
        .await
        .with_context(|_| ApplyPdbSnafu { name: pdb_name })?;

    Ok(())
}

fn max_unavailable_servers(num_servers: u16) -> u16 {
    // Minimum required amount of servers to form quorum.
    let quorum_size = quorum_size(num_servers);

    // Subtract once to not cause a single point of failure
    let max_unavailable = num_servers.saturating_sub(quorum_size).saturating_sub(1);

    // Clamp to at least a single node allowed to be offline, so we don't block Kubernetes nodes from draining.
    max(max_unavailable, 1)
}

fn quorum_size(num_servers: u16) -> u16 {
    // Same as max((num_servers / 2) + 1, 1), but without the need for floating point arithmetics,
    // which are subject to rounding errors.
    max((num_servers + 2) / 2, 1)
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(0, 1, 1)]
    #[case(1, 1, 1)]
    #[case(2, 2, 1)]
    #[case(3, 2, 1)]
    #[case(4, 3, 1)]
    #[case(5, 3, 1)]
    #[case(6, 4, 1)]
    #[case(7, 4, 2)]
    #[case(8, 5, 2)]
    #[case(9, 5, 3)]
    #[case(10, 6, 3)]
    #[case(20, 11, 8)]
    #[case(100, 51, 48)]

    fn test_max_unavailable_servers(
        #[case] num_servers: u16,
        #[case] expected_quorum_size: u16,
        #[case] expected_max_unavailable: u16,
    ) {
        let quorum_size = quorum_size(num_servers);
        let max_unavailable = max_unavailable_servers(num_servers);
        assert_eq!(quorum_size, expected_quorum_size);
        assert_eq!(max_unavailable, expected_max_unavailable);
    }
}
