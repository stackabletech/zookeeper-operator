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

#[cfg(test)]
mod tests {
    use stackable_operator::k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

    use super::*;
    use crate::zk_controller::test_support::{minimal_zk, validated_cluster};

    /// By default PDBs are enabled and default to `maxUnavailable: 1` for the server role.
    #[test]
    fn pdb_enabled_by_default_with_max_unavailable_one() {
        let validated = validated_cluster(&minimal_zk(
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
                    replicas: 3
            "#,
        ));
        let role_config = validated.role_config.as_ref().expect("role config present");

        let pdb = build_pdb(&role_config.pdb, &validated, &ZookeeperRole::Server)
            .expect("PDB enabled by default");
        let spec = pdb.spec.as_ref().unwrap();
        assert_eq!(spec.max_unavailable, Some(IntOrString::Int(1)));

        let match_labels = spec
            .selector
            .as_ref()
            .unwrap()
            .match_labels
            .as_ref()
            .unwrap();
        assert_eq!(
            match_labels
                .get("app.kubernetes.io/component")
                .map(String::as_str),
            Some("server")
        );
    }

    /// Disabling the PDB via `roleConfig` produces no `PodDisruptionBudget`.
    #[test]
    fn pdb_disabled_returns_none() {
        let validated = validated_cluster(&minimal_zk(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: simple-zookeeper
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleConfig:
                  podDisruptionBudget:
                    enabled: false
                roleGroups:
                  default:
                    replicas: 3
            "#,
        ));
        let role_config = validated.role_config.as_ref().expect("role config present");

        assert!(
            build_pdb(&role_config.pdb, &validated, &ZookeeperRole::Server).is_none(),
            "PDB should be None when disabled"
        );
    }
}
