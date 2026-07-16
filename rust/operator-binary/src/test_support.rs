//! Shared helpers for building validated test clusters from minimal YAML fixtures.
//!
//! Lives at the crate root because it is used across `zk_controller`, `znode_controller` and
//! `crd` test modules, not just one of them.

use std::str::FromStr;

use stackable_operator::{
    cli::OperatorEnvironmentOptions, commons::networking::DomainName,
    utils::cluster_info::KubernetesClusterInfo, v2::types::operator::RoleGroupName,
};

use crate::{
    crd::{ZookeeperRole, authentication::DereferencedAuthenticationClasses, v1alpha1},
    zk_controller::{
        dereference::DereferencedObjects,
        validate::{ValidatedCluster, ZookeeperRoleGroupConfig, validate},
    },
};

/// Parses a minimal `ZookeeperCluster` test fixture, defaulting `namespace`/`uid` so the
/// validate step can build a [`ValidatedCluster`].
pub fn minimal_zk(yaml: &str) -> v1alpha1::ZookeeperCluster {
    let mut zk: v1alpha1::ZookeeperCluster =
        serde_yaml::from_str(yaml).expect("invalid test ZookeeperCluster YAML");
    zk.metadata
        .namespace
        .get_or_insert_with(|| "default".to_owned());
    zk.metadata
        .uid
        .get_or_insert_with(|| "c27b3971-ca72-42c1-80a4-abdfc1db0ddd".to_owned());
    zk
}

/// Minimal valid `ZookeeperCluster` YAML: a single `default` server role group with `replicas`
/// servers and nothing else.
///
/// Use this (or [`minimal_zk_default`]) for tests that just need *a* valid cluster and don't
/// care about the spec. Keep an inline fixture when the spec detail is the thing under test.
pub fn minimal_zk_yaml(replicas: u16) -> String {
    format!(
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
                replicas: {replicas}
        "#
    )
}

/// Parsed counterpart of [`minimal_zk_yaml`].
pub fn minimal_zk_default(replicas: u16) -> v1alpha1::ZookeeperCluster {
    minimal_zk(&minimal_zk_yaml(replicas))
}

pub fn cluster_info() -> KubernetesClusterInfo {
    KubernetesClusterInfo {
        cluster_domain: DomainName::try_from("cluster.local").expect("valid domain"),
    }
}

fn operator_environment() -> OperatorEnvironmentOptions {
    OperatorEnvironmentOptions {
        operator_namespace: "stackable-operators".to_owned(),
        operator_service_name: "zookeeper-operator".to_owned(),
        image_repository: "oci.example.org".to_owned(),
    }
}

/// Runs the real validate step against a minimal (auth-free) fixture, returning the result so
/// tests can assert on validation errors.
pub fn try_validate(
    zk: &v1alpha1::ZookeeperCluster,
) -> Result<ValidatedCluster, crate::zk_controller::validate::Error> {
    try_validate_with_auth(zk, DereferencedAuthenticationClasses::new_for_tests())
}

/// Runs the real validate step with caller-supplied (dereferenced) AuthenticationClasses, so
/// tests can exercise the client-mTLS matrix.
pub fn try_validate_with_auth(
    zk: &v1alpha1::ZookeeperCluster,
    authentication_classes: DereferencedAuthenticationClasses,
) -> Result<ValidatedCluster, crate::zk_controller::validate::Error> {
    validate(
        zk,
        &DereferencedObjects {
            authentication_classes,
        },
        &operator_environment(),
    )
}

/// Runs the real validate step against a minimal (auth-free) fixture.
pub fn validated_cluster(zk: &v1alpha1::ZookeeperCluster) -> ValidatedCluster {
    try_validate(zk).expect("validate should succeed for the test fixture")
}

/// Runs the real validate step with a single TLS client-auth `AuthenticationClass`.
pub fn validated_cluster_with_client_auth(zk: &v1alpha1::ZookeeperCluster) -> ValidatedCluster {
    try_validate_with_auth(
        zk,
        DereferencedAuthenticationClasses::new_for_tests_with_tls_client_auth(),
    )
    .expect("validate should succeed for the test fixture")
}

/// Looks up the validated, merged config of the named `server` role group together with its
/// parsed [`RoleGroupName`] — the standard `(name, config)` inputs to the
/// `build_server_rolegroup_*` functions. Panics if the group does not exist.
pub fn server_rolegroup_config<'a>(
    validated: &'a ValidatedCluster,
    role_group: &str,
) -> (RoleGroupName, &'a ZookeeperRoleGroupConfig) {
    let role_group_name = RoleGroupName::from_str(role_group).expect("valid role group name");
    let config = validated
        .role_group_configs
        .get(&ZookeeperRole::Server)
        .and_then(|groups| groups.get(&role_group_name))
        .unwrap_or_else(|| panic!("server role group {role_group:?} should exist"));
    (role_group_name, config)
}
