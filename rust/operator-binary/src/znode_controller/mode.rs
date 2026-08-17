//! The ownership split between the central operator and the per-cluster znode agent.
//!
//! Exactly one instance must claim each `ZookeeperZnode`. Two writers race on the status subresource
//! and the ZooKeeper tree; zero writers wedges namespace deletion, because nothing removes the
//! finalizer. See the spike ADR for why the operator keeps a catch-all controller.

use stackable_operator::{kube::ResourceExt, v2::types::common::Port};

use crate::crd::v1alpha1;

/// Which `ZookeeperZnode`s a znode-controller instance reconciles.
#[derive(Debug, Clone)]
pub enum Mode {
    /// The central operator. Catch-all: claims every znode that no per-cluster agent owns — znodes
    /// whose cluster has no `platformAccess`, cross-namespace znodes, and znodes whose cluster is
    /// missing (so the finalizer can still be removed on teardown, the property that stops
    /// `kubectl delete namespace` from hanging forever).
    OperatorFallback,

    /// A per-cluster agent, fixed to one cluster at creation. Claims only znodes in its own
    /// namespace whose `clusterRef` resolves to its cluster — a purely syntactic check, no API call.
    Agent {
        cluster_name: String,
        namespace: String,
        /// The ZooKeeper client port to connect to (passed in from the operator that created the
        /// agent, which computed it from the cluster's TLS settings). Currently informational: the
        /// znode `validate` step recomputes the port from the referenced cluster, so both agree.
        #[allow(dead_code)]
        client_port: Port,
    },
}

/// What a znode-controller instance should do with a given `ZookeeperZnode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// This instance provisions the znode (creates/updates it and manages its finalizer).
    Reconcile,
    /// Another instance owns it and is assumed healthy; do nothing.
    Ignore,
    /// The operator does not provision this znode — a per-cluster agent does — but must report the
    /// agent's liveness on it (writing an `AgentUnavailable` condition when the agent's lease is
    /// stale). See spike step 4c.
    ReportAgentLiveness,
}

impl Mode {
    /// What this instance should do with `znode`.
    ///
    /// `zk` is the dereferenced parent cluster (or `None` if it could not be fetched). The agent's
    /// decision is purely syntactic and ignores it; the operator uses it to defer to an agent when
    /// the cluster has `platformAccess` and the znode shares the cluster's namespace — but still
    /// reports that agent's liveness.
    pub fn disposition(
        &self,
        znode: &v1alpha1::ZookeeperZnode,
        zk: Option<&v1alpha1::ZookeeperCluster>,
    ) -> Disposition {
        match self {
            Mode::Agent {
                cluster_name,
                namespace,
                ..
            } => {
                if agent_owns(znode, cluster_name, namespace) {
                    Disposition::Reconcile
                } else {
                    Disposition::Ignore
                }
            }
            Mode::OperatorFallback => {
                if some_agent_owns(znode, zk) {
                    Disposition::ReportAgentLiveness
                } else {
                    Disposition::Reconcile
                }
            }
        }
    }
}

/// Whether the agent for `cluster_name`/`namespace` owns `znode`: the znode is in the agent's
/// namespace and its `clusterRef` resolves to the agent's cluster in that same namespace.
fn agent_owns(znode: &v1alpha1::ZookeeperZnode, cluster_name: &str, namespace: &str) -> bool {
    let ref_name = znode.spec.cluster_ref.name.as_deref();
    // `namespace_relative_from` resolves `clusterRef.namespace`, defaulting to the znode's own
    // namespace when omitted. A cross-namespace reference therefore does not match.
    let ref_ns = znode.spec.cluster_ref.namespace_relative_from(znode);
    znode.namespace().as_deref() == Some(namespace)
        && ref_name == Some(cluster_name)
        && ref_ns == Some(namespace)
}

/// Whether *some* per-cluster agent owns `znode`: its cluster exists, has `platformAccess`, and the
/// znode shares the cluster's namespace (agents are namespaced and only claim same-namespace znodes).
fn some_agent_owns(
    znode: &v1alpha1::ZookeeperZnode,
    zk: Option<&v1alpha1::ZookeeperCluster>,
) -> bool {
    let Some(zk) = zk else {
        return false;
    };
    if zk.spec.cluster_config.platform_access.is_none() {
        return false;
    }
    let cluster_ns = zk.namespace();
    cluster_ns.is_some() && znode.namespace() == cluster_ns
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use stackable_operator::v2::types::{common::Port, kubernetes::SecretClassName};

    use super::*;
    use crate::crd::platform_access::v1alpha1::{
        ZookeeperPlatformAccess, ZookeeperPlatformAccessCredential,
    };

    /// A minimal `platformAccess` (built in Rust rather than YAML: serde_yaml represents
    /// externally-tagged enums as YAML tags, not the `{secretClass: …}` maps the JSON k8s API uses).
    fn test_platform_access() -> ZookeeperPlatformAccess {
        let tls = SecretClassName::from_str("tls").expect("valid SecretClass name");
        ZookeeperPlatformAccess {
            trust_anchor_secret_class: tls.clone(),
            credential: ZookeeperPlatformAccessCredential::SecretClass(tls),
        }
    }

    /// Builds a znode in `ns` referencing cluster `ref_name`; `ref_ns` = `None` means the reference
    /// omits the namespace (so it defaults to the znode's own namespace).
    fn znode(ns: &str, ref_name: &str, ref_ns: Option<&str>) -> v1alpha1::ZookeeperZnode {
        let ns_line = ref_ns
            .map(|n| format!("\n    namespace: {n}"))
            .unwrap_or_default();
        serde_yaml::from_str(&format!(
            "apiVersion: zookeeper.stackable.tech/v1alpha1\n\
             kind: ZookeeperZnode\n\
             metadata:\n  name: test-znode\n  namespace: {ns}\n\
             spec:\n  clusterRef:\n    name: {ref_name}{ns_line}\n"
        ))
        .expect("valid test znode YAML")
    }

    /// Builds a cluster in `ns`, with or without `platformAccess` (set in Rust, see
    /// [`test_platform_access`]).
    fn cluster(ns: &str, name: &str, platform_access: bool) -> v1alpha1::ZookeeperCluster {
        let mut zk: v1alpha1::ZookeeperCluster = serde_yaml::from_str(&format!(
            "apiVersion: zookeeper.stackable.tech/v1alpha1\n\
             kind: ZookeeperCluster\n\
             metadata:\n  name: {name}\n  namespace: {ns}\n\
             spec:\n  image:\n    productVersion: \"3.9.5\"\n\
             \x20 servers:\n    roleGroups:\n      default:\n        replicas: 1\n"
        ))
        .expect("valid test cluster YAML");
        if platform_access {
            zk.spec.cluster_config.platform_access = Some(test_platform_access());
        }
        zk
    }

    fn agent() -> Mode {
        Mode::Agent {
            cluster_name: "zk".to_owned(),
            namespace: "prod".to_owned(),
            client_port: Port(2282),
        }
    }

    // --- Agent: reconciles only same-namespace, matching-cluster znodes ---

    #[test]
    fn agent_reconciles_same_ns_matching_cluster() {
        assert_eq!(
            agent().disposition(&znode("prod", "zk", None), None),
            Disposition::Reconcile
        );
    }

    #[test]
    fn agent_ignores_other_namespace() {
        assert_eq!(
            agent().disposition(&znode("other", "zk", None), None),
            Disposition::Ignore
        );
    }

    #[test]
    fn agent_ignores_other_cluster() {
        assert_eq!(
            agent().disposition(&znode("prod", "other-zk", None), None),
            Disposition::Ignore
        );
    }

    #[test]
    fn agent_ignores_cross_namespace_reference() {
        // znode in `prod` but referencing a cluster in `other` — cross-namespace, agent skips.
        assert_eq!(
            agent().disposition(&znode("prod", "zk", Some("other")), None),
            Disposition::Ignore
        );
    }

    // --- Operator: reconciles the complement, keyed on the cluster's platformAccess ---

    #[test]
    fn operator_reconciles_when_cluster_missing() {
        // Cluster gone (e.g. deleted): the operator must reconcile it to remove the finalizer.
        assert_eq!(
            Mode::OperatorFallback.disposition(&znode("prod", "zk", None), None),
            Disposition::Reconcile
        );
    }

    #[test]
    fn operator_reconciles_cluster_without_platform_access() {
        let zk = cluster("prod", "zk", false);
        assert_eq!(
            Mode::OperatorFallback.disposition(&znode("prod", "zk", None), Some(&zk)),
            Disposition::Reconcile
        );
    }

    #[test]
    fn operator_reports_liveness_for_agent_owned_znode() {
        // platformAccess cluster + same-namespace znode ⇒ the agent provisions it; the operator only
        // reports the agent's liveness.
        let zk = cluster("prod", "zk", true);
        assert_eq!(
            Mode::OperatorFallback.disposition(&znode("prod", "zk", None), Some(&zk)),
            Disposition::ReportAgentLiveness
        );
    }

    #[test]
    fn operator_reconciles_cross_namespace_znode_to_platform_access_cluster() {
        // Cross-namespace znodes stay with the operator even for platformAccess clusters.
        let zk = cluster("other", "zk", true);
        assert_eq!(
            Mode::OperatorFallback.disposition(&znode("prod", "zk", Some("other")), Some(&zk)),
            Disposition::Reconcile
        );
    }

    /// For a znode bound to a platformAccess cluster in the agent's namespace, the agent reconciles
    /// and the operator does not — the core "no two writers" invariant (the operator only ever
    /// *reports* liveness there, and only writes when the lease is stale, i.e. when the agent isn't).
    #[test]
    fn exactly_one_reconciles_each_same_ns_znode() {
        let zk = cluster("prod", "zk", true);
        let znode = znode("prod", "zk", None);
        assert_eq!(
            agent().disposition(&znode, Some(&zk)),
            Disposition::Reconcile
        );
        assert_eq!(
            Mode::OperatorFallback.disposition(&znode, Some(&zk)),
            Disposition::ReportAgentLiveness
        );
    }
}
