//! Conditions on `ZookeeperZnode` (spike step 4b/4c).
//!
//! `ClusterConditionType` is a closed enum, so the znode's provisioning state is mapped onto the
//! `Available` / `Degraded` pair with a distinguishing `reason` — the readable signal the issue asks
//! for ("when no credentials can be provided this needs to show up as a readable condition").

use stackable_operator::status::condition::{
    ClusterCondition, ClusterConditionSet, ClusterConditionStatus, ClusterConditionType,
    ConditionBuilder,
};

/// The provisioning state of a single `ZookeeperZnode`.
pub enum ZnodeState {
    /// Provisioned successfully.
    Provisioned,
    /// Provisioning was attempted but failed (bad credential, ZooKeeper rejected the client cert,
    /// connection failed, ACL clash, …). Written by whichever instance owns the znode.
    Degraded { reason: String, message: String },
    /// No agent is running to provision this znode — its liveness lease is stale or absent. Written
    /// by the operator fallback, never the agent (which by definition isn't running then).
    AgentUnavailable { message: String },
}

/// Builds the `Available`/`Degraded` conditions for a [`ZnodeState`].
pub struct ZnodeConditionBuilder {
    state: ZnodeState,
}

impl ZnodeConditionBuilder {
    pub fn provisioned() -> Self {
        Self {
            state: ZnodeState::Provisioned,
        }
    }

    pub fn degraded(reason: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            state: ZnodeState::Degraded {
                reason: reason.into(),
                message: message.into(),
            },
        }
    }

    pub fn agent_unavailable(message: impl Into<String>) -> Self {
        Self {
            state: ZnodeState::AgentUnavailable {
                message: message.into(),
            },
        }
    }
}

impl ConditionBuilder for ZnodeConditionBuilder {
    fn build_conditions(&self) -> ClusterConditionSet {
        // (available status, degraded status, reason, message)
        let (available, degraded, reason, message) = match &self.state {
            ZnodeState::Provisioned => (
                ClusterConditionStatus::True,
                ClusterConditionStatus::False,
                None,
                "The ZNode is provisioned.".to_string(),
            ),
            ZnodeState::Degraded { reason, message } => (
                ClusterConditionStatus::False,
                ClusterConditionStatus::True,
                Some(reason.clone()),
                message.clone(),
            ),
            ZnodeState::AgentUnavailable { message } => (
                ClusterConditionStatus::False,
                ClusterConditionStatus::True,
                Some("AgentUnavailable".to_string()),
                message.clone(),
            ),
        };

        vec![
            ClusterCondition {
                type_: ClusterConditionType::Available,
                status: available,
                reason: reason.clone(),
                message: Some(message.clone()),
                last_transition_time: None,
            },
            ClusterCondition {
                type_: ClusterConditionType::Degraded,
                status: degraded,
                reason,
                message: Some(message),
                last_transition_time: None,
            },
        ]
        .into()
    }
}
