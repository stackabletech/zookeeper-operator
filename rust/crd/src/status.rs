use crate::ZookeeperCluster;

use stackable_operator::k8s_openapi::{
    api::{
        apps::v1::{
            DaemonSet, DaemonSetStatus, Deployment, DeploymentStatus, StatefulSet,
            StatefulSetStatus,
        },
        core::v1::PodStatus,
    },
    apimachinery::pkg::apis::meta::v1::Time,
    chrono::Utc,
};

use serde::{Deserialize, Serialize};
use stackable_operator::kube::Resource;
use stackable_operator::schemars::{self, JsonSchema};
use tracing::{info, trace};

#[derive(
    strum::Display, Clone, Debug, Default, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize,
)]
#[serde(rename_all = "camelCase")]
pub enum ClusterConditionType {
    #[default]
    Available,
    Degraded,
    Progressing,
    Paused,
    Stopped,
}

#[derive(
    strum::Display, Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize,
)]
#[serde(rename_all = "camelCase")]
pub enum ClusterConditionStatus {
    #[default]
    True,
    False,
    Unknown,
}

impl From<bool> for ClusterConditionStatus {
    fn from(b: bool) -> ClusterConditionStatus {
        if b {
            ClusterConditionStatus::True
        } else {
            ClusterConditionStatus::False
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, JsonSchema, Deserialize, Serialize)]
pub struct ClusterCondition {
    /// Last time the condition transitioned from one status to another.
    pub last_transition_time: Option<Time>,

    /// The last time this condition was updated.
    pub last_update_time: Option<Time>,

    /// A human readable message indicating details about the transition.
    pub message: Option<String>,

    /// The reason for the condition's last transition.
    pub reason: Option<String>,

    /// Status of the condition, one of True, False, Unknown.
    pub status: ClusterConditionStatus,

    /// Type of deployment condition.
    pub type_: ClusterConditionType,
}

impl ClusterCondition {}

pub fn compute_conditions(
    zk: &ZookeeperCluster,
    stateful_sets: &[StatefulSet],
) -> Vec<ClusterCondition> {
    let mut result = vec![];

    result.push(available(zk, stateful_sets));
    result
}

fn available(zk: &ZookeeperCluster, stateful_sets: &[StatefulSet]) -> ClusterCondition {
    let opt_old_available = zk
        .status
        .as_ref()
        .map(|status| status.conditions.clone())
        .unwrap_or_default()
        .iter()
        .find(|cond| cond.type_ == ClusterConditionType::Available)
        .cloned();

    let mut sts_available = true;
    for sts in stateful_sets {
        sts_available = sts_available && stateful_set_available(sts);
    }

    let now = Time(Utc::now());
    if let Some(old_available) = opt_old_available {
        if old_available.status == sts_available.into() {
            ClusterCondition {
                last_update_time: Some(now),
                ..old_available
            }
        } else {
            ClusterCondition {
                last_update_time: Some(now.clone()),
                last_transition_time: Some(now),
                status: sts_available.into(),
                ..old_available
            }
        }
    } else {
        ClusterCondition {
            last_update_time: Some(now.clone()),
            last_transition_time: Some(now),
            status: sts_available.into(),
            message: Some("first time setting condition".to_string()),
            reason: Some("first time setting condition".to_string()),
            type_: ClusterConditionType::Available,
        }
    }
}

fn stateful_set_available(sts: &StatefulSet) -> bool {
    let requested_replicas = sts
        .spec
        .as_ref()
        .and_then(|spec| spec.replicas)
        .unwrap_or_default();
    let available_replicas = sts
        .status
        .as_ref()
        .and_then(|status| status.available_replicas)
        .unwrap_or_default();

    info!("requested_replicas={requested_replicas}");
    info!("available_replicas={available_replicas}");
    requested_replicas == available_replicas
}
