use crate::ZookeeperCluster;

use stackable_operator::k8s_openapi::{
    api::{
        apps::v1::{
            DaemonSet, DaemonSetStatus, Deployment, DeploymentStatus, StatefulSet,
            StatefulSetStatus,
        },
        core::v1::{Pod, PodStatus},
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

trait ConditionBuilder {
    fn conditions(&self) -> Vec<ClusterCondition>;
    //fn paused(&self, cluster_resource: &T) -> Option<ClusterCondition>;
    //fn stopped(&self, cluster_resource: &T) -> Option<ClusterCondition>;
}

struct StatefulSetConditionBuilder {
    resource: &T,
    stateful_sets: Vec<StatefulSet>
}

impl StatefulSetConditionBuilder {
    pub fn new(resource: &T) -> StatefulSetConditionBuilder {

    }
}
impl ConditionBuilder for StatefulSetConditionBuilder {
    fn add(&mut self, sts: &StatefulSet) {
        self.stateful_sets.push(sts.clone());
    }
    fn conditions(&self) -> Vec<ClusterCondition> {

    }
}
pub fn compute_conditions(
    zk: &ZookeeperCluster,
    condition_builder: &[ConditionBuilder],
) -> Vec<ClusterCondition> {
    let mut result = vec![];

    let mut current_conditions = HashMap<ClusterConditionType, ClusterCondition>::new();
    for cb in condition_builder {
        let cb_conditions: HashMap<lusterConditionType, ClusterCondition> = cb.conditions().iter().map(|c| (c.type_, c)).collect();

        for (current_condition_type, cb_condition) in cb_conditions {
            let next_condition = if let Some(cb_condition) = cb_conditions.get(current_condition_type) {
                // take max
                match (current_condition.status, cb_condition_status) {
                    True, False => False
                    False, True => False
                    Unknown, True => Unknown
                    Unknown, False => Unknown
                    True, Unknown => Unknown
                    False, Unknown => Unknown
                    _, _ => _
                }
            } else {
                cb_condition
            }

            current_conditions.insert(current_condition_type, next_condition);
        }
    }

    result.values()
}

fn available(
    zk: &ZookeeperCluster,
    stateful_sets: &[StatefulSet],
    pods: &[Pod],
) -> ClusterCondition {
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

    let message = match sts_available {
        true => Some("cluster has the requested amount of ready replicas".to_string()),
        false => Some("cluster does not have the requested amount of ready replicas".to_string()),
    };

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
                message,
                ..old_available
            }
        }
    } else {
        ClusterCondition {
            last_update_time: Some(now.clone()),
            last_transition_time: Some(now),
            status: sts_available.into(),
            message,
            reason: None,
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
