use serde::{Deserialize, Serialize};
use stackable_operator::{
    k8s_openapi::{
        api::{
            apps::v1::{
                DaemonSet, DaemonSetStatus, Deployment, DeploymentStatus, StatefulSet,
                StatefulSetStatus,
            },
            core::v1::{Pod, PodStatus},
        },
        apimachinery::pkg::apis::meta::v1::Time,
        chrono::Utc,
    },
    kube::Resource,
    schemars::{self, JsonSchema},
};
use std::collections::{BTreeMap, HashMap};
use tracing::info;

#[derive(
    strum::Display,
    Clone,
    Debug,
    Default,
    Deserialize,
    Eq,
    Hash,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "camelCase")]
pub enum ClusterConditionType {
    #[default]
    /// Available indicates that the binary maintained by the operator (eg: zookeeper for the
    /// zookeeper-operator), is functional and available in the cluster.
    Available,
    /// Degraded indicates that the operand is not functioning completely. An example of a degraded
    /// state would be if there should be 5 copies of the operand running but only 4 are running.
    /// It may still be available, but it is degraded.
    Degraded,
    /// Progressing indicates that the operator is actively making changes to the binary maintained
    /// by the operator (eg: zookeeper for the zookeeper-operator).
    Progressing,
    /// Paused indicates that the operator is not reconciling the cluster. This may be used for
    /// debugging or operator updating.
    Paused,
    /// Stopped indicates that all the cluster replicas are scaled down to 0. All resources (e.g.
    /// ConfigMaps, Services etc.) are kept.
    Stopped,
}

#[derive(
    strum::Display,
    Clone,
    Debug,
    Default,
    Deserialize,
    Eq,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "camelCase")]
pub enum ClusterConditionStatus {
    #[default]
    /// True means a resource is in the condition.
    True,
    /// False means a resource is not in the condition.
    False,
    /// Unknown means kubernetes cannot decide if a resource is in the condition or not.
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ClusterConditionMergeStrategy {
    #[default]
    /// Ordered merge strategy means that a higher (Unknown > False > True) ClusterConditionStatus
    /// will take precedence over lower ClusterConditionStatus. This means a condition for
    /// Available with status true will be overwritten by a condition for Available with status false.
    Ordered,
}

pub trait HasCondition {
    fn conditions(&self) -> Vec<ClusterCondition>;
}

pub trait ConditionBuilder {
    fn conditions(&self) -> Vec<ClusterCondition>;
    //fn paused(&self, cluster_resource: &T) -> Option<ClusterCondition>;
    //fn stopped(&self, cluster_resource: &T) -> Option<ClusterCondition>;
}

pub struct StatefulSetConditionBuilder<'a, T: HasCondition> {
    resource: &'a T,
    stateful_sets: Vec<StatefulSet>,
}

impl<'a, T: HasCondition> StatefulSetConditionBuilder<'a, T> {
    pub fn new(resource: &'a T) -> StatefulSetConditionBuilder<T> {
        StatefulSetConditionBuilder {
            resource,
            stateful_sets: Vec::new(),
        }
    }

    pub fn add(&mut self, sts: StatefulSet) {
        self.stateful_sets.push(sts);
    }

    pub fn available(&self) -> ClusterCondition {
        let opt_old_available = self
            .resource
            .conditions()
            .iter()
            .find(|cond| cond.type_ == ClusterConditionType::Available)
            .cloned();

        let mut sts_available = true;
        for sts in &self.stateful_sets {
            sts_available = sts_available && stateful_set_available(sts);
        }

        let message = match sts_available {
            true => Some("cluster has the requested amount of ready replicas".to_string()),
            false => {
                Some("cluster does not have the requested amount of ready replicas".to_string())
            }
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
}

impl<'a, T: HasCondition> ConditionBuilder for StatefulSetConditionBuilder<'a, T> {
    fn conditions(&self) -> Vec<ClusterCondition> {
        vec![self.available()]
    }
}

pub fn compute_conditions<T: ConditionBuilder>(condition_builder: &[T]) -> Vec<ClusterCondition> {
    let mut current_conditions = BTreeMap::<ClusterConditionType, ClusterCondition>::new();
    for cb in condition_builder {
        let cb_conditions: HashMap<ClusterConditionType, ClusterCondition> = cb
            .conditions()
            .iter()
            .map(|c| (c.type_.clone(), c.clone()))
            .collect();

        for (current_condition_type, cb_condition) in cb_conditions {
            let current_condition = current_conditions.get(&current_condition_type);

            let next_condition = if let Some(current) = current_condition {
                if current.status > cb_condition.status {
                    current
                } else {
                    &cb_condition
                }
            } else {
                &cb_condition
            };

            current_conditions.insert(current_condition_type, next_condition.clone());
        }
    }

    current_conditions.values().cloned().collect()
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
