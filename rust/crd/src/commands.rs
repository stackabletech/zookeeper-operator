use crate::ZookeeperRole;
use k8s_openapi::chrono::{DateTime, FixedOffset, Utc};
use kube::api::ApiResource;
use kube::{CustomResource, CustomResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use stackable_operator::command::{CanBeRolling, HasRoles};
use stackable_operator::command_controller::Command;

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.zookeeper.stackable.tech",
    version = "v1alpha1",
    kind = "Restart",
    namespaced
)]
#[kube(status = "CommandStatus")]
#[serde(rename_all = "camelCase")]
pub struct RestartCommandSpec {
    pub name: String,
    pub rolling: bool,
    pub roles: Option<Vec<ZookeeperRole>>,
    // TODO: Change these to some form of Time type
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

impl Command for Restart {
    fn get_owner_name(&self) -> String {
        self.spec.name.clone()
    }

    fn start(&mut self) {
        self.spec.started_at = Some(Utc::now().to_rfc3339());
    }

    fn done(&mut self) {
        self.spec.finished_at = Some(Utc::now().to_rfc3339());
    }

    fn start_time(&self) -> Option<DateTime<FixedOffset>> {
        self.spec
            .started_at
            .as_ref()
            .map(|time_string| DateTime::<FixedOffset>::parse_from_rfc3339(time_string).unwrap())
    }

    fn get_start_patch(&self) -> Value {
        json!({
            "spec": {
                "startedAt": &self.spec.started_at
            }
        })
    }
}

impl CanBeRolling for Restart {
    fn is_rolling(&self) -> bool {
        self.spec.rolling
    }
}

impl HasRoles for Restart {
    fn get_role_order(&self) -> Option<Vec<String>> {
        self.spec
            .roles
            .clone()
            .map(|roles| roles.into_iter().map(|role| role.to_string()).collect())
    }
}

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.zookeeper.stackable.tech",
    version = "v1alpha1",
    kind = "Start",
    namespaced
)]
#[kube(status = "CommandStatus")]
#[serde(rename_all = "camelCase")]
pub struct StartCommandSpec {
    pub name: String,
}
impl Command for Start {
    fn get_owner_name(&self) -> String {
        self.spec.name.clone()
    }

    fn start(&mut self) {
        todo!()
    }

    fn done(&mut self) {
        todo!()
    }

    fn start_time(&self) -> Option<DateTime<FixedOffset>> {
        todo!()
    }

    fn get_start_patch(&self) -> Value {
        todo!()
    }
}

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.zookeeper.stackable.tech",
    version = "v1alpha1",
    kind = "Stop",
    namespaced
)]
#[kube(status = "CommandStatus")]
#[serde(rename_all = "camelCase")]
pub struct StopCommandSpec {
    pub name: String,
}

impl Command for Stop {
    fn get_owner_name(&self) -> String {
        self.spec.name.clone()
    }

    fn start(&mut self) {
        todo!()
    }

    fn done(&mut self) {
        todo!()
    }

    fn start_time(&self) -> Option<DateTime<FixedOffset>> {
        todo!()
    }
    fn get_start_patch(&self) -> Value {
        todo!()
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CommandStatus {}
