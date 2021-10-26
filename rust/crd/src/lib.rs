pub mod commands;
pub mod discovery;
pub mod error;

#[deprecated(note = "The util module has been renamed to discovery, please use this instead.")]
pub use discovery as util;

use crate::commands::{Restart, Start, Stop};

use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::json;
use stackable_operator::command::{CommandRef, HasCommands, HasRoleRestartOrder};
use stackable_operator::controller::HasOwned;
use stackable_operator::crd::HasApplication;
use stackable_operator::identity::PodToNodeMapping;
use stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use stackable_operator::k8s_openapi::schemars::_serde_json::Value;
use stackable_operator::kube::api::ApiResource;
use stackable_operator::kube::CustomResource;
use stackable_operator::kube::CustomResourceExt;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::Role;
use stackable_operator::schemars::{self, JsonSchema};
use stackable_operator::status::{
    ClusterExecutionStatus, Conditions, HasClusterExecutionStatus, HasCurrentCommand, Status,
    Versioned,
};
use stackable_operator::versioning::{ProductVersion, Versioning, VersioningState};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use strum_macros::Display;
use strum_macros::EnumIter;

pub const APP_NAME: &str = "zookeeper";
pub const MANAGED_BY: &str = "zookeeper-operator";

pub const CLIENT_PORT_PROPERTY: &str = "clientPort";
pub const DATA_DIR: &str = "dataDir";
pub const INIT_LIMIT: &str = "initLimit";
pub const SYNC_LIMIT: &str = "syncLimit";
pub const TICK_TIME: &str = "tickTime";
pub const METRICS_PORT_PROPERTY: &str = "metricsPort";
pub const ADMIN_PORT_PROPERTY: &str = "admin.serverPort";

pub const CONFIG_MAP_TYPE_DATA: &str = "data";
pub const CONFIG_MAP_TYPE_ID: &str = "id";

pub const CLIENT_PORT: &str = "client";
pub const ADMIN_PORT: &str = "admin";
pub const METRICS_PORT: &str = "metrics";

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "zookeeper.stackable.tech",
    version = "v1alpha1",
    kind = "ZookeeperCluster",
    plural = "zookeeperclusters",
    shortname = "zk",
    namespaced,
    kube_core = "stackable_operator::kube::core",
    k8s_openapi = "stackable_operator::k8s_openapi",
    schemars = "stackable_operator::schemars"
)]
#[kube(status = "ZookeeperClusterStatus")]
pub struct ZookeeperClusterSpec {
    pub version: ZookeeperVersion,
    pub servers: Role<ZookeeperConfig>,
}

#[derive(
    Clone, Debug, Deserialize, Display, EnumIter, Eq, Hash, JsonSchema, PartialEq, Serialize,
)]
pub enum ZookeeperRole {
    #[strum(serialize = "server")]
    Server,
}

impl Status<ZookeeperClusterStatus> for ZookeeperCluster {
    fn status(&self) -> &Option<ZookeeperClusterStatus> {
        &self.status
    }
    fn status_mut(&mut self) -> &mut Option<ZookeeperClusterStatus> {
        &mut self.status
    }
}

impl HasRoleRestartOrder for ZookeeperCluster {
    fn get_role_restart_order() -> Vec<String> {
        vec![ZookeeperRole::Server.to_string()]
    }
}

impl HasCommands for ZookeeperCluster {
    fn get_command_types() -> Vec<ApiResource> {
        vec![
            Start::api_resource(),
            Stop::api_resource(),
            Restart::api_resource(),
        ]
    }
}

impl HasOwned for ZookeeperCluster {
    fn owned_objects() -> Vec<&'static str> {
        vec![Restart::crd_name(), Start::crd_name(), Stop::crd_name()]
    }
}

impl HasApplication for ZookeeperCluster {
    fn get_application_name() -> &'static str {
        APP_NAME
    }
}

impl HasClusterExecutionStatus for ZookeeperCluster {
    fn cluster_execution_status(&self) -> Option<ClusterExecutionStatus> {
        self.status
            .as_ref()
            .and_then(|status| status.cluster_execution_status.clone())
    }

    fn cluster_execution_status_patch(&self, execution_status: &ClusterExecutionStatus) -> Value {
        json!({ "clusterExecutionStatus": execution_status })
    }
}

// TODO: These all should be "Property" Enums that can be either simple or complex where complex allows forcing/ignoring errors and/or warnings
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperConfig {
    pub client_port: Option<u16>,
    pub data_dir: Option<String>,
    pub init_limit: Option<u32>,
    pub sync_limit: Option<u32>,
    pub tick_time: Option<u32>,
    pub metrics_port: Option<u16>,
    pub admin_port: Option<u16>,
}

impl Configuration for ZookeeperConfig {
    type Configurable = ZookeeperCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();
        if let Some(metrics_port) = self.metrics_port {
            result.insert(
                METRICS_PORT_PROPERTY.to_string(),
                Some(metrics_port.to_string()),
            );
        }
        Ok(result)
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();
        if let Some(client_port) = &self.client_port {
            result.insert(
                CLIENT_PORT_PROPERTY.to_string(),
                Some(client_port.to_string()),
            );
        }
        if let Some(data_dir) = &self.data_dir {
            result.insert(DATA_DIR.to_string(), Some(data_dir.clone()));
        }
        if let Some(init_limit) = self.init_limit {
            result.insert(INIT_LIMIT.to_string(), Some(init_limit.to_string()));
        }
        if let Some(sync_limit) = self.sync_limit {
            result.insert(SYNC_LIMIT.to_string(), Some(sync_limit.to_string()));
        }
        if let Some(tick_time) = self.tick_time {
            result.insert(TICK_TIME.to_string(), Some(tick_time.to_string()));
        }
        if let Some(admin_port) = self.admin_port {
            result.insert(
                ADMIN_PORT_PROPERTY.to_string(),
                Some(admin_port.to_string()),
            );
        }
        Ok(result)
    }
}

#[allow(non_camel_case_types)]
#[derive(
    Clone,
    Debug,
    Deserialize,
    Eq,
    JsonSchema,
    PartialEq,
    Serialize,
    strum_macros::Display,
    strum_macros::EnumString,
)]
pub enum ZookeeperVersion {
    #[serde(rename = "3.4.14")]
    #[strum(serialize = "3.4.14")]
    v3_4_14,

    #[serde(rename = "3.5.8")]
    #[strum(serialize = "3.5.8")]
    v3_5_8,
}

impl ZookeeperVersion {
    pub fn package_name(&self) -> String {
        match self {
            ZookeeperVersion::v3_4_14 => {
                format!("zookeeper-{}", self.to_string())
            }
            ZookeeperVersion::v3_5_8 => {
                format!("apache-zookeeper-{}-bin", self.to_string())
            }
        }
    }
}

impl Versioning for ZookeeperVersion {
    fn versioning_state(&self, other: &Self) -> VersioningState {
        let from_version = match Version::parse(&self.to_string()) {
            Ok(v) => v,
            Err(e) => {
                return VersioningState::Invalid(format!(
                    "Could not parse [{}] to SemVer: {}",
                    self.to_string(),
                    e.to_string()
                ))
            }
        };

        let to_version = match Version::parse(&other.to_string()) {
            Ok(v) => v,
            Err(e) => {
                return VersioningState::Invalid(format!(
                    "Could not parse [{}] to SemVer: {}",
                    other.to_string(),
                    e.to_string()
                ))
            }
        };

        match to_version.cmp(&from_version) {
            Ordering::Greater => VersioningState::ValidUpgrade,
            Ordering::Less => VersioningState::ValidDowngrade,
            Ordering::Equal => VersioningState::NoOp,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperClusterStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(schema_with = "stackable_operator::conditions::conditions_schema")]
    pub conditions: Vec<Condition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<ProductVersion<ZookeeperVersion>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<PodToNodeMapping>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_command: Option<CommandRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_execution_status: Option<ClusterExecutionStatus>,
}

impl Versioned<ZookeeperVersion> for ZookeeperClusterStatus {
    fn version(&self) -> &Option<ProductVersion<ZookeeperVersion>> {
        &self.version
    }
    fn version_mut(&mut self) -> &mut Option<ProductVersion<ZookeeperVersion>> {
        &mut self.version
    }
}

impl Conditions for ZookeeperClusterStatus {
    fn conditions(&self) -> &[Condition] {
        self.conditions.as_slice()
    }
    fn conditions_mut(&mut self) -> &mut Vec<Condition> {
        &mut self.conditions
    }
}

impl HasCurrentCommand for ZookeeperClusterStatus {
    fn current_command(&self) -> Option<CommandRef> {
        self.current_command.clone()
    }
    fn set_current_command(&mut self, command: CommandRef) {
        self.current_command = Some(command);
    }
    fn clear_current_command(&mut self) {
        self.current_command = None
    }
    fn tracking_location() -> &'static str {
        "/status/currentCommand"
    }
}

#[cfg(test)]
mod tests {
    use crate::ZookeeperVersion;
    use stackable_operator::versioning::{Versioning, VersioningState};
    use std::str::FromStr;

    #[test]
    fn test_zookeeper_version_versioning() {
        assert_eq!(
            ZookeeperVersion::v3_4_14.versioning_state(&ZookeeperVersion::v3_5_8),
            VersioningState::ValidUpgrade
        );
        assert_eq!(
            ZookeeperVersion::v3_5_8.versioning_state(&ZookeeperVersion::v3_4_14),
            VersioningState::ValidDowngrade
        );
        assert_eq!(
            ZookeeperVersion::v3_4_14.versioning_state(&ZookeeperVersion::v3_4_14),
            VersioningState::NoOp
        );
    }

    #[test]
    fn test_version_conversion() {
        ZookeeperVersion::from_str("3.4.14").unwrap();
        ZookeeperVersion::from_str("3.5.8").unwrap();
        ZookeeperVersion::from_str("1.2.3").unwrap_err();
    }

    #[test]
    fn test_package_name() {
        assert_eq!(
            ZookeeperVersion::v3_4_14.package_name(),
            format!("zookeeper-{}", ZookeeperVersion::v3_4_14.to_string())
        );
        assert_eq!(
            ZookeeperVersion::v3_5_8.package_name(),
            format!(
                "apache-zookeeper-{}-bin",
                ZookeeperVersion::v3_5_8.to_string()
            )
        );
    }
}
