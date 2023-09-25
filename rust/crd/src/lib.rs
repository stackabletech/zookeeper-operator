pub mod affinity;
pub mod authentication;
pub mod security;
pub mod tls;

use crate::authentication::ZookeeperAuthentication;
use crate::tls::ZookeeperTls;

use affinity::get_affinity;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        cluster_operation::ClusterOperation,
        product_image_selection::ProductImage,
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            PvcConfig, PvcConfigFragment, Resources, ResourcesFragment,
        },
    },
    config::{fragment, fragment::Fragment, fragment::ValidationError, merge::Merge},
    crd::ClusterRef,
    k8s_openapi::{
        api::core::v1::{PersistentVolumeClaim, ResourceRequirements},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::{runtime::reflector::ObjectRef, CustomResource, ResourceExt},
    memory::{BinaryMultiple, MemoryQuantity},
    product_config_utils::{ConfigError, Configuration},
    product_logging::{self, spec::Logging},
    role_utils::{Role, RoleConfig, RoleGroup, RoleGroupRef},
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
};
use std::{collections::BTreeMap, str::FromStr};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

pub const APP_NAME: &str = "zookeeper";
pub const OPERATOR_NAME: &str = "zookeeper.stackable.tech";

pub const ZOOKEEPER_PROPERTIES_FILE: &str = "zoo.cfg";
pub const JVM_SECURITY_PROPERTIES_FILE: &str = "security.properties";

pub const METRICS_PORT: u16 = 9505;

pub const STACKABLE_DATA_DIR: &str = "/stackable/data";
pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";
pub const STACKABLE_LOG_CONFIG_DIR: &str = "/stackable/log_config";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
pub const STACKABLE_RW_CONFIG_DIR: &str = "/stackable/rwconfig";

pub const LOGBACK_CONFIG_FILE: &str = "logback.xml";
pub const LOG4J_CONFIG_FILE: &str = "log4j.properties";

pub const ZOOKEEPER_LOG_FILE: &str = "zookeeper.log4j.xml";

pub const MAX_ZK_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};
pub const MAX_PREPARE_LOG_FILE_SIZE: MemoryQuantity = MemoryQuantity {
    value: 1.0,
    unit: BinaryMultiple::Mebi,
};

const JVM_HEAP_FACTOR: f32 = 0.8;

pub const DOCKER_IMAGE_BASE_NAME: &str = "zookeeper";

mod built_info {
    pub const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
}

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
    #[snafu(display("unknown role {role}. Should be one of {roles:?}"))]
    UnknownZookeeperRole {
        source: strum::ParseError,
        role: String,
        roles: Vec<String>,
    },
    #[snafu(display("the role {role} is not defined"))]
    CannotRetrieveZookeeperRole { role: String },
    #[snafu(display("the role group {role_group} is not defined"))]
    CannotRetrieveZookeeperRoleGroup { role_group: String },
    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },
    #[snafu(display("invalid java heap config - missing default or value in crd?"))]
    InvalidJavaHeapConfig,
    #[snafu(display("failed to convert java heap config to unit [{unit}]"))]
    FailedToConvertJavaHeap {
        source: stackable_operator::error::Error,
        unit: String,
    },
}

/// A cluster of ZooKeeper nodes
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "zookeeper.stackable.tech",
    version = "v1alpha1",
    kind = "ZookeeperCluster",
    plural = "zookeeperclusters",
    shortname = "zk",
    status = "ZookeeperClusterStatus",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperClusterSpec {
    /// Global ZooKeeper cluster configuration that applies to all roles and role groups.
    #[serde(default = "cluster_config_default")]
    pub cluster_config: ZookeeperClusterConfig,
    /// Cluster operations like pause reconciliation or cluster stop.
    #[serde(default)]
    pub cluster_operation: ClusterOperation,
    /// Desired ZooKeeper image to use.
    pub image: ProductImage,
    /// ZooKeeper server configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub servers: Option<Role<ZookeeperConfigFragment>>,
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperClusterConfig {
    /// Authentication class settings for ZooKeeper like mTLS authentication.
    #[serde(default)]
    pub authentication: Vec<ZookeeperAuthentication>,
    /// Logging options for ZooKeeper.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logging: Option<ZookeeperLogging>,
    /// TLS encryption settings for ZooKeeper (server, quorum).
    #[serde(
        default = "tls::default_zookeeper_tls",
        skip_serializing_if = "Option::is_none"
    )]
    pub tls: Option<ZookeeperTls>,
    /// This field controls which type of Service the Operator creates for this ZookeeperCluster:
    ///
    /// * cluster-internal: Use a ClusterIP service
    ///
    /// * external-unstable: Use a NodePort service
    ///
    /// This is a temporary solution with the goal to keep yaml manifests forward compatible.
    /// In the future, this setting will control which ListenerClass <https://docs.stackable.tech/home/stable/listener-operator/listenerclass.html>
    /// will be used to expose the service, and ListenerClass names will stay the same, allowing for a non-breaking change.
    #[serde(default)]
    pub listener_class: CurrentlySupportedListenerClasses,
}

fn cluster_config_default() -> ZookeeperClusterConfig {
    ZookeeperClusterConfig {
        authentication: vec![],
        logging: None,
        tls: tls::default_zookeeper_tls(),
        listener_class: CurrentlySupportedListenerClasses::default(),
    }
}

// TODO: Temporary solution until listener-operator is finished
#[derive(Clone, Debug, Default, Display, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum CurrentlySupportedListenerClasses {
    #[default]
    #[serde(rename = "cluster-internal")]
    ClusterInternal,
    #[serde(rename = "external-unstable")]
    ExternalUnstable,
}

impl CurrentlySupportedListenerClasses {
    pub fn k8s_service_type(&self) -> String {
        match self {
            CurrentlySupportedListenerClasses::ClusterInternal => "ClusterIP".to_string(),
            CurrentlySupportedListenerClasses::ExternalUnstable => "NodePort".to_string(),
        }
    }
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperLogging {
    /// Name of the Vector discovery ConfigMap.
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct ZookeeperConfig {
    pub init_limit: Option<u32>,
    pub sync_limit: Option<u32>,
    pub tick_time: Option<u32>,
    pub myid_offset: u16,
    #[fragment_attrs(serde(default))]
    pub resources: Resources<ZookeeperStorageConfig, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<Container>,
    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,
}

#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Fragment)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct ZookeeperStorageConfig {
    #[fragment_attrs(serde(default))]
    pub data: PvcConfig,
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    Eq,
    EnumIter,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "camelCase")]
pub enum Container {
    Prepare,
    Vector,
    Zookeeper,
}

impl ZookeeperConfig {
    pub const INIT_LIMIT: &'static str = "initLimit";
    pub const SYNC_LIMIT: &'static str = "syncLimit";
    pub const TICK_TIME: &'static str = "tickTime";
    pub const DATA_DIR: &'static str = "dataDir";

    pub const MYID_OFFSET: &'static str = "MYID_OFFSET";
    pub const SERVER_JVMFLAGS: &'static str = "SERVER_JVMFLAGS";
    pub const ZK_SERVER_HEAP: &'static str = "ZK_SERVER_HEAP";

    fn default_server_config(cluster_name: &str, role: &ZookeeperRole) -> ZookeeperConfigFragment {
        ZookeeperConfigFragment {
            init_limit: None,
            sync_limit: None,
            tick_time: None,
            myid_offset: Some(1),
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("200m".to_owned())),
                    max: Some(Quantity("800m".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("512Mi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: ZookeeperStorageConfigFragment {
                    data: PvcConfigFragment {
                        capacity: Some(Quantity("1Gi".to_owned())),
                        storage_class: None,
                        selectors: None,
                    },
                },
            },
            logging: product_logging::spec::default_logging(),
            affinity: get_affinity(cluster_name, role),
        }
    }
}

impl Configuration for ZookeeperConfigFragment {
    type Configurable = ZookeeperCluster;

    fn compute_env(
        &self,
        resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let logging_framework =
            resource
                .logging_framework()
                .map_err(|e| ConfigError::InvalidConfiguration {
                    reason: e.to_string(),
                })?;
        let jvm_flags = [
            format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/server.yaml"),
            match logging_framework {
                LoggingFramework::LOG4J => format!("-Dlog4j.configuration=file:{STACKABLE_LOG_CONFIG_DIR}/{LOG4J_CONFIG_FILE}"),
                LoggingFramework::LOGBACK => format!("-Dlogback.configurationFile={STACKABLE_LOG_CONFIG_DIR}/{LOGBACK_CONFIG_FILE}"),
            },
            format!("-Djava.security.properties={STACKABLE_CONFIG_DIR}/{JVM_SECURITY_PROPERTIES_FILE}"),
        ].join(" ");
        Ok([
            (
                ZookeeperConfig::MYID_OFFSET.to_string(),
                self.myid_offset
                    .or(ZookeeperConfig::default_server_config(
                        &resource.name_any(),
                        &ZookeeperRole::Server,
                    )
                    .myid_offset)
                    .map(|myid_offset| myid_offset.to_string()),
            ),
            (
                ZookeeperConfig::SERVER_JVMFLAGS.to_string(),
                Some(jvm_flags),
            ),
        ]
        .into())
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
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();
        if file == ZOOKEEPER_PROPERTIES_FILE {
            if let Some(init_limit) = self.init_limit {
                result.insert(
                    ZookeeperConfig::INIT_LIMIT.to_string(),
                    Some(init_limit.to_string()),
                );
            }
            if let Some(sync_limit) = self.sync_limit {
                result.insert(
                    ZookeeperConfig::SYNC_LIMIT.to_string(),
                    Some(sync_limit.to_string()),
                );
            }
            if let Some(tick_time) = self.tick_time {
                result.insert(
                    ZookeeperConfig::TICK_TIME.to_string(),
                    Some(tick_time.to_string()),
                );
            }
            result.insert(
                ZookeeperConfig::DATA_DIR.to_string(),
                Some(STACKABLE_DATA_DIR.to_string()),
            );
        }

        Ok(result)
    }
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    JsonSchema,
    PartialEq,
    Serialize,
    EnumString,
)]
#[strum(serialize_all = "camelCase")]
pub enum ZookeeperRole {
    #[strum(serialize = "server")]
    Server,
}

impl ZookeeperRole {
    pub fn roles() -> Vec<String> {
        let mut roles = vec![];
        for role in Self::iter() {
            roles.push(role.to_string())
        }
        roles
    }
}

#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperClusterStatus {
    /// An opaque value that changes every time a discovery detail does
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_hash: Option<String>,
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

impl HasStatusCondition for ZookeeperCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

pub enum LoggingFramework {
    LOG4J,
    LOGBACK,
}

impl ZookeeperCluster {
    pub fn logging_framework(&self) -> Result<LoggingFramework, Error> {
        let version = self
            .spec
            .image
            .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::CARGO_PKG_VERSION)
            .product_version;
        let zookeeper_versions_with_log4j = [
            "1.", "2.", "3.0.", "3.1.", "3.2.", "3.3.", "3.4.", "3.5.", "3.6.", "3.7.",
        ];

        let framework = if zookeeper_versions_with_log4j
            .into_iter()
            .any(|prefix| version.starts_with(prefix))
        {
            LoggingFramework::LOG4J
        } else {
            LoggingFramework::LOGBACK
        };

        Ok(framework)
    }

    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn server_role_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }

    /// The fully-qualified domain name of the role-level load-balanced Kubernetes `Service`
    pub fn server_role_service_fqdn(&self) -> Option<String> {
        Some(format!(
            "{}.{}.svc.cluster.local",
            self.server_role_service_name()?,
            self.metadata.namespace.as_ref()?
        ))
    }

    /// Returns a reference to the role. Raises an error if the role is not defined.
    pub fn role(
        &self,
        role_variant: &ZookeeperRole,
    ) -> Result<&Role<ZookeeperConfigFragment>, Error> {
        match role_variant {
            ZookeeperRole::Server => self.spec.servers.as_ref(),
        }
        .with_context(|| CannotRetrieveZookeeperRoleSnafu {
            role: role_variant.to_string(),
        })
    }

    /// Returns a reference to the role group. Raises an error if the role or role group are not defined.
    pub fn rolegroup(
        &self,
        rolegroup_ref: &RoleGroupRef<ZookeeperCluster>,
    ) -> Result<RoleGroup<ZookeeperConfigFragment>, Error> {
        let role_variant = ZookeeperRole::from_str(&rolegroup_ref.role).with_context(|_| {
            UnknownZookeeperRoleSnafu {
                role: rolegroup_ref.role.to_owned(),
                roles: ZookeeperRole::roles(),
            }
        })?;
        let role = self.role(&role_variant)?;
        role.role_groups
            .get(&rolegroup_ref.role_group)
            .with_context(|| CannotRetrieveZookeeperRoleGroupSnafu {
                role_group: rolegroup_ref.role_group.to_owned(),
            })
            .cloned()
    }

    /// Metadata about a server rolegroup
    pub fn server_rolegroup_ref(
        &self,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<ZookeeperCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: ZookeeperRole::Server.to_string(),
            role_group: group_name.into(),
        }
    }

    pub fn role_config(&self, role: &ZookeeperRole) -> Option<&RoleConfig> {
        match role {
            ZookeeperRole::Server => self.spec.servers.as_ref().map(|s| &s.role_config),
        }
    }

    /// List all pods expected to form the cluster
    ///
    /// We try to predict the pods here rather than looking at the current cluster state in order to
    /// avoid instance churn. For example, regenerating zoo.cfg based on the cluster state would lead to
    /// a lot of spurious restarts, as well as opening us up to dangerous split-brain conditions because
    /// the pods have inconsistent snapshots of which servers they should expect to be in quorum.
    pub fn pods(&self) -> Result<impl Iterator<Item = ZookeeperPodRef> + '_, Error> {
        let ns = self.metadata.namespace.clone().context(NoNamespaceSnafu)?;
        let role_groups = self
            .spec
            .servers
            .iter()
            .flat_map(|role| &role.role_groups)
            // Order rolegroups consistently, to avoid spurious downstream rewrites
            .collect::<BTreeMap<_, _>>();
        let mut pod_refs = Vec::new();
        for (rolegroup_name, rolegroup) in role_groups {
            let rolegroup_ref = self.server_rolegroup_ref(rolegroup_name);
            let myid_offset = self
                .merged_config(&ZookeeperRole::Server, &rolegroup_ref)?
                .myid_offset;
            let ns = ns.clone();
            // In case no replicas are specified we default to 1
            for i in 0..rolegroup.replicas.unwrap_or(1) {
                pod_refs.push(ZookeeperPodRef {
                    namespace: ns.clone(),
                    role_group_service_name: rolegroup_ref.object_name(),
                    pod_name: format!("{}-{}", rolegroup_ref.object_name(), i),
                    zookeeper_myid: i + myid_offset,
                });
            }
        }
        Ok(pod_refs.into_iter())
    }

    pub fn merged_config(
        &self,
        role: &ZookeeperRole,
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> Result<ZookeeperConfig, Error> {
        // Initialize the result with all default values as baseline
        let conf_defaults = ZookeeperConfig::default_server_config(&self.name_any(), role);

        // Retrieve role resource config
        let role = self.role(role)?;
        let mut conf_role = role.config.config.to_owned();

        // Retrieve rolegroup specific resource config
        let role_group = self.rolegroup(rolegroup_ref)?;
        let mut conf_role_group = role_group.config.config;

        if let Some(RoleGroup {
            selector: Some(selector),
            ..
        }) = role.role_groups.get(&rolegroup_ref.role_group)
        {
            // Migrate old `selector` attribute, see ADR 26 affinities.
            // TODO Can be removed after support for the old `selector` field is dropped.
            #[allow(deprecated)]
            conf_role_group.affinity.add_legacy_selector(selector);
        }

        // Merge more specific configs into default config
        // Hierarchy is:
        // 1. RoleGroup
        // 2. Role
        // 3. Default
        conf_role.merge(&conf_defaults);
        conf_role_group.merge(&conf_role);

        fragment::validate(conf_role_group).context(FragmentValidationFailureSnafu)
    }

    pub fn logging(
        &self,
        role: &ZookeeperRole,
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> Result<Logging<Container>, Error> {
        let config = self.merged_config(role, rolegroup_ref)?;
        Ok(config.logging)
    }

    /// Build the [`PersistentVolumeClaim`]s and [`ResourceRequirements`] for the given `rolegroup_ref`.
    /// These can be defined at the role or rolegroup level and as usual, the
    /// following precedence rules are implemented:
    /// 1. group pvc
    /// 2. role pvc
    /// 3. a default PVC with 1Gi capacity
    pub fn resources(
        &self,
        role: &ZookeeperRole,
        rolegroup_ref: &RoleGroupRef<ZookeeperCluster>,
    ) -> Result<(Vec<PersistentVolumeClaim>, ResourceRequirements), Error> {
        let config = self.merged_config(role, rolegroup_ref)?;
        let resources: Resources<ZookeeperStorageConfig, NoRuntimeLimits> = config.resources;

        let data_pvc = resources
            .storage
            .data
            .build_pvc("data", Some(vec!["ReadWriteOnce"]));

        Ok((vec![data_pvc], resources.into()))
    }

    pub fn heap_limits(&self, resources: &ResourceRequirements) -> Result<Option<u32>, Error> {
        let memory_limit = MemoryQuantity::try_from(
            resources
                .limits
                .as_ref()
                .and_then(|limits| limits.get("memory"))
                .context(InvalidJavaHeapConfigSnafu)?,
        )
        .context(FailedToConvertJavaHeapSnafu {
            unit: BinaryMultiple::Mebi.to_java_memory_unit(),
        })?;

        Ok(Some(
            (memory_limit * JVM_HEAP_FACTOR)
                .scale_to(BinaryMultiple::Mebi)
                .floor()
                .value as u32,
        ))
    }

    pub fn num_servers(&self) -> u16 {
        self.spec
            .servers
            .iter()
            .flat_map(|s| s.role_groups.values())
            .map(|rg| rg.replicas.unwrap_or(1))
            .sum()
    }
}

/// Reference to a single `Pod` that is a component of a [`ZookeeperCluster`]
///
/// Used for service discovery.
pub struct ZookeeperPodRef {
    pub namespace: String,
    pub role_group_service_name: String,
    pub pod_name: String,
    pub zookeeper_myid: u16,
}

impl ZookeeperPodRef {
    pub fn fqdn(&self) -> String {
        format!(
            "{}.{}.{}.svc.cluster.local",
            self.pod_name, self.role_group_service_name, self.namespace
        )
    }
}

/// A claim for a single ZooKeeper ZNode tree (filesystem node)
///
/// A `ConfigMap` will automatically be created with the same name, containing the connection string in the field `ZOOKEEPER`.
/// Each `ZookeeperZnode` gets an isolated ZNode chroot, which the `ZOOKEEPER` automatically contains.
/// All data inside of this chroot will be deleted when the corresponding `ZookeeperZnode` is.
///
/// `ZookeeperZnode` is *not* designed to manage the contents of this ZNode. Instead, it should be used to create a chroot
/// for an installation of an application to work inside. Initializing the contents is the responsibility of the application.
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "zookeeper.stackable.tech",
    version = "v1alpha1",
    kind = "ZookeeperZnode",
    plural = "zookeeperznodes",
    shortname = "zno",
    shortname = "znode",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperZnodeSpec {
    #[serde(default)]
    pub cluster_ref: ClusterRef<ZookeeperCluster>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_server_secret_class(zk: &ZookeeperCluster) -> Option<&str> {
        zk.spec
            .cluster_config
            .tls
            .as_ref()
            .and_then(|tls| tls.server_secret_class.as_deref())
    }

    fn get_quorum_secret_class(zk: &ZookeeperCluster) -> &str {
        zk.spec
            .cluster_config
            .tls
            .as_ref()
            .unwrap()
            .quorum_secret_class
            .as_str()
    }

    #[test]
    fn test_client_tls() {
        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.8.1"
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            get_server_secret_class(&zookeeper),
            tls::server_tls_default().as_deref()
        );
        assert_eq!(
            get_quorum_secret_class(&zookeeper),
            tls::quorum_tls_default().as_str()
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.8.1"
          clusterConfig:
            tls:
              serverSecretClass: simple-zookeeper-client-tls
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");

        assert_eq!(
            get_server_secret_class(&zookeeper),
            Some("simple-zookeeper-client-tls")
        );
        assert_eq!(
            get_quorum_secret_class(&zookeeper),
            tls::quorum_tls_default().as_str()
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.8.1"
          clusterConfig:
            tls:
              serverSecretClass: null
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(get_server_secret_class(&zookeeper), None);
        assert_eq!(
            get_quorum_secret_class(&zookeeper),
            tls::quorum_tls_default().as_str()
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.8.1"
          clusterConfig:
            tls:
              quorumSecretClass: simple-zookeeper-quorum-tls
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            get_server_secret_class(&zookeeper),
            tls::server_tls_default().as_deref()
        );
        assert_eq!(
            get_quorum_secret_class(&zookeeper),
            "simple-zookeeper-quorum-tls"
        );
    }

    #[test]
    fn test_quorum_tls() {
        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.8.1"
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");

        assert_eq!(
            get_server_secret_class(&zookeeper),
            tls::server_tls_default().as_deref()
        );
        assert_eq!(
            get_quorum_secret_class(&zookeeper),
            tls::quorum_tls_default()
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.8.1"
          clusterConfig:
            tls:
              quorumSecretClass: simple-zookeeper-quorum-tls
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            get_server_secret_class(&zookeeper),
            tls::server_tls_default().as_deref()
        );
        assert_eq!(
            get_quorum_secret_class(&zookeeper),
            "simple-zookeeper-quorum-tls"
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.8.1"
          clusterConfig:
            tls:
              serverSecretClass: simple-zookeeper-server-tls
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            get_server_secret_class(&zookeeper),
            Some("simple-zookeeper-server-tls")
        );
        assert_eq!(
            get_quorum_secret_class(&zookeeper),
            tls::quorum_tls_default().as_str()
        );
    }
}
