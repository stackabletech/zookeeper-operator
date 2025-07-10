use std::{collections::BTreeMap, str::FromStr};

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
    config::{
        fragment::{self, Fragment, ValidationError},
        merge::Merge,
    },
    crd::ClusterRef,
    k8s_openapi::{
        api::core::v1::{PersistentVolumeClaim, ResourceRequirements},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::{CustomResource, ResourceExt, runtime::reflector::ObjectRef},
    memory::{BinaryMultiple, MemoryQuantity},
    product_config_utils::{self, Configuration},
    product_logging::{self, spec::Logging},
    role_utils::{GenericRoleConfig, JavaCommonConfig, Role, RoleGroup, RoleGroupRef},
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
    time::Duration,
    utils::cluster_info::KubernetesClusterInfo,
    versioned::versioned,
};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

use crate::{
    crd::{affinity::get_affinity, v1alpha1::ZookeeperServerRoleConfig},
    discovery::build_role_group_headless_service_name,
    listener::role_listener_name,
};

pub mod affinity;
pub mod authentication;
pub mod security;
pub mod tls;

pub const APP_NAME: &str = "zookeeper";
pub const OPERATOR_NAME: &str = "zookeeper.stackable.tech";

pub const ZOOKEEPER_PROPERTIES_FILE: &str = "zoo.cfg";
pub const JVM_SECURITY_PROPERTIES_FILE: &str = "security.properties";

pub const ZOOKEEPER_SERVER_PORT_NAME: &str = "zk";
pub const ZOOKEEPER_LEADER_PORT_NAME: &str = "zk-leader";
pub const ZOOKEEPER_LEADER_PORT: u16 = 2888;
pub const ZOOKEEPER_ELECTION_PORT_NAME: &str = "zk-election";
pub const ZOOKEEPER_ELECTION_PORT: u16 = 3888;

pub const JMX_METRICS_PORT_NAME: &str = "metrics";
pub const JMX_METRICS_PORT: u16 = 9505;
pub const METRICS_PROVIDER_HTTP_PORT_KEY: &str = "metricsProvider.httpPort";
pub const METRICS_PROVIDER_HTTP_PORT_NAME: &str = "native-metrics";
pub const METRICS_PROVIDER_HTTP_PORT: u16 = 7000;

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

pub const DOCKER_IMAGE_BASE_NAME: &str = "zookeeper";

const DEFAULT_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_minutes_unchecked(2);
pub const DEFAULT_LISTENER_CLASS: &str = "cluster-internal";

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
}

pub enum LoggingFramework {
    LOG4J,
    LOGBACK,
}

#[versioned(
    version(name = "v1alpha1"),
    crates(
        kube_core = "stackable_operator::kube::core",
        kube_client = "stackable_operator::kube::client",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars",
        versioned = "stackable_operator::versioned",
    )
)]
pub mod versioned {
    /// A ZooKeeper cluster stacklet. This resource is managed by the Stackable operator for Apache ZooKeeper.
    /// Find more information on how to use it and the resources that the operator generates in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/zookeeper/).
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[versioned(crd(
        group = "zookeeper.stackable.tech",
        plural = "zookeeperclusters",
        shortname = "zk",
        status = "v1alpha1::ZookeeperClusterStatus",
        namespaced,
    ))]
    #[serde(rename_all = "camelCase")]
    pub struct ZookeeperClusterSpec {
        /// Settings that affect all roles and role groups.
        /// The settings in the `clusterConfig` are cluster wide settings that do not need to be configurable at role or role group level.
        #[serde(default = "cluster_config_default")]
        pub cluster_config: ZookeeperClusterConfig,

        // no doc - it's in the struct.
        #[serde(default)]
        pub cluster_operation: ClusterOperation,

        // no doc - it's in the struct.
        pub image: ProductImage,

        // no doc - it's in the struct.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub servers:
            Option<Role<ZookeeperConfigFragment, ZookeeperServerRoleConfig, JavaCommonConfig>>,
    }

    #[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ZookeeperServerRoleConfig {
        #[serde(flatten)]
        pub common: GenericRoleConfig,

        /// This field controls which [ListenerClass](DOCS_BASE_URL_PLACEHOLDER/listener-operator/listenerclass.html) is used to expose the ZooKeeper servers.
        #[serde(default = "default_listener_class")]
        pub listener_class: String,
    }

    #[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ZookeeperClusterConfig {
        /// Authentication settings for ZooKeeper like mTLS authentication.
        /// Read more in the [authentication usage guide](DOCS_BASE_URL_PLACEHOLDER/zookeeper/usage_guide/authentication).
        #[serde(default)]
        pub authentication: Vec<authentication::v1alpha1::ZookeeperAuthentication>,

        /// Name of the Vector aggregator [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery).
        /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
        /// Follow the [logging tutorial](DOCS_BASE_URL_PLACEHOLDER/tutorials/logging-vector-aggregator)
        /// to learn how to configure log aggregation with Vector.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub vector_aggregator_config_map_name: Option<String>,

        /// TLS encryption settings for ZooKeeper (server, quorum).
        /// Read more in the [encryption usage guide](DOCS_BASE_URL_PLACEHOLDER/zookeeper/usage_guide/encryption).
        #[serde(
            default = "tls::default_zookeeper_tls",
            skip_serializing_if = "Option::is_none"
        )]
        pub tls: Option<tls::v1alpha1::ZookeeperTls>,
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

        /// Time period Pods have to gracefully shut down, e.g. `30m`, `1h` or `2d`. Consult the operator documentation for details.
        #[fragment_attrs(serde(default))]
        pub graceful_shutdown_timeout: Option<Duration>,

        /// Request secret (currently only autoTls certificates) lifetime from the secret operator, e.g. `7d`, or `30d`.
        /// This can be shortened by the `maxCertificateLifetime` setting on the SecretClass issuing the TLS certificate.
        #[fragment_attrs(serde(default))]
        pub requested_secret_lifetime: Option<Duration>,
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

    #[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ZookeeperClusterStatus {
        /// An opaque value that changes every time a discovery detail does
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub discovery_hash: Option<String>,
        #[serde(default)]
        pub conditions: Vec<ClusterCondition>,
    }

    /// A claim for a single ZooKeeper ZNode tree (filesystem node).
    ///
    /// A ConfigMap will automatically be created with the same name, containing the connection string in the field `ZOOKEEPER`.
    /// Each ZookeeperZnode gets an isolated ZNode chroot, which the `ZOOKEEPER` automatically contains.
    /// All data inside of this chroot will be deleted when the corresponding `ZookeeperZnode` is.
    ///
    /// `ZookeeperZnode` is *not* designed to manage the contents of this ZNode. Instead, it should be used to create a chroot
    /// for an installation of an application to work inside. Initializing the contents is the responsibility of the application.
    ///
    /// You can learn more about this in the
    /// [Isolating clients with ZNodes usage guide](DOCS_BASE_URL_PLACEHOLDER/zookeeper/usage_guide/isolating_clients_with_znodes).
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[versioned(crd(
        group = "zookeeper.stackable.tech",
        plural = "zookeeperznodes",
        shortname = "zno",
        shortname = "znode",
        status = "v1alpha1::ZookeeperZnodeStatus",
        namespaced,
    ))]
    #[serde(rename_all = "camelCase")]
    pub struct ZookeeperZnodeSpec {
        /// The reference to the ZookeeperCluster that this ZNode belongs to.
        #[serde(default)]
        pub cluster_ref: ClusterRef<ZookeeperCluster>,
    }

    #[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ZookeeperZnodeStatus {
        /// The absolute ZNode allocated to the ZookeeperZnode. This will typically be set by the operator.
        ///
        /// This can be set explicitly by an administrator, such as when restoring from a backup.
        pub znode_path: Option<String>,
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

/// Reference to a single `Pod` that is a component of a [`v1alpha1::ZookeeperCluster`]
///
/// Used for service discovery.
pub struct ZookeeperPodRef {
    pub namespace: String,
    pub role_group_headless_service_name: String,
    pub pod_name: String,
    pub zookeeper_myid: u16,
}

fn cluster_config_default() -> v1alpha1::ZookeeperClusterConfig {
    v1alpha1::ZookeeperClusterConfig {
        authentication: vec![],
        vector_aggregator_config_map_name: None,
        tls: tls::default_zookeeper_tls(),
    }
}

fn default_listener_class() -> String {
    DEFAULT_LISTENER_CLASS.to_owned()
}

impl Default for ZookeeperServerRoleConfig {
    fn default() -> Self {
        Self {
            listener_class: default_listener_class(),
            common: Default::default(),
        }
    }
}

impl v1alpha1::ZookeeperConfig {
    pub const DATA_DIR: &'static str = "dataDir";
    const DEFAULT_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);
    pub const INIT_LIMIT: &'static str = "initLimit";
    pub const MYID_OFFSET: &'static str = "MYID_OFFSET";
    pub const SYNC_LIMIT: &'static str = "syncLimit";
    pub const TICK_TIME: &'static str = "tickTime";

    fn default_server_config(
        cluster_name: &str,
        role: &ZookeeperRole,
    ) -> v1alpha1::ZookeeperConfigFragment {
        v1alpha1::ZookeeperConfigFragment {
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
                storage: v1alpha1::ZookeeperStorageConfigFragment {
                    data: PvcConfigFragment {
                        capacity: Some(Quantity("1Gi".to_owned())),
                        storage_class: None,
                        selectors: None,
                    },
                },
            },
            logging: product_logging::spec::default_logging(),
            affinity: get_affinity(cluster_name, role),
            graceful_shutdown_timeout: Some(DEFAULT_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT),
            requested_secret_lifetime: Some(Self::DEFAULT_SECRET_LIFETIME),
        }
    }
}

impl Configuration for v1alpha1::ZookeeperConfigFragment {
    type Configurable = v1alpha1::ZookeeperCluster;

    fn compute_env(
        &self,
        resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, product_config_utils::Error> {
        Ok([(
            v1alpha1::ZookeeperConfig::MYID_OFFSET.to_string(),
            self.myid_offset
                .or(v1alpha1::ZookeeperConfig::default_server_config(
                    &resource.name_any(),
                    &ZookeeperRole::Server,
                )
                .myid_offset)
                .map(|myid_offset| myid_offset.to_string()),
        )]
        .into())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, product_config_utils::Error> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, product_config_utils::Error> {
        let mut result = BTreeMap::new();
        if file == ZOOKEEPER_PROPERTIES_FILE {
            if let Some(init_limit) = self.init_limit {
                result.insert(
                    v1alpha1::ZookeeperConfig::INIT_LIMIT.to_string(),
                    Some(init_limit.to_string()),
                );
            }
            if let Some(sync_limit) = self.sync_limit {
                result.insert(
                    v1alpha1::ZookeeperConfig::SYNC_LIMIT.to_string(),
                    Some(sync_limit.to_string()),
                );
            }
            if let Some(tick_time) = self.tick_time {
                result.insert(
                    v1alpha1::ZookeeperConfig::TICK_TIME.to_string(),
                    Some(tick_time.to_string()),
                );
            }
            result.insert(
                v1alpha1::ZookeeperConfig::DATA_DIR.to_string(),
                Some(STACKABLE_DATA_DIR.to_string()),
            );
            result.insert(
                "metricsProvider.className".to_string(),
                Some(
                    "org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider".to_string(),
                ),
            );
            result.insert(
                METRICS_PROVIDER_HTTP_PORT_KEY.to_string(),
                Some(METRICS_PROVIDER_HTTP_PORT.to_string()),
            );
        }

        Ok(result)
    }
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

impl HasStatusCondition for v1alpha1::ZookeeperCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

impl ZookeeperPodRef {
    pub fn internal_fqdn(&self, cluster_info: &KubernetesClusterInfo) -> String {
        format!(
            "{pod_name}.{service_name}.{namespace}.svc.{cluster_domain}",
            pod_name = self.pod_name,
            service_name = self.role_group_headless_service_name,
            namespace = self.namespace,
            cluster_domain = cluster_info.cluster_domain
        )
    }
}

impl v1alpha1::ZookeeperCluster {
    pub fn logging_framework(&self) -> LoggingFramework {
        let version = self
            .spec
            .image
            .resolve(
                DOCKER_IMAGE_BASE_NAME,
                crate::crd::built_info::CARGO_PKG_VERSION,
            )
            .product_version;
        let zookeeper_versions_with_log4j = [
            "1.", "2.", "3.0.", "3.1.", "3.2.", "3.3.", "3.4.", "3.5.", "3.6.", "3.7.",
        ];

        if zookeeper_versions_with_log4j
            .into_iter()
            .any(|prefix| version.starts_with(prefix))
        {
            LoggingFramework::LOG4J
        } else {
            LoggingFramework::LOGBACK
        }
    }

    /// The fully-qualified domain name of the role-level [Listener]
    ///
    /// [Listener]: stackable_operator::crd::listener::v1alpha1::Listener
    pub fn server_role_listener_fqdn(
        &self,
        cluster_info: &KubernetesClusterInfo,
    ) -> Option<String> {
        Some(format!(
            "{role_listener_name}.{namespace}.svc.{cluster_domain}",
            role_listener_name = role_listener_name(self, &ZookeeperRole::Server),
            namespace = self.metadata.namespace.as_ref()?,
            cluster_domain = cluster_info.cluster_domain
        ))
    }

    /// Returns a reference to the role. Raises an error if the role is not defined.
    pub fn role(
        &self,
        role_variant: &ZookeeperRole,
    ) -> Result<
        &Role<v1alpha1::ZookeeperConfigFragment, ZookeeperServerRoleConfig, JavaCommonConfig>,
        Error,
    > {
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
        rolegroup_ref: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
    ) -> Result<RoleGroup<v1alpha1::ZookeeperConfigFragment, JavaCommonConfig>, Error> {
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
    ) -> RoleGroupRef<v1alpha1::ZookeeperCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: ZookeeperRole::Server.to_string(),
            role_group: group_name.into(),
        }
    }

    pub fn role_config(&self, role: &ZookeeperRole) -> Option<&ZookeeperServerRoleConfig> {
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
                    role_group_headless_service_name: build_role_group_headless_service_name(
                        rolegroup_ref.object_name(),
                    ),
                    pod_name: format!("{role_group}-{i}", role_group = rolegroup_ref.object_name()),
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
    ) -> Result<v1alpha1::ZookeeperConfig, Error> {
        // Initialize the result with all default values as baseline
        let conf_defaults =
            v1alpha1::ZookeeperConfig::default_server_config(&self.name_any(), role);

        // Retrieve role resource config
        let role = self.role(role)?;
        let mut conf_role = role.config.config.to_owned();

        // Retrieve rolegroup specific resource config
        let role_group = self.rolegroup(rolegroup_ref)?;
        let mut conf_role_group = role_group.config.config;

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
    ) -> Result<Logging<v1alpha1::Container>, Error> {
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
        rolegroup_ref: &RoleGroupRef<v1alpha1::ZookeeperCluster>,
    ) -> Result<(Vec<PersistentVolumeClaim>, ResourceRequirements), Error> {
        let config = self.merged_config(role, rolegroup_ref)?;
        let resources: Resources<v1alpha1::ZookeeperStorageConfig, NoRuntimeLimits> =
            config.resources;

        let data_pvc = resources
            .storage
            .data
            .build_pvc("data", Some(vec!["ReadWriteOnce"]));

        Ok((vec![data_pvc], resources.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_server_secret_class(zk: &v1alpha1::ZookeeperCluster) -> Option<&str> {
        zk.spec
            .cluster_config
            .tls
            .as_ref()
            .and_then(|tls| tls.server_secret_class.as_deref())
    }

    fn get_quorum_secret_class(zk: &v1alpha1::ZookeeperCluster) -> &str {
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
            productVersion: "3.9.3"
        "#;
        let zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(input).expect("illegal test input");
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
            productVersion: "3.9.3"
          clusterConfig:
            tls:
              serverSecretClass: simple-zookeeper-client-tls
        "#;
        let zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(input).expect("illegal test input");

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
            productVersion: "3.9.3"
          clusterConfig:
            tls:
              serverSecretClass: null
        "#;
        let zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(input).expect("illegal test input");
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
            productVersion: "3.9.3"
          clusterConfig:
            tls:
              quorumSecretClass: simple-zookeeper-quorum-tls
        "#;
        let zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(input).expect("illegal test input");
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
            productVersion: "3.9.3"
        "#;
        let zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(input).expect("illegal test input");

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
            productVersion: "3.9.3"
          clusterConfig:
            tls:
              quorumSecretClass: simple-zookeeper-quorum-tls
        "#;
        let zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(input).expect("illegal test input");
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
            productVersion: "3.9.3"
          clusterConfig:
            tls:
              serverSecretClass: simple-zookeeper-server-tls
        "#;
        let zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(input).expect("illegal test input");
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
