use std::str::FromStr;

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, Snafu};
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
    config::{fragment::Fragment, merge::Merge},
    crd::ClusterRef,
    deep_merger::ObjectOverrides,
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    kube::{CustomResource, ResourceExt},
    product_logging::{self, spec::Logging},
    role_utils::{GenericRoleConfig, Role},
    schemars::{self, JsonSchema},
    shared::time::Duration,
    status::condition::{ClusterCondition, HasStatusCondition},
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        config_overrides::KeyValueConfigOverrides,
        role_utils::JavaCommonConfig,
        types::kubernetes::{
            ConfigMapName, ListenerClassName, ListenerName, NamespaceName, ServiceName,
        },
    },
    versioned::versioned,
};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

use crate::crd::{affinity::get_affinity, v1alpha1::ZookeeperServerRoleConfig};

pub mod affinity;
pub mod authentication;
pub mod security;
pub mod tls;

/// The name of the role-level [`Listener`](stackable_operator::crd::listener::v1alpha1::Listener)
/// exposing the given `zk_role`, `<cluster>-<role>`.
///
/// Lives in the `crd` module (rather than the controller build tree) because it is shared by both
/// controllers and by [`ZookeeperCluster::server_role_listener_fqdn`].
pub fn role_listener_name(cluster_name: &str, zk_role: &ZookeeperRole) -> ListenerName {
    ListenerName::from_str(&format!("{cluster_name}-{zk_role}"))
        .expect("the role listener name should be a valid Listener name")
}

pub const APP_NAME: &str = "zookeeper";
pub const OPERATOR_NAME: &str = "zookeeper.stackable.tech";
pub const FIELD_MANAGER: &str = "zookeeper-operator";

pub const ZOOKEEPER_SERVER_PORT_NAME: &str = "zk";
pub const ZOOKEEPER_LEADER_PORT_NAME: &str = "zk-leader";
pub const ZOOKEEPER_LEADER_PORT: u16 = 2888;
pub const ZOOKEEPER_ELECTION_PORT_NAME: &str = "zk-election";
pub const ZOOKEEPER_ELECTION_PORT: u16 = 3888;

pub const JMX_METRICS_PORT_NAME: &str = "jmx-metrics";
pub const JMX_METRICS_PORT: u16 = 9505;
pub const METRICS_PROVIDER_HTTP_PORT_KEY: &str = "metricsProvider.httpPort";
pub const METRICS_PROVIDER_HTTP_PORT_NAME: &str = "metrics";
pub const METRICS_PROVIDER_HTTP_PORT: u16 = 7000;

pub const STACKABLE_DATA_DIR: &str = "/stackable/data";
pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";
pub const STACKABLE_LOG_CONFIG_DIR: &str = "/stackable/log_config";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
pub const STACKABLE_RW_CONFIG_DIR: &str = "/stackable/rwconfig";

pub const CONTAINER_IMAGE_BASE_NAME: &str = "zookeeper";

const DEFAULT_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_minutes_unchecked(2);
pub const DEFAULT_LISTENER_CLASS: &str = "cluster-internal";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("the role {role} is not defined"))]
    CannotRetrieveZookeeperRole { role: String },
}

pub type ZookeeperServerRoleType = Role<
    v1alpha1::ZookeeperConfigFragment,
    v1alpha1::ZookeeperConfigOverrides,
    ZookeeperServerRoleConfig,
    JavaCommonConfig,
>;

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
        #[serde(default)]
        pub object_overrides: ObjectOverrides,

        // no doc - it's in the struct.
        pub image: ProductImage,

        // no doc - it's in the struct.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub servers: Option<ZookeeperServerRoleType>,
    }

    #[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ZookeeperServerRoleConfig {
        #[serde(flatten)]
        pub common: GenericRoleConfig,

        /// This field controls which [ListenerClass](DOCS_BASE_URL_PLACEHOLDER/listener-operator/listenerclass.html) is used to expose the ZooKeeper servers.
        #[serde(default = "default_listener_class")]
        pub listener_class: ListenerClassName,
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
        pub vector_aggregator_config_map_name: Option<ConfigMapName>,

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

    #[derive(Clone, Debug, Default, Deserialize, Merge, JsonSchema, PartialEq, Serialize)]
    pub struct ZookeeperConfigOverrides {
        #[serde(default, rename = "zoo.cfg")]
        pub zoo_cfg: KeyValueConfigOverrides,

        #[serde(default, rename = "security.properties")]
        pub security_properties: KeyValueConfigOverrides,
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

        // no doc - it's in the struct.
        #[serde(default)]
        pub object_overrides: ObjectOverrides,
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
    Ord,
    PartialEq,
    PartialOrd,
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
    pub namespace: NamespaceName,
    pub role_group_headless_service_name: ServiceName,
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

pub(crate) fn default_listener_class() -> ListenerClassName {
    ListenerClassName::from_str(DEFAULT_LISTENER_CLASS)
        .expect("the default listener class should be a valid ListenerClass name")
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

    pub(crate) fn default_server_config(
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
    /// The fully-qualified domain name of the role-level [Listener]
    ///
    /// [Listener]: stackable_operator::crd::listener::v1alpha1::Listener
    pub fn server_role_listener_fqdn(
        &self,
        cluster_info: &KubernetesClusterInfo,
    ) -> Option<String> {
        Some(format!(
            "{role_listener_name}.{namespace}.svc.{cluster_domain}",
            role_listener_name = role_listener_name(&self.name_any(), &ZookeeperRole::Server),
            namespace = self.metadata.namespace.as_ref()?,
            cluster_domain = cluster_info.cluster_domain
        ))
    }

    /// Returns a reference to the role. Raises an error if the role is not defined.
    pub fn role(&self, role_variant: &ZookeeperRole) -> Result<&ZookeeperServerRoleType, Error> {
        match role_variant {
            ZookeeperRole::Server => self.spec.servers.as_ref(),
        }
        .with_context(|| CannotRetrieveZookeeperRoleSnafu {
            role: role_variant.to_string(),
        })
    }

    pub fn role_config(&self, role: &ZookeeperRole) -> Option<&ZookeeperServerRoleConfig> {
        match role {
            ZookeeperRole::Server => self.spec.servers.as_ref().map(|s| &s.role_config),
        }
    }
}

#[cfg(test)]
mod tests {
    use stackable_operator::versioned::test_utils::RoundtripTestData;

    use super::*;

    fn get_server_secret_class(zk: &v1alpha1::ZookeeperCluster) -> Option<&str> {
        zk.spec
            .cluster_config
            .tls
            .as_ref()
            .and_then(|tls| tls.server_secret_class.as_ref())
            .map(AsRef::as_ref)
    }

    fn get_quorum_secret_class(zk: &v1alpha1::ZookeeperCluster) -> &str {
        zk.spec
            .cluster_config
            .tls
            .as_ref()
            .unwrap()
            .quorum_secret_class
            .as_ref()
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
            productVersion: "3.9.5"
        "#;
        let zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            get_server_secret_class(&zookeeper),
            tls::server_tls_default().as_ref().map(AsRef::as_ref)
        );
        assert_eq!(
            get_quorum_secret_class(&zookeeper),
            tls::quorum_tls_default().as_ref()
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.5"
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
            tls::quorum_tls_default().as_ref()
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.5"
          clusterConfig:
            tls:
              serverSecretClass: null
        "#;
        let zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(get_server_secret_class(&zookeeper), None);
        assert_eq!(
            get_quorum_secret_class(&zookeeper),
            tls::quorum_tls_default().as_ref()
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.5"
          clusterConfig:
            tls:
              quorumSecretClass: simple-zookeeper-quorum-tls
        "#;
        let zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            get_server_secret_class(&zookeeper),
            tls::server_tls_default().as_ref().map(AsRef::as_ref)
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
            productVersion: "3.9.5"
        "#;
        let zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(input).expect("illegal test input");

        assert_eq!(
            get_server_secret_class(&zookeeper),
            tls::server_tls_default().as_ref().map(AsRef::as_ref)
        );
        assert_eq!(
            get_quorum_secret_class(&zookeeper),
            tls::quorum_tls_default().as_ref()
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.5"
          clusterConfig:
            tls:
              quorumSecretClass: simple-zookeeper-quorum-tls
        "#;
        let zookeeper: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            get_server_secret_class(&zookeeper),
            tls::server_tls_default().as_ref().map(AsRef::as_ref)
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
            productVersion: "3.9.5"
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
            tls::quorum_tls_default().as_ref()
        );
    }

    impl RoundtripTestData for v1alpha1::ZookeeperClusterSpec {
        fn roundtrip_test_data() -> Vec<Self> {
            stackable_operator::utils::yaml_from_str_singleton_map(indoc::indoc! {r#"
              - image:
                  productVersion: 1.2.3
                  pullPolicy: IfNotPresent
                clusterOperation:
                  reconciliationPaused: false
                  stopped: true
                clusterConfig:
                  authentication:
                    - authenticationClass: my-auth-class
                  tls:
                    quorumSecretClass: null
                    serverSecretClass: tls
                  vectorAggregatorConfigMapName: vector-aggregator-discovery
                servers:
                  envOverrides:
                    COMMON_VAR: role-value
                    ROLE_VAR: role-value
                  config:
                    logging:
                      enableVectorAgent: true
                    requestedSecretLifetime: 7d
                    gracefulShutdownTimeout: 30s
                    initLimit: 5
                    syncLimit: 2
                    tickTime: 2000
                    myidOffset: 1
                  configOverrides:
                    zoo.cfg:
                      maxClientCnxns: "60"
                  roleConfig:
                    listenerClass: cluster-internal
                  roleGroups:
                    default:
                      replicas: 1
                      configOverrides:
                        zoo.cfg:
                          maxClientCnxns: "120"
                      envOverrides:
                        COMMON_VAR: group-value
                        GROUP_VAR: group-value
        "#})
            .expect("Failed to parse ZookeeperClusterSpec YAML")
        }
    }

    impl RoundtripTestData for v1alpha1::ZookeeperZnodeSpec {
        fn roundtrip_test_data() -> Vec<Self> {
            stackable_operator::utils::yaml_from_str_singleton_map(indoc::indoc! {"
              - clusterRef:
                  name: test-zk
                  namespace: default
        "})
            .expect("Failed to parse ZookeeperZnodeSpec YAML")
        }
    }
}
