use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::resources::{
        CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
        PvcConfig, PvcConfigFragment, Resources, ResourcesFragment,
    },
    config::{fragment, fragment::Fragment, fragment::ValidationError, merge::Merge},
    crd::ClusterRef,
    error::OperatorResult,
    k8s_openapi::{
        api::core::v1::{PersistentVolumeClaim, ResourceRequirements},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::{runtime::reflector::ObjectRef, CustomResource},
    memory::to_java_heap_value,
    memory::BinaryMultiple,
    product_config_utils::{ConfigError, Configuration},
    role_utils::{Role, RoleGroupRef},
    schemars::{self, JsonSchema},
};
use std::collections::BTreeMap;
use strum::{Display, EnumString};

pub const ZOOKEEPER_PROPERTIES_FILE: &str = "zoo.cfg";

pub const CLIENT_PORT: u16 = 2181;
pub const SECURE_CLIENT_PORT: u16 = 2282;
pub const METRICS_PORT: u16 = 9505;

pub const STACKABLE_DATA_DIR: &str = "/stackable/data";
pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";
pub const STACKABLE_RW_CONFIG_DIR: &str = "/stackable/rwconfig";

pub const QUORUM_TLS_DIR: &str = "/stackable/quorum_tls";
pub const QUORUM_TLS_MOUNT_DIR: &str = "/stackable/quorum_tls_mount";
pub const CLIENT_TLS_DIR: &str = "/stackable/client_tls";
pub const CLIENT_TLS_MOUNT_DIR: &str = "/stackable/client_tls_mount";
pub const SYSTEM_TRUST_STORE_DIR: &str = "/etc/pki/java/cacerts";

const JVM_HEAP_FACTOR: f32 = 0.8;
const TLS_DEFAULT_SECRET_CLASS: &str = "tls";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,
    #[snafu(display("unknown ZooKeeper role found {role}. Should be one of {roles:?}"))]
    UnknownZookeeperRole { role: String, roles: Vec<String> },
    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },
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
#[serde(default, rename_all = "camelCase")]
pub struct ZookeeperClusterSpec {
    /// Emergency stop button, if `true` then all pods are stopped without affecting configuration (as setting `replicas` to `0` would)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stopped: Option<bool>,
    /// Desired ZooKeeper version
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<GlobalZookeeperConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub servers: Option<Role<ZookeeperConfig>>,
}

impl Default for ZookeeperClusterSpec {
    fn default() -> Self {
        Self {
            config: Some(GlobalZookeeperConfig::default()),
            stopped: Default::default(),
            version: Default::default(),
            servers: Default::default(),
        }
    }
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct GlobalZookeeperConfig {
    /// Only affects client connections. This setting controls:
    /// - If TLS encryption is used at all
    /// - Which cert the servers should use to authenticate themselves against the client
    /// Defaults to `TlsSecretClass` { secret_class: "tls".to_string() }.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsSecretClass>,
    /// Only affects client connections. This setting controls:
    /// - If clients need to authenticate themselves against the server via TLS
    /// - Which ca.crt to use when validating the provided client certs
    /// Defaults to `None`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_authentication: Option<ClientAuthenticationClass>,
    /// Only affects quorum communication. Use mutual verification between Zookeeper Nodes
    /// (mandatory). This setting controls:
    /// - Which cert the servers should use to authenticate themselves against other servers
    /// - Which ca.crt to use when validating the other server
    pub quorum_tls_secret_class: String,
}

impl Default for GlobalZookeeperConfig {
    fn default() -> Self {
        Self {
            tls: Some(TlsSecretClass::default()),
            client_authentication: None,
            quorum_tls_secret_class: TLS_DEFAULT_SECRET_CLASS.to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsSecretClass {
    pub secret_class: String,
}

impl Default for TlsSecretClass {
    fn default() -> Self {
        Self {
            secret_class: TLS_DEFAULT_SECRET_CLASS.into(),
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientAuthenticationClass {
    pub authentication_class: String,
}

#[derive(Clone, Default, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperConfig {
    pub init_limit: Option<u32>,
    pub sync_limit: Option<u32>,
    pub tick_time: Option<u32>,
    pub myid_offset: Option<u16>,
    pub resources: Option<ResourcesFragment<ZookeeperStorageConfig, NoRuntimeLimits>>,
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

impl ZookeeperConfig {
    pub const INIT_LIMIT: &'static str = "initLimit";
    pub const SYNC_LIMIT: &'static str = "syncLimit";
    pub const TICK_TIME: &'static str = "tickTime";
    pub const DATA_DIR: &'static str = "dataDir";

    pub const MYID_OFFSET: &'static str = "MYID_OFFSET";
    pub const SERVER_JVMFLAGS: &'static str = "SERVER_JVMFLAGS";
    pub const ZK_SERVER_HEAP: &'static str = "ZK_SERVER_HEAP";
    pub const CLIENT_PORT: &'static str = "clientPort";

    // Quorum TLS
    pub const SSL_QUORUM: &'static str = "sslQuorum";
    pub const SSL_QUORUM_CLIENT_AUTH: &'static str = "ssl.quorum.clientAuth";
    pub const SSL_QUORUM_HOST_NAME_VERIFICATION: &'static str = "ssl.quorum.hostnameVerification";
    pub const SSL_QUORUM_KEY_STORE_LOCATION: &'static str = "ssl.quorum.keyStore.location";
    pub const SSL_QUORUM_KEY_STORE_PASSWORD: &'static str = "ssl.quorum.keyStore.password";
    pub const SSL_QUORUM_TRUST_STORE_LOCATION: &'static str = "ssl.quorum.trustStore.location";
    pub const SSL_QUORUM_TRUST_STORE_PASSWORD: &'static str = "ssl.quorum.trustStore.password";
    // Client TLS
    pub const SECURE_CLIENT_PORT: &'static str = "secureClientPort";
    pub const SSL_CLIENT_AUTH: &'static str = "ssl.clientAuth";
    pub const SSL_HOST_NAME_VERIFICATION: &'static str = "ssl.hostnameVerification";
    pub const SSL_KEY_STORE_LOCATION: &'static str = "ssl.keyStore.location";
    pub const SSL_KEY_STORE_PASSWORD: &'static str = "ssl.keyStore.password";
    pub const SSL_TRUST_STORE_LOCATION: &'static str = "ssl.trustStore.location";
    pub const SSL_TRUST_STORE_PASSWORD: &'static str = "ssl.trustStore.password";
    // Common TLS
    pub const SSL_AUTH_PROVIDER_X509: &'static str = "authProvider.x509";
    pub const SERVER_CNXN_FACTORY: &'static str = "serverCnxnFactory";

    fn myid_offset(&self) -> u16 {
        self.myid_offset.unwrap_or(1)
    }

    fn default_resources() -> ResourcesFragment<ZookeeperStorageConfig, NoRuntimeLimits> {
        ResourcesFragment {
            cpu: CpuLimitsFragment {
                min: Some(Quantity("500m".to_owned())),
                max: Some(Quantity("4".to_owned())),
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
        }
    }
}

impl Configuration for ZookeeperConfig {
    type Configurable = ZookeeperCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let jvm_flags = format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={METRICS_PORT}:/stackable/jmx/server.yaml");
        Ok([
            (
                Self::MYID_OFFSET.to_string(),
                Some(self.myid_offset().to_string()),
            ),
            (Self::SERVER_JVMFLAGS.to_string(), Some(jvm_flags)),
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
        resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();
        if let Some(init_limit) = self.init_limit {
            result.insert(Self::INIT_LIMIT.to_string(), Some(init_limit.to_string()));
        }
        if let Some(sync_limit) = self.sync_limit {
            result.insert(Self::SYNC_LIMIT.to_string(), Some(sync_limit.to_string()));
        }
        if let Some(tick_time) = self.tick_time {
            result.insert(Self::TICK_TIME.to_string(), Some(tick_time.to_string()));
        }
        result.insert(
            Self::DATA_DIR.to_string(),
            Some(STACKABLE_DATA_DIR.to_string()),
        );

        // Quorum TLS
        result.insert(Self::SSL_QUORUM.to_string(), Some("true".to_string()));
        result.insert(
            Self::SSL_QUORUM_HOST_NAME_VERIFICATION.to_string(),
            Some("true".to_string()),
        );
        result.insert(
            Self::SSL_QUORUM_CLIENT_AUTH.to_string(),
            Some("need".to_string()),
        );
        result.insert(
            Self::SERVER_CNXN_FACTORY.to_string(),
            Some("org.apache.zookeeper.server.NettyServerCnxnFactory".to_string()),
        );
        result.insert(
            Self::SSL_AUTH_PROVIDER_X509.to_string(),
            Some("org.apache.zookeeper.server.auth.X509AuthenticationProvider".to_string()),
        );
        // The keystore and truststore passwords should not be in the configmap and are generated
        // and written later via script in the init container
        result.insert(
            Self::SSL_QUORUM_KEY_STORE_LOCATION.to_string(),
            Some(format!("{dir}/keystore.p12", dir = QUORUM_TLS_DIR)),
        );
        result.insert(
            Self::SSL_QUORUM_TRUST_STORE_LOCATION.to_string(),
            Some(format!("{dir}/truststore.p12", dir = QUORUM_TLS_DIR)),
        );

        // Client TLS
        if resource.client_tls_enabled() {
            // We set only the clientPort and portUnification here because otherwise there is a port bind exception
            // See: https://issues.apache.org/jira/browse/ZOOKEEPER-4276
            // --> Normally we would like to only set the secureClientPort (check out commented code below)
            // What we tried:
            // 1) Set clientPort and secureClientPort will fail with
            // "static.config different from dynamic config .. "
            // result.insert(
            //     Self::CLIENT_PORT.to_string(),
            //     Some(CLIENT_PORT.to_string()),
            // );
            // result.insert(
            //     Self::SECURE_CLIENT_PORT.to_string(),
            //     Some(SECURE_CLIENT_PORT.to_string()),
            // );

            // 2) Setting only secureClientPort will result in the above mentioned bind exception.
            // The NettyFactory tries to bind multiple times on the secureClientPort.
            // result.insert(
            //     Self::SECURE_CLIENT_PORT.to_string(),
            //     Some(resource.client_port().to_string()),
            // );

            // 3) Using the clientPort and portUnification still allows plaintext connection without
            // authentication, but at least TLS and authentication works when connecting securely.
            result.insert(
                Self::CLIENT_PORT.to_string(),
                Some(resource.client_port().to_string()),
            );
            result.insert(
                "client.portUnification".to_string(),
                Some("true".to_string()),
            );
            // TODO: Remove clientPort and portUnification (above) in favor of secureClientPort once the bug is fixed
            // result.insert(
            //     Self::SECURE_CLIENT_PORT.to_string(),
            //     Some(resource.client_port().to_string()),
            // );
            // END TICKET

            result.insert(
                Self::SSL_HOST_NAME_VERIFICATION.to_string(),
                Some("true".to_string()),
            );
            // The keystore and truststore passwords should not be in the configmap and are generated
            // and written later via script in the init container
            result.insert(
                Self::SSL_KEY_STORE_LOCATION.to_string(),
                Some(format!("{dir}/keystore.p12", dir = CLIENT_TLS_DIR)),
            );
            result.insert(
                Self::SSL_TRUST_STORE_LOCATION.to_string(),
                Some(format!("{dir}/truststore.p12", dir = CLIENT_TLS_DIR)),
            );
            // Check if we need to enable authentication
            if resource.client_tls_authentication_class().is_some() {
                result.insert(Self::SSL_CLIENT_AUTH.to_string(), Some("need".to_string()));
            }
        } else {
            result.insert(
                Self::CLIENT_PORT.to_string(),
                Some(resource.client_port().to_string()),
            );
        }

        Ok(result)
    }
}

#[derive(
    Clone, Debug, Deserialize, Display, EnumString, Eq, Hash, JsonSchema, PartialEq, Serialize,
)]
#[strum(serialize_all = "camelCase")]
pub enum ZookeeperRole {
    #[strum(serialize = "server")]
    Server,
}

#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperClusterStatus {
    /// An opaque value that changes every time a discovery detail does
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_hash: Option<String>,
}

impl ZookeeperCluster {
    /// The image version provided in the `spec.version` field
    pub fn image_version(&self) -> Result<&str, Error> {
        self.spec
            .version
            .as_deref()
            .context(ObjectHasNoVersionSnafu)
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

    /// List all pods expected to form the cluster
    ///
    /// We try to predict the pods here rather than looking at the current cluster state in order to
    /// avoid instance churn. For example, regenerating zoo.cfg based on the cluster state would lead to
    /// a lot of spurious restarts, as well as opening us up to dangerous split-brain conditions because
    /// the pods have inconsistent snapshots of which servers they should expect to be in quorum.
    pub fn pods(&self) -> Result<impl Iterator<Item = ZookeeperPodRef> + '_, Error> {
        let ns = self.metadata.namespace.clone().context(NoNamespaceSnafu)?;
        Ok(self
            .spec
            .servers
            .iter()
            .flat_map(|role| &role.role_groups)
            // Order rolegroups consistently, to avoid spurious downstream rewrites
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .flat_map(move |(rolegroup_name, rolegroup)| {
                let rolegroup_ref = self.server_rolegroup_ref(rolegroup_name);
                let ns = ns.clone();
                (0..rolegroup.replicas.unwrap_or(0)).map(move |i| ZookeeperPodRef {
                    namespace: ns.clone(),
                    role_group_service_name: rolegroup_ref.object_name(),
                    pod_name: format!("{}-{}", rolegroup_ref.object_name(), i),
                    zookeeper_myid: i + rolegroup.config.config.myid_offset(),
                })
            }))
    }

    pub fn client_port(&self) -> u16 {
        if self.client_tls_enabled() {
            SECURE_CLIENT_PORT
        } else {
            CLIENT_PORT
        }
    }

    /// Returns the secret class for client connection encryption. Defaults to `tls`.
    pub fn client_tls_secret_class(&self) -> Option<&TlsSecretClass> {
        let spec: &ZookeeperClusterSpec = &self.spec;
        spec.config.as_ref().and_then(|c| c.tls.as_ref())
    }

    /// Checks if we should use TLS to encrypt client connections.
    pub fn client_tls_enabled(&self) -> bool {
        self.client_tls_secret_class().is_some() || self.client_tls_authentication_class().is_some()
    }

    /// Returns the authentication class used for client authentication
    pub fn client_tls_authentication_class(&self) -> Option<&str> {
        let spec: &ZookeeperClusterSpec = &self.spec;
        spec.config
            .as_ref()
            .and_then(|c| c.client_authentication.as_ref())
            .map(|tls| tls.authentication_class.as_ref())
    }

    /// Returns the secret class for internal server encryption
    pub fn quorum_tls_secret_class(&self) -> &str {
        let spec: &ZookeeperClusterSpec = &self.spec;
        spec.config
            .as_ref()
            .map(|c| c.quorum_tls_secret_class.as_ref())
            .unwrap_or_default()
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
        // Initialize the result with all default values as baseline
        let conf_defaults = ZookeeperConfig::default_resources();

        let role = match role {
            ZookeeperRole::Server => {
                self.spec
                    .servers
                    .as_ref()
                    .context(UnknownZookeeperRoleSnafu {
                        role: role.to_string(),
                        roles: vec![role.to_string()],
                    })?
            }
        };

        // Retrieve role resource config
        let mut conf_role: ResourcesFragment<ZookeeperStorageConfig, NoRuntimeLimits> =
            role.config.config.resources.clone().unwrap_or_default();

        // Retrieve rolegroup specific resource config
        let mut conf_rolegroup: ResourcesFragment<ZookeeperStorageConfig, NoRuntimeLimits> = role
            .role_groups
            .get(&rolegroup_ref.role_group)
            .and_then(|rg| rg.config.config.resources.clone())
            .unwrap_or_default();

        // Merge more specific configs into default config
        // Hierarchy is:
        // 1. RoleGroup
        // 2. Role
        // 3. Default
        conf_role.merge(&conf_defaults);
        conf_rolegroup.merge(&conf_role);

        let resources: Resources<ZookeeperStorageConfig, NoRuntimeLimits> =
            fragment::validate(conf_rolegroup).context(FragmentValidationFailureSnafu)?;

        let data_pvc = resources
            .storage
            .data
            .build_pvc("data", Some(vec!["ReadWriteOnce"]));

        Ok((vec![data_pvc], resources.into()))
    }

    pub fn heap_limits(&self, resources: &ResourceRequirements) -> OperatorResult<Option<u32>> {
        resources
            .limits
            .as_ref()
            .and_then(|limits| limits.get("memory"))
            .map(|memory_limit| {
                to_java_heap_value(memory_limit, JVM_HEAP_FACTOR, BinaryMultiple::Mebi)
            })
            .transpose()
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

    #[test]
    fn test_client_tls() {
        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          version: abc
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            zookeeper.client_tls_secret_class().unwrap().secret_class,
            TLS_DEFAULT_SECRET_CLASS.to_string()
        );
        assert_eq!(
            zookeeper.quorum_tls_secret_class(),
            TLS_DEFAULT_SECRET_CLASS.to_string()
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          version: abc
          config:
            tls:
              secretClass: simple-zookeeper-client-tls
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");

        assert_eq!(
            zookeeper.client_tls_secret_class().unwrap().secret_class,
            "simple-zookeeper-client-tls".to_string()
        );
        assert_eq!(
            zookeeper.quorum_tls_secret_class(),
            TLS_DEFAULT_SECRET_CLASS
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          version: abc
          config:
            tls: null
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(zookeeper.client_tls_secret_class(), None);
        assert_eq!(
            zookeeper.quorum_tls_secret_class(),
            TLS_DEFAULT_SECRET_CLASS.to_string()
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          version: abc
          config:
            quorumTlsSecretClass: simple-zookeeper-quorum-tls
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            zookeeper.client_tls_secret_class().unwrap().secret_class,
            TLS_DEFAULT_SECRET_CLASS.to_string()
        );
        assert_eq!(
            zookeeper.quorum_tls_secret_class(),
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
          version: abc
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            zookeeper.quorum_tls_secret_class(),
            TLS_DEFAULT_SECRET_CLASS.to_string()
        );
        assert_eq!(
            zookeeper.client_tls_secret_class().unwrap().secret_class,
            TLS_DEFAULT_SECRET_CLASS
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          version: abc
          config:
            quorumTlsSecretClass: simple-zookeeper-quorum-tls
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            zookeeper.quorum_tls_secret_class(),
            "simple-zookeeper-quorum-tls".to_string()
        );
        assert_eq!(
            zookeeper.client_tls_secret_class().unwrap().secret_class,
            TLS_DEFAULT_SECRET_CLASS
        );

        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          version: abc
          config:
            tls:
              secretClass: simple-zookeeper-client-tls
        "#;
        let zookeeper: ZookeeperCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            zookeeper.quorum_tls_secret_class(),
            TLS_DEFAULT_SECRET_CLASS.to_string()
        );
        assert_eq!(
            zookeeper.client_tls_secret_class().unwrap().secret_class,
            "simple-zookeeper-client-tls"
        );
    }
}
