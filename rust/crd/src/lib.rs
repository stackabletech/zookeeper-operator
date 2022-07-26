use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::commons::resources::{
    CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimitsFragment, PvcConfigFragment,
    ResourcesFragment,
};
use stackable_operator::config::fragment::{Fragment, ValidationError};
use stackable_operator::k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use stackable_operator::memory::BinaryMultiple;
use stackable_operator::role_utils::RoleGroup;
use stackable_operator::{
    commons::resources::{NoRuntimeLimits, PvcConfig, Resources},
    config::merge::Merge,
    crd::ClusterRef,
    error::OperatorResult,
    k8s_openapi::api::core::v1::{PersistentVolumeClaim, ResourceRequirements},
    kube::{runtime::reflector::ObjectRef, CustomResource},
    memory::to_java_heap_value,
    product_config_utils::{ConfigError, Configuration},
    role_utils::{Role, RoleGroupRef},
    schemars::{self, JsonSchema},
};
use std::collections::{BTreeMap, BTreeSet};

pub const CLIENT_PORT: u16 = 2181;
pub const SECURE_CLIENT_PORT: u16 = 2282;
pub const METRICS_PORT: u16 = 9505;

pub const STACKABLE_DATA_DIR: &str = "/stackable/data";
pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";
pub const STACKABLE_RW_CONFIG_DIR: &str = "/stackable/rwconfig";
pub const QUORUM_TLS_DIR: &str = "/stackable/tls/quorum";
pub const CLIENT_TLS_DIR: &str = "/stackable/tls/client";

const JVM_HEAP_FACTOR: f32 = 0.8;
const DEFAULT_SECRET_CLASS: &str = "tls";

/// A cluster of ZooKeeper nodes
#[derive(Clone, CustomResource, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
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
    /// Emergency stop button, if `true` then all pods are stopped without affecting configuration (as setting `replicas` to `0` would)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<bool>,
    /// Desired ZooKeeper version
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub servers: Option<Role<ZookeeperConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<GlobalZookeeperConfig>,
}

#[derive(Clone, Default, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GlobalZookeeperConfig {
    #[serde(flatten)]
    pub tls_config: Option<TlsConfig>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    /// Only affects client connections. This setting controls:
    /// - If TLS encryption is used at all
    /// - Which cert the servers should use to authenticate themselves against the client
    /// Defaults to `TlsSecretClass` { secret_class: "tls".to_string() }.
    pub tls: Option<TlsSecretClass>,
    /// Only affects client connections. This setting controls:
    /// - If clients need to authenticate themselves against the server via TLS
    /// - Which ca.crt to use when validating the provided client certs
    /// Defaults to `None`
    pub client_authentication: Option<ClientAuthenticationClass>,
    /// Only affects quorum communication. Use mutual verification between Zookeeper Nodes
    /// (mandatory). This setting controls:
    /// - Which cert the servers should use to authenticate themselves against other servers
    /// - Which ca.crt to use when validating the other server
    pub quorum_tls_secret_class: String,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            tls: Some(TlsSecretClass {
                secret_class: DEFAULT_SECRET_CLASS.to_string(),
            }),
            client_authentication: None,
            quorum_tls_secret_class: DEFAULT_SECRET_CLASS.to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsSecretClass {
    pub secret_class: String,
}

impl Default for TlsSecretClass {
    fn default() -> Self {
        Self {
            secret_class: DEFAULT_SECRET_CLASS.to_string(),
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientAuthenticationClass {
    pub authentication_class: String,
}

#[derive(Clone, Default, Debug, JsonSchema, PartialEq, Fragment)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        JsonSchema,
        Serialize,
        PartialEq,
        Merge
    ),
    serde(rename_all = "camelCase")
)]
pub struct ZookeeperConfig {
    pub init_limit: Option<u32>,
    pub sync_limit: Option<u32>,
    pub tick_time: Option<u32>,
    pub myid_offset: u16,
    #[fragment_attrs(serde(default))]
    pub resources: Resources<Storage, NoRuntimeLimits>,
}

#[derive(Default, Clone, Debug, JsonSchema, PartialEq, Fragment)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Merge,
        Serialize,
        Deserialize,
        JsonSchema,
        PartialEq,
    ),
    serde(rename_all = "camelCase")
)]
pub struct Storage {
    #[fragment_attrs(serde(default))]
    pub data: PvcConfig,
}

impl Storage {
    pub fn build_pvcs(&self) -> Vec<PersistentVolumeClaim> {
        let data_pvc = self.data.build_pvc("data", Some(vec!["ReadWriteOnce"]));
        vec![data_pvc]
    }
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
}

impl Configuration for ZookeeperConfigFragment {
    type Configurable = ZookeeperCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let jvm_flags = format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={METRICS_PORT}:/stackable/jmx/server.yaml");
        Ok([
            (
                ZookeeperConfig::MYID_OFFSET.to_string(),
                Some(self.myid_offset.unwrap_or(0).to_string()),
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
        resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();
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

        // Quorum TLS
        result.insert(
            ZookeeperConfig::SSL_QUORUM.to_string(),
            Some("true".to_string()),
        );
        result.insert(
            ZookeeperConfig::SSL_QUORUM_HOST_NAME_VERIFICATION.to_string(),
            Some("true".to_string()),
        );
        result.insert(
            ZookeeperConfig::SSL_QUORUM_CLIENT_AUTH.to_string(),
            Some("need".to_string()),
        );
        result.insert(
            ZookeeperConfig::SERVER_CNXN_FACTORY.to_string(),
            Some("org.apache.zookeeper.server.NettyServerCnxnFactory".to_string()),
        );
        result.insert(
            ZookeeperConfig::SSL_AUTH_PROVIDER_X509.to_string(),
            Some("org.apache.zookeeper.server.auth.X509AuthenticationProvider".to_string()),
        );
        // The keystore and truststore passwords should not be in the configmap and are generated
        // and written later via script in the init container
        result.insert(
            ZookeeperConfig::SSL_QUORUM_KEY_STORE_LOCATION.to_string(),
            Some(format!("{dir}/keystore.p12", dir = QUORUM_TLS_DIR)),
        );
        result.insert(
            ZookeeperConfig::SSL_QUORUM_TRUST_STORE_LOCATION.to_string(),
            Some(format!("{dir}/truststore.p12", dir = QUORUM_TLS_DIR)),
        );

        // Client TLS
        if resource.is_client_secure() {
            // We set only the clientPort and portUnification here because otherwise there is a port bind exception
            // See: https://issues.apache.org/jira/browse/ZOOKEEPER-4276
            // --> Normally we would like to only set the secureClientPort (check out commented code below)
            // What we tried:
            // 1) Set clientPort and secureClientPort will fail with
            // "static.config different from dynamic config .. "
            // result.insert(
            //     ZookeeperConfig::CLIENT_PORT.to_string(),
            //     Some(CLIENT_PORT.to_string()),
            // );
            // result.insert(
            //     ZookeeperConfig::SECURE_CLIENT_PORT.to_string(),
            //     Some(SECURE_CLIENT_PORT.to_string()),
            // );

            // 2) Setting only secureClientPort will result in the above mentioned bind exception.
            // The NettyFactory tries to bind multiple times on the secureClientPort.
            // result.insert(
            //     ZookeeperConfig::SECURE_CLIENT_PORT.to_string(),
            //     Some(resource.client_port().to_string()),
            // );

            // 3) Using the clientPort and portUnification still allows plaintext connection without
            // authentication, but at least TLS and authentication works when connecting securely.
            result.insert(
                ZookeeperConfig::CLIENT_PORT.to_string(),
                Some(resource.client_port().to_string()),
            );
            result.insert(
                "client.portUnification".to_string(),
                Some("true".to_string()),
            );
            // TODO: Remove clientPort and portUnification in favor of secureClientPort once the bug is fixed
            // result.insert(
            //     ZookeeperConfig::SECURE_CLIENT_PORT.to_string(),
            //     Some(resource.client_port().to_string()),
            // );
            // END TICKET

            result.insert(
                ZookeeperConfig::SSL_HOST_NAME_VERIFICATION.to_string(),
                Some("true".to_string()),
            );
            // The keystore and truststore passwords should not be in the configmap and are generated
            // and written later via script in the init container
            result.insert(
                ZookeeperConfig::SSL_KEY_STORE_LOCATION.to_string(),
                Some(format!("{dir}/keystore.p12", dir = CLIENT_TLS_DIR)),
            );
            result.insert(
                ZookeeperConfig::SSL_TRUST_STORE_LOCATION.to_string(),
                Some(format!("{dir}/truststore.p12", dir = CLIENT_TLS_DIR)),
            );
            // Check if we need to enable authentication
            if resource.client_tls_authentication_class().is_some() {
                result.insert(
                    ZookeeperConfig::SSL_CLIENT_AUTH.to_string(),
                    Some("need".to_string()),
                );
            }
        } else {
            result.insert(
                ZookeeperConfig::CLIENT_PORT.to_string(),
                Some(resource.client_port().to_string()),
            );
        }

        Ok(result)
    }
}

#[derive(strum::Display, strum::EnumString)]
#[strum(serialize_all = "camelCase")]
pub enum ZookeeperRole {
    Server,
}

#[derive(Clone, Default, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZookeeperClusterStatus {
    /// An opaque value that changes every time a discovery detail does
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_hash: Option<String>,
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum ListPodsError {
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
    #[snafu(display("failed to validate config of rolegroup {rolegroup}"))]
    RoleGroupValidation {
        rolegroup: RoleGroupRef<ZookeeperCluster>,
        source: ValidationError,
    },
}

impl ZookeeperCluster {
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

    pub fn rolegroup(
        &self,
        rolegroup_ref: &RoleGroupRef<ZookeeperCluster>,
    ) -> Option<(
        &Role<ZookeeperConfigFragment>,
        &RoleGroup<ZookeeperConfigFragment>,
    )> {
        match rolegroup_ref.role.parse().ok()? {
            ZookeeperRole::Server => {
                let role = &self.spec.servers.as_ref()?;
                let rg = role.role_groups.get(&rolegroup_ref.role_group)?;
                Some((role, rg))
            }
        }
    }

    /// List all pods expected to form the cluster
    ///
    /// We try to predict the pods here rather than looking at the current cluster state in order to
    /// avoid instance churn. For example, regenerating zoo.cfg based on the cluster state would lead to
    /// a lot of spurious restarts, as well as opening us up to dangerous split-brain conditions because
    /// the pods have inconsistent snapshots of which servers they should expect to be in quorum.
    pub fn pods(&self) -> Result<impl Iterator<Item = ZookeeperPodRef>, ListPodsError> {
        let ns = self
            .metadata
            .namespace
            .as_deref()
            .context(list_pods_error::NoNamespaceSnafu)?;
        // Order pods consistently, to avoid spurious downstream rewrites
        let mut pods = BTreeSet::new();
        for role in &self.spec.servers {
            for (rolegroup_name, rolegroup) in &role.role_groups {
                let rolegroup_ref = self.server_rolegroup_ref(rolegroup_name);
                let rg_config = rolegroup
                    .validate_config::<ZookeeperConfig>(
                        role,
                        &ZookeeperCluster::server_default_config(),
                    )
                    .with_context(|_| list_pods_error::RoleGroupValidationSnafu {
                        rolegroup: rolegroup_ref.clone(),
                    })?;
                pods.extend(
                    (0..rolegroup.replicas.unwrap_or(0)).map(move |i| ZookeeperPodRef {
                        namespace: ns.to_string(),
                        role_group_service_name: rolegroup_ref.object_name(),
                        pod_name: format!("{}-{}", rolegroup_ref.object_name(), i),
                        zookeeper_myid: i + rg_config.myid_offset,
                    }),
                );
            }
        }
        Ok(pods.into_iter())
    }

    pub fn client_port(&self) -> u16 {
        if self.is_client_secure() {
            SECURE_CLIENT_PORT
        } else {
            CLIENT_PORT
        }
    }

    /// Returns the secret class for client connection encryption. Defaults to `tls`.
    pub fn client_tls_secret_class(&self) -> String {
        let spec: &ZookeeperClusterSpec = &self.spec;
        spec.config
            .as_ref()
            .and_then(|c| c.tls_config.as_ref())
            .and_then(|tls| tls.tls.clone())
            .unwrap_or_default()
            .secret_class
    }

    /// Checks if we should use TLS to encrypt client connections.
    pub fn is_client_secure(&self) -> bool {
        let spec: &ZookeeperClusterSpec = &self.spec;
        spec.config
            .as_ref()
            .and_then(|c| c.tls_config.as_ref())
            .and_then(|tls| tls.tls.as_ref())
            .is_some()
    }

    /// Returns the authentication class used for client authentication
    pub fn client_tls_authentication_class(&self) -> Option<String> {
        let spec: &ZookeeperClusterSpec = &self.spec;
        spec.config
            .as_ref()
            .and_then(|c| c.tls_config.as_ref())
            .and_then(|tls| tls.client_authentication.as_ref())
            .map(|auth| auth.authentication_class.clone())
    }

    /// Returns the secret class for internal server encryption
    pub fn quorum_tls_secret_class(&self) -> String {
        let spec: &ZookeeperClusterSpec = &self.spec;
        spec.config
            .as_ref()
            .and_then(|c| c.tls_config.as_ref())
            .map(|tls| tls.quorum_tls_secret_class.as_ref())
            .unwrap_or(DEFAULT_SECRET_CLASS)
            .to_string()
    }

    pub fn server_default_config() -> ZookeeperConfigFragment {
        ZookeeperConfigFragment {
            myid_offset: Some(0),
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: None,
                    max: None,
                },
                memory: MemoryLimitsFragment {
                    limit: None,
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: StorageFragment {
                    data: PvcConfigFragment {
                        capacity: Some(Quantity("1Gi".to_owned())),
                        storage_class: None,
                        selectors: None,
                    },
                },
            },
            ..Default::default()
        }
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
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
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
