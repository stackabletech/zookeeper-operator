//! A helper module to process Apache ZooKeeper security configuration
//!
//! This module merges the `tls` and `authentication` module and offers better accessibility
//! and helper functions
//!
//! This is required due to overlaps between TLS encryption and e.g. mTLS authentication or Kerberos
use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        pod::{
            container::ContainerBuilder,
            volume::{
                SecretFormat, SecretOperatorVolumeSourceBuilder,
                SecretOperatorVolumeSourceBuilderError, VolumeBuilder,
            },
            PodBuilder,
        },
    },
    client::Client,
    commons::authentication::AuthenticationClassProvider,
    k8s_openapi::api::core::v1::Volume,
    time::Duration,
};

use crate::{authentication, authentication::ResolvedAuthenticationClasses, tls, ZookeeperCluster};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to process authentication class"))]
    InvalidAuthenticationClassConfiguration { source: authentication::Error },

    #[snafu(display("failed to build TLS volume for {volume_name:?}"))]
    BuildTlsVolume {
        source: SecretOperatorVolumeSourceBuilderError,
        volume_name: String,
    },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },
}

/// Helper struct combining TLS settings for server and quorum with the resolved AuthenticationClasses
pub struct ZookeeperSecurity {
    resolved_authentication_classes: ResolvedAuthenticationClasses,
    server_secret_class: Option<String>,
    quorum_secret_class: String,
}

impl ZookeeperSecurity {
    // ports
    pub const CLIENT_PORT: u16 = 2181;
    pub const CLIENT_PORT_NAME: &'static str = "clientPort";
    pub const SECURE_CLIENT_PORT: u16 = 2282;
    pub const SECURE_CLIENT_PORT_NAME: &'static str = "secureClientPort";
    // directories
    pub const QUORUM_TLS_DIR: &'static str = "/stackable/quorum_tls";
    pub const QUORUM_TLS_MOUNT_DIR: &'static str = "/stackable/quorum_tls_mount";
    pub const SERVER_TLS_DIR: &'static str = "/stackable/server_tls";
    pub const SERVER_TLS_MOUNT_DIR: &'static str = "/stackable/server_tls_mount";
    pub const SYSTEM_TRUST_STORE_DIR: &'static str = "/etc/pki/java/cacerts";
    // Quorum TLS
    pub const SSL_QUORUM: &'static str = "sslQuorum";
    pub const SSL_QUORUM_CLIENT_AUTH: &'static str = "ssl.quorum.clientAuth";
    pub const SSL_QUORUM_HOST_NAME_VERIFICATION: &'static str = "ssl.quorum.hostnameVerification";
    pub const SSL_QUORUM_KEY_STORE_LOCATION: &'static str = "ssl.quorum.keyStore.location";
    pub const SSL_QUORUM_KEY_STORE_PASSWORD: &'static str = "ssl.quorum.keyStore.password";
    pub const SSL_QUORUM_TRUST_STORE_LOCATION: &'static str = "ssl.quorum.trustStore.location";
    pub const SSL_QUORUM_TRUST_STORE_PASSWORD: &'static str = "ssl.quorum.trustStore.password";
    // Client TLS
    pub const SSL_CLIENT_AUTH: &'static str = "ssl.clientAuth";
    pub const SSL_HOST_NAME_VERIFICATION: &'static str = "ssl.hostnameVerification";
    pub const SSL_KEY_STORE_LOCATION: &'static str = "ssl.keyStore.location";
    pub const SSL_KEY_STORE_PASSWORD: &'static str = "ssl.keyStore.password";
    pub const SSL_TRUST_STORE_LOCATION: &'static str = "ssl.trustStore.location";
    pub const SSL_TRUST_STORE_PASSWORD: &'static str = "ssl.trustStore.password";
    // Common TLS
    pub const SSL_AUTH_PROVIDER_X509: &'static str = "authProvider.x509";
    pub const SERVER_CNXN_FACTORY: &'static str = "serverCnxnFactory";
    // Mis
    pub const STORE_PASSWORD_ENV: &'static str = "STORE_PASSWORD";

    /// Create a `ZookeeperSecurity` struct from the Zookeeper custom resource and resolve
    /// all provided `AuthenticationClass` references.
    pub async fn new_from_zookeeper_cluster(
        client: &Client,
        zk: &ZookeeperCluster,
    ) -> Result<Self, Error> {
        Ok(ZookeeperSecurity {
            resolved_authentication_classes: authentication::resolve_authentication_classes(
                client,
                &zk.spec.cluster_config.authentication,
            )
            .await
            .context(InvalidAuthenticationClassConfigurationSnafu)?,
            server_secret_class: zk
                .spec
                .cluster_config
                .tls
                .as_ref()
                .and_then(|tls| tls.server_secret_class.clone()),
            quorum_secret_class: zk
                .spec
                .cluster_config
                .tls
                .as_ref()
                .map(|tls| tls.quorum_secret_class.clone())
                .unwrap_or_else(tls::quorum_tls_default),
        })
    }

    /// Check if TLS encryption is enabled. This could be due to:
    /// - A provided server `SecretClass`
    /// - A provided client `AuthenticationClass`
    ///
    /// This affects init container commands, ZooKeeper configuration, volume mounts and
    /// the ZooKeeper client port
    pub fn tls_enabled(&self) -> bool {
        // TODO: This must be adapted if other authentication methods are supported and require TLS
        self.server_secret_class.is_some()
            || self
                .resolved_authentication_classes
                .get_tls_authentication_class()
                .is_some()
    }

    /// Return the ZooKeeper (secure) client port depending on tls or authentication settings.
    pub fn client_port(&self) -> u16 {
        if self.tls_enabled() {
            Self::SECURE_CLIENT_PORT
        } else {
            Self::CLIENT_PORT
        }
    }

    /// Adds required volumes and volume mounts to the pod and container builders
    /// depending on the tls and authentication settings.
    pub fn add_volume_mounts(
        &self,
        pod_builder: &mut PodBuilder,
        cb_zookeeper: &mut ContainerBuilder,
        requested_secret_lifetime: &Duration,
    ) -> Result<()> {
        let tls_secret_class = self.get_tls_secret_class();

        if let Some(secret_class) = tls_secret_class {
            let tls_volume_name = "server-tls";
            cb_zookeeper
                .add_volume_mount(tls_volume_name, Self::SERVER_TLS_DIR)
                .context(AddVolumeMountSnafu)?;
            pod_builder
                .add_volume(Self::create_tls_volume(
                    tls_volume_name,
                    secret_class,
                    requested_secret_lifetime,
                )?)
                .context(AddVolumeSnafu)?;
        }

        // quorum
        let tls_volume_name = "quorum-tls";
        cb_zookeeper
            .add_volume_mount(tls_volume_name, Self::QUORUM_TLS_DIR)
            .context(AddVolumeMountSnafu)?;
        pod_builder
            .add_volume(Self::create_tls_volume(
                tls_volume_name,
                &self.quorum_secret_class,
                requested_secret_lifetime,
            )?)
            .context(AddVolumeSnafu)?;

        Ok(())
    }

    /// Returns required ZooKeeper configuration settings for the `zoo.cfg` properties file
    /// depending on the tls and authentication settings.
    pub fn config_settings(&self) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();
        // Quorum TLS
        config.insert(Self::SSL_QUORUM.to_string(), "true".to_string());
        config.insert(
            Self::SSL_QUORUM_HOST_NAME_VERIFICATION.to_string(),
            "true".to_string(),
        );
        config.insert(Self::SSL_QUORUM_CLIENT_AUTH.to_string(), "need".to_string());
        config.insert(
            Self::SERVER_CNXN_FACTORY.to_string(),
            "org.apache.zookeeper.server.NettyServerCnxnFactory".to_string(),
        );
        config.insert(
            Self::SSL_AUTH_PROVIDER_X509.to_string(),
            "org.apache.zookeeper.server.auth.X509AuthenticationProvider".to_string(),
        );
        // The keystore and truststore passwords should not be in the configmap and are generated
        // and written later via script in the init container
        config.insert(
            Self::SSL_QUORUM_KEY_STORE_LOCATION.to_string(),
            format!("{dir}/keystore.p12", dir = Self::QUORUM_TLS_DIR),
        );
        config.insert(
            Self::SSL_QUORUM_TRUST_STORE_LOCATION.to_string(),
            format!("{dir}/truststore.p12", dir = Self::QUORUM_TLS_DIR),
        );

        // Server TLS
        if self.tls_enabled() {
            // We set only the clientPort and portUnification here because otherwise there is a port bind exception
            // See: https://issues.apache.org/jira/browse/ZOOKEEPER-4276
            // --> Normally we would like to only set the secureClientPort (check out commented code below)
            // What we tried:
            // 1) Set clientPort and secureClientPort will fail with
            // "static.config different from dynamic config .. "
            // config.insert(
            //     Self::CLIENT_PORT_NAME.to_string(),
            //     CLIENT_PORT.to_string(),
            // );
            // config.insert(
            //     Self::SECURE_CLIENT_PORT_NAME.to_string(),
            //     SECURE_CLIENT_PORT.to_string(),
            // );

            // 2) Setting only secureClientPort will config in the above mentioned bind exception.
            // The NettyFactory tries to bind multiple times on the secureClientPort.
            // config.insert(
            //     Self::SECURE_CLIENT_PORT_NAME.to_string(),
            //     self.client_port(.to_string()),
            // );

            // 3) Using the clientPort and portUnification still allows plaintext connection without
            // authentication, but at least TLS and authentication works when connecting securely.
            config.insert(
                Self::CLIENT_PORT_NAME.to_string(),
                self.client_port().to_string(),
            );
            config.insert("client.portUnification".to_string(), "true".to_string());
            // TODO: Remove clientPort and portUnification (above) in favor of secureClientPort once the bug is fixed
            // config.insert(
            //     Self::SECURE_CLIENT_PORT_NAME.to_string(),
            //     self.client_port(.to_string()),
            // );
            // END TICKET

            config.insert(
                Self::SSL_HOST_NAME_VERIFICATION.to_string(),
                "true".to_string(),
            );
            // The keystore and truststore passwords should not be in the configmap and are generated
            // and written later via script in the init container
            config.insert(
                Self::SSL_KEY_STORE_LOCATION.to_string(),
                format!("{dir}/keystore.p12", dir = Self::SERVER_TLS_DIR),
            );
            config.insert(
                Self::SSL_TRUST_STORE_LOCATION.to_string(),
                format!("{dir}/truststore.p12", dir = Self::SERVER_TLS_DIR),
            );
            // Check if we need to enable client TLS authentication
            if self
                .resolved_authentication_classes
                .get_tls_authentication_class()
                .is_some()
            {
                config.insert(Self::SSL_CLIENT_AUTH.to_string(), "need".to_string());
            }
        } else {
            config.insert(
                Self::CLIENT_PORT_NAME.to_string(),
                self.client_port().to_string(),
            );
        }

        config
    }

    /// Returns the `SecretClass` provided in a `AuthenticationClass` for TLS.
    fn get_tls_secret_class(&self) -> Option<&String> {
        self.resolved_authentication_classes
            .get_tls_authentication_class()
            .and_then(|auth_class| match &auth_class.spec.provider {
                AuthenticationClassProvider::Tls(tls) => tls.client_cert_secret_class.as_ref(),
                AuthenticationClassProvider::Ldap(_)
                | AuthenticationClassProvider::Oidc(_)
                | AuthenticationClassProvider::Static(_)
                | AuthenticationClassProvider::Kerberos(_) => None,
            })
            .or(self.server_secret_class.as_ref())
    }

    /// Creates ephemeral volumes to mount the `SecretClass` into the Pods
    fn create_tls_volume(
        volume_name: &str,
        secret_class_name: &str,
        requested_secret_lifetime: &Duration,
    ) -> Result<Volume> {
        let volume = VolumeBuilder::new(volume_name)
            .ephemeral(
                SecretOperatorVolumeSourceBuilder::new(secret_class_name)
                    .with_pod_scope()
                    .with_node_scope()
                    .with_format(SecretFormat::TlsPkcs12)
                    .with_auto_tls_cert_lifetime(*requested_secret_lifetime)
                    .build()
                    .context(BuildTlsVolumeSnafu { volume_name })?,
            )
            .build();

        Ok(volume)
    }

    /// USE ONLY IN TESTS! We can not put it behind `#[cfg(test)]` because of <https://github.com/rust-lang/cargo/issues/8379>
    pub fn new_for_tests() -> Self {
        ZookeeperSecurity {
            resolved_authentication_classes: ResolvedAuthenticationClasses::new_for_tests(),
            server_secret_class: Some("tls".to_owned()),
            quorum_secret_class: "tls".to_string(),
        }
    }
}
