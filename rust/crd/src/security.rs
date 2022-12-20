//! A helper module to process Apache ZooKeeper security configuration
//!
//! This module merges the `tls` and `authentication` module and offers better accessibility
//! and helper functions
//!
//! This is required due to overlaps between TLS encryption and e.g. mTLS authentication or Kerberos

use crate::{
    authentication, authentication::ResolvedAuthenticationClasses, tls, ZookeeperCluster,
    STACKABLE_RW_CONFIG_DIR, ZOOKEEPER_PROPERTIES_FILE,
};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{ContainerBuilder, PodBuilder, SecretOperatorVolumeSourceBuilder, VolumeBuilder},
    client::Client,
    commons::authentication::AuthenticationClassProvider,
    k8s_openapi::api::core::v1::Volume,
};
use std::collections::BTreeMap;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to process authentication class"))]
    InvalidAuthenticationClassConfiguration { source: authentication::Error },
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

    /// Returns required (init) container commands to generate keystores and truststores
    /// depending on the tls and authentication settings.
    pub fn commands(&self) -> Vec<String> {
        let mut args = vec![];
        // Quorum
        args.push(Self::generate_password(Self::STORE_PASSWORD_ENV));
        args.extend(Self::create_key_and_trust_store_cmd(
            Self::QUORUM_TLS_MOUNT_DIR,
            Self::QUORUM_TLS_DIR,
            "quorum-tls",
            Self::STORE_PASSWORD_ENV,
        ));
        args.extend(vec![
            Self::write_store_password_to_config(
                Self::SSL_QUORUM_KEY_STORE_PASSWORD,
                STACKABLE_RW_CONFIG_DIR,
                Self::STORE_PASSWORD_ENV,
            ),
            Self::write_store_password_to_config(
                Self::SSL_QUORUM_TRUST_STORE_PASSWORD,
                STACKABLE_RW_CONFIG_DIR,
                Self::STORE_PASSWORD_ENV,
            ),
        ]);

        // server-tls or client-auth-tls (only the certificates specified are accepted)
        if self.tls_enabled() {
            args.push(Self::generate_password(Self::STORE_PASSWORD_ENV));

            args.extend(Self::create_key_and_trust_store_cmd(
                Self::SERVER_TLS_MOUNT_DIR,
                Self::SERVER_TLS_DIR,
                "server-tls",
                Self::STORE_PASSWORD_ENV,
            ));

            args.extend(vec![
                Self::write_store_password_to_config(
                    Self::SSL_KEY_STORE_PASSWORD,
                    STACKABLE_RW_CONFIG_DIR,
                    Self::STORE_PASSWORD_ENV,
                ),
                Self::write_store_password_to_config(
                    Self::SSL_TRUST_STORE_PASSWORD,
                    STACKABLE_RW_CONFIG_DIR,
                    Self::STORE_PASSWORD_ENV,
                ),
            ]);
        }

        args
    }

    /// Adds required volumes and volume mounts to the pod and container builders
    /// depending on the tls and authentication settings.
    pub fn add_volume_mounts(
        &self,
        pod_builder: &mut PodBuilder,
        cb_prepare: &mut ContainerBuilder,
        cb_zookeeper: &mut ContainerBuilder,
    ) {
        let tls_secret_class = self.get_tls_secret_class();

        if let Some(secret_class) = tls_secret_class {
            // mounts for secret volume
            cb_prepare.add_volume_mount("server-tls-mount", Self::SERVER_TLS_MOUNT_DIR);
            cb_zookeeper.add_volume_mount("server-tls-mount", Self::SERVER_TLS_MOUNT_DIR);
            pod_builder.add_volume(Self::create_tls_volume("server-tls-mount", secret_class));
            // empty mount for trust and keystore
            cb_prepare.add_volume_mount("server-tls", Self::SERVER_TLS_DIR);
            cb_zookeeper.add_volume_mount("server-tls", Self::SERVER_TLS_DIR);
            pod_builder.add_volume(
                VolumeBuilder::new("server-tls")
                    .with_empty_dir(Some(""), None)
                    .build(),
            );
        }

        // quorum
        // mounts for secret volume
        cb_prepare.add_volume_mount("quorum-tls-mount", Self::QUORUM_TLS_MOUNT_DIR);
        cb_zookeeper.add_volume_mount("quorum-tls-mount", Self::QUORUM_TLS_MOUNT_DIR);
        pod_builder.add_volume(Self::create_tls_volume(
            "quorum-tls-mount",
            &self.quorum_secret_class,
        ));
        // empty mount for trust and keystore
        cb_prepare.add_volume_mount("quorum-tls", Self::QUORUM_TLS_DIR);
        cb_zookeeper.add_volume_mount("quorum-tls", Self::QUORUM_TLS_DIR);
        pod_builder.add_volume(
            VolumeBuilder::new("quorum-tls")
                .with_empty_dir(Some(""), None)
                .build(),
        );
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
                _ => None,
            })
            .or(self.server_secret_class.as_ref())
    }

    /// Creates ephemeral volumes to mount the `SecretClass` into the Pods
    fn create_tls_volume(volume_name: &str, secret_class_name: &str) -> Volume {
        VolumeBuilder::new(volume_name)
            .ephemeral(
                SecretOperatorVolumeSourceBuilder::new(secret_class_name)
                    .with_pod_scope()
                    .with_node_scope()
                    .build(),
            )
            .build()
    }

    /// Generates the shell script to retrieve a random 20 character password
    fn generate_password(store_password_env_var: &str) -> String {
        format!(
            "export {store_password_env_var}=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 20 ; echo '')",
        )
    }

    /// Generates the shell script to append the generated password from `generate_password()`
    /// to the zoo.cfg to set key and truststore passwords.
    fn write_store_password_to_config(
        property: &str,
        rw_conf_dir: &str,
        store_password_env_var: &str,
    ) -> String {
        format!(
            "echo {property}=${store_password_env_var} >> {rw_conf_dir}/{ZOOKEEPER_PROPERTIES_FILE}",
        )
    }

    /// Generates the shell script to create key and trust stores from the certificates provided
    /// by the secret operator
    fn create_key_and_trust_store_cmd(
        mount_directory: &str,
        stackable_directory: &str,
        alias_name: &str,
        store_password_env_var: &str,
    ) -> Vec<String> {
        vec![
            format!("echo [{stackable_directory}] Cleaning up truststore - just in case"),
            format!("rm -f {stackable_directory}/truststore.p12"),
            format!("echo [{stackable_directory}] Creating truststore"),
            format!("keytool -importcert -file {mount_directory}/ca.crt -keystore {stackable_directory}/truststore.p12 -storetype pkcs12 -noprompt -alias {alias_name} -storepass ${store_password_env_var}"),
            format!("echo [{stackable_directory}] Creating certificate chain"),
            format!("cat {mount_directory}/ca.crt {mount_directory}/tls.crt > {stackable_directory}/chain.crt"),
            format!("echo [{stackable_directory}] Creating keystore"),
            format!("openssl pkcs12 -export -in {stackable_directory}/chain.crt -inkey {mount_directory}/tls.key -out {stackable_directory}/keystore.p12 --passout pass:${store_password_env_var}"),
        ]
    }
}
