//! Platform-access configuration: how the Stackable platform (specifically the per-cluster
//! `ZookeeperZnode` agent) authenticates to an auth-enabled ZooKeeper ensemble.
//!
//! Spike shape (see the ADR): the trust anchor and the credential are two separate fields. For a
//! minted certificate both name the same SecretClass, which is why one field looks sufficient — but
//! a customer-supplied static certificate has no SecretClass CA to expose, so what ZooKeeper trusts
//! must be stated on its own. The credential is an externally-tagged enum whose variant name is the
//! YAML key (`secretClass` / `secret`), mirroring Trino's `TrinoCatalogConnector` and Druid's
//! `MetadataDatabaseConnection`.

use serde::{Deserialize, Serialize};
use stackable_operator::{
    schemars::{self, JsonSchema},
    v2::types::kubernetes::SecretClassName,
    versioned::versioned,
};

#[versioned(version(name = "v1alpha1"))]
pub mod versioned {
    /// Grants the Stackable platform authenticated access to this ZooKeeper ensemble.
    ///
    /// When set, the operator runs a dedicated per-cluster agent that owns `ZookeeperZnode`
    /// provisioning and mounts its own client credential, and ZooKeeper is configured to require and
    /// trust client certificates from `trustAnchorSecretClass` (`ssl.clientAuth=need`). When unset
    /// (the default), znode provisioning falls back to the central operator connecting in plaintext.
    #[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ZookeeperPlatformAccess {
        /// The [SecretClass](DOCS_BASE_URL_PLACEHOLDER/secret-operator/secretclass) whose CA
        /// ZooKeeper should trust for platform (agent) client certificates. This is what
        /// `ssl.trustStore.location` is populated from (a PKCS#12 truststore minted by
        /// secret-operator).
        ///
        /// Kept separate from `credential` because a customer-supplied static certificate
        /// (`credential.secret`) has no SecretClass CA to expose, so the trust anchor must be stated
        /// explicitly. For a minted certificate this typically names the same SecretClass as
        /// `credential.secretClass`. Pointing it at the platform `tls` SecretClass reuses the autoTls
        /// CA — a choice the customer wrote down, not an implicit backdoor.
        pub trust_anchor_secret_class: SecretClassName,

        /// The client credential (certificate + private key) the agent authenticates with. Exactly
        /// one variant. Both variants land on the same files (`tls.crt` / `tls.key`) in the same
        /// directory, so the agent code is identical across them.
        pub credential: ZookeeperPlatformAccessCredential,
    }

    /// The source of the agent's client credential.
    ///
    /// Externally tagged: the variant name (`secretClass` / `secret`) is the YAML key.
    #[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub enum ZookeeperPlatformAccessCredential {
        /// A [SecretClass](DOCS_BASE_URL_PLACEHOLDER/secret-operator/secretclass) that
        /// secret-operator mints a certificate from, mounted into the agent via a CSI ephemeral
        /// volume (never written to etcd).
        SecretClass(SecretClassName),

        /// The name of an existing `kubernetes.io/tls` Secret (e.g. one cert-manager emits, or a
        /// certificate a customer dropped in). Its well-known `tls.crt` / `tls.key` keys are mounted
        /// directly, producing exactly the layout a secret-operator CSI volume does.
        Secret(String),
    }
}
