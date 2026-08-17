//! Builds the per-cluster `ZookeeperZnode` agent (spike): a Deployment running the operator binary
//! in `agent` mode, plus the ServiceAccount and (namespaced) RoleBinding it needs.
//!
//! Only produced when the cluster has `platformAccess` configured. Modelled on
//! [`statefulset`](super::statefulset) (for the Pod) and [`rbac`](super::rbac) (for the SA/binding),
//! but with a distinct role so the agent Pods are not selected by the server Services, and with a
//! credential volume that is identical in the Pod regardless of whether the source is a SecretClass
//! (CSI ephemeral) or a static `kubernetes.io/tls` Secret — the constraint the spike exists to prove.

use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            volume::{
                SecretFormat, SecretOperatorVolumeSourceBuilder,
                SecretOperatorVolumeSourceBuilderError, VolumeBuilder,
            },
        },
    },
    commons::secret_class::SecretClassVolumeProvisionParts,
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        api::{
            apps::v1::{Deployment, DeploymentSpec},
            core::v1::{
                EnvVar, EnvVarSource, ObjectFieldSelector, PodSecurityContext, SecretVolumeSource,
                ServiceAccount, Volume,
            },
            rbac::v1::{RoleBinding, RoleRef, Subject},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    shared::time::Duration,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        builder::meta::ownerreference_from_resource,
        types::operator::{RoleGroupName, RoleName},
    },
};

use crate::{
    crd::{APP_NAME, platform_access::v1alpha1::ZookeeperPlatformAccessCredential},
    zk_controller::validate::ValidatedCluster,
};

type Result<T, E = Error> = std::result::Result<T, E>;

// The agent is a distinct role: `recommended_labels`/`role_group_selector` would otherwise hardcode
// `server` and the agent Pods would be selected by the server Services.
stackable_operator::constant!(AGENT_ROLE_NAME: RoleName = "znode-agent");
stackable_operator::constant!(AGENT_ROLE_GROUP_NAME: RoleGroupName = "default");

/// Where the agent's client credential (`tls.crt` / `tls.key`) is mounted. The same path for both
/// credential sources, so the agent binary cannot tell which it got.
const PLATFORM_ACCESS_CERT_DIR: &str = "/stackable/platform_access_tls";
const PLATFORM_ACCESS_VOLUME_NAME: &str = "platform-access-tls";

/// Where the ZooKeeper **server** CA (`ca.crt`) is mounted, so the agent can verify the server it
/// connects to. This is the server SecretClass's CA, which may differ from the credential/trust-anchor
/// CA — mutual TLS needs both directions, and the credential's own `ca.crt` is the wrong one (and is
/// absent entirely for a static `kubernetes.io/tls` `secret`).
const SERVER_CA_DIR: &str = "/stackable/platform_access_server_ca";
const SERVER_CA_VOLUME_NAME: &str = "platform-access-server-ca";

/// The agent credential's requested lifetime. Must stay well above secret-operator's pod-restart
/// buffer (~6h): it refuses to issue a certificate whose lifetime is shorter, because it can't
/// schedule the pre-expiry pod restart in the past. Spike finding: "exercise cert renewal inside a
/// short test" is therefore infeasible — the restart-controller path only plays out over hours.
const AGENT_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build the platform-access credential volume"))]
    BuildCredentialVolume {
        source: SecretOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to add a volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add a volume mount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },
}

/// The Kubernetes resources for one cluster's znode agent.
pub struct ZnodeAgentResources {
    pub deployment: Deployment,
    pub service_account: ServiceAccount,
    pub role_binding: RoleBinding,
}

/// Builds the znode agent for `cluster`, or `None` if it has no `platformAccess` (no agent) or no
/// `operator_image` was provided (nothing to run).
pub fn build_znode_agent(
    cluster: &ValidatedCluster,
    operator_image: Option<&str>,
    image_repository: &str,
    cluster_info: &KubernetesClusterInfo,
) -> Result<Option<ZnodeAgentResources>> {
    let Some(platform_access) = cluster.cluster_config.zookeeper_security.platform_access() else {
        return Ok(None);
    };
    let Some(operator_image) = operator_image else {
        tracing::warn!(
            cluster = %cluster.name,
            "platformAccess is set but OPERATOR_IMAGE is unset; skipping the znode agent Deployment \
             (the operator fallback still provisions znodes)"
        );
        return Ok(None);
    };
    // The agent verifies the ZooKeeper server against the server SecretClass's CA. `validate` already
    // rejects platformAccess without a server SecretClass, so this is belt-and-suspenders.
    let Some(server_secret_class) = cluster.cluster_config.zookeeper_security.server_secret_class()
    else {
        tracing::warn!(
            cluster = %cluster.name,
            "platformAccess is set but no server TLS SecretClass is; skipping the znode agent"
        );
        return Ok(None);
    };

    let name = format!("{}-znode-agent", cluster.name);
    let namespace = cluster.namespace.to_string();
    let client_port = cluster.cluster_config.zookeeper_security.client_port();

    // --- Pod ---
    // Client identity (tls.crt/tls.key) from the credential; server trust (ca.crt) from the server
    // SecretClass — the two directions of mutual TLS, which may chain to different CAs.
    let credential_volume = build_credential_volume(&platform_access.credential)?;
    let server_ca_volume = build_server_ca_volume(server_secret_class.as_ref())?;

    let mut container = ContainerBuilder::new("znode-agent")
        .expect("'znode-agent' is a valid container name");
    container
        .image(operator_image)
        .image_pull_policy("IfNotPresent")
        // The image ENTRYPOINT is the operator binary; CMD is ["run"]. Setting `args` replaces the
        // CMD, so the binary runs in `agent` mode.
        .args(vec![
            "agent".to_string(),
            format!("--zookeeper-cluster-name={}", cluster.name),
            format!("--zookeeper-cluster-namespace={namespace}"),
            format!("--zookeeper-client-port={client_port}"),
            format!("--image-repository={image_repository}"),
            format!("--platform-access-cert-dir={PLATFORM_ACCESS_CERT_DIR}"),
            format!("--platform-access-server-ca-dir={SERVER_CA_DIR}"),
        ])
        // KUBERNETES_CLUSTER_DOMAIN is passed explicitly so the agent does NOT hit the kubelet
        // (`nodes/proxy`) to auto-detect the cluster domain — which would need cluster-scoped RBAC
        // the agent must not have.
        .add_env_var(
            "KUBERNETES_CLUSTER_DOMAIN",
            cluster_info.cluster_domain.to_string(),
        )
        .add_env_vars(vec![EnvVar {
            name: "KUBERNETES_NODE_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_string()),
                    field_path: "spec.nodeName".to_string(),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        }])
        .add_volume_mount(PLATFORM_ACCESS_VOLUME_NAME, PLATFORM_ACCESS_CERT_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(SERVER_CA_VOLUME_NAME, SERVER_CA_DIR)
        .context(AddVolumeMountSnafu)?;
    let container = container.build();

    // Pod template metadata: recommended labels for the agent role, plus the image annotation the
    // Tiltfile rewrites for live reload.
    let mut pod_metadata = ObjectMetaBuilder::new()
        .with_labels(cluster.recommended_labels_for(&AGENT_ROLE_NAME, &AGENT_ROLE_GROUP_NAME))
        .build();
    pod_metadata
        .annotations
        .get_or_insert_with(Default::default)
        .insert(
            "internal.stackable.tech/image".to_string(),
            operator_image.to_string(),
        );

    let mut pod_builder = PodBuilder::new();
    pod_builder
        .metadata(pod_metadata)
        .add_container(container)
        .add_volume(credential_volume)
        .context(AddVolumeSnafu)?
        .add_volume(server_ca_volume)
        .context(AddVolumeSnafu)?
        .service_account_name(name.clone())
        .security_context(PodSecurityContext {
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        });
    let pod_template = pod_builder.build_template();

    // --- Deployment ---
    let deployment_metadata = ObjectMetaBuilder::new()
        .name_and_namespace(cluster)
        .name(name.clone())
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(cluster.recommended_labels_for(&AGENT_ROLE_NAME, &AGENT_ROLE_GROUP_NAME))
        // CSI ephemeral volumes are provisioned at Pod start and never refreshed in place; the
        // commons-operator restart controller rolls the Pod ahead of cert expiry.
        .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
        .build();

    let deployment = Deployment {
        metadata: deployment_metadata,
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                // Version-free: Deployment selectors are immutable but the version label changes on
                // upgrade.
                match_labels: Some(
                    cluster
                        .role_group_selector_for(&AGENT_ROLE_NAME, &AGENT_ROLE_GROUP_NAME)
                        .into(),
                ),
                ..LabelSelector::default()
            },
            template: pod_template,
            ..DeploymentSpec::default()
        }),
        status: None,
    };

    // --- ServiceAccount + RoleBinding (hand-rolled: `rbac::build_service_account` hardcodes the
    // `<cluster>-serviceaccount` name). All three resources share the `<cluster>-znode-agent` name;
    // different kinds, no collision. ---
    let rbac_labels = cluster.recommended_labels_for(&AGENT_ROLE_NAME, &AGENT_ROLE_GROUP_NAME);

    let service_account = ServiceAccount {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(name.clone())
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            .with_labels(rbac_labels.clone())
            .build(),
        ..ServiceAccount::default()
    };

    let role_binding = RoleBinding {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(name.clone())
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            .with_labels(rbac_labels)
            .build(),
        role_ref: RoleRef {
            api_group: Some("rbac.authorization.k8s.io".to_string()),
            kind: "ClusterRole".to_string(),
            name: format!("{APP_NAME}-znode-agent-clusterrole"),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_string(),
            name: name.clone(),
            namespace: Some(namespace),
            ..Subject::default()
        }]),
    };

    Ok(Some(ZnodeAgentResources {
        deployment,
        service_account,
        role_binding,
    }))
}

/// Builds the agent's client-credential volume. Both variants land on `tls.crt` / `tls.key` under
/// [`PLATFORM_ACCESS_CERT_DIR`], so the agent code is identical across them.
fn build_credential_volume(credential: &ZookeeperPlatformAccessCredential) -> Result<Volume> {
    let volume = match credential {
        ZookeeperPlatformAccessCredential::SecretClass(secret_class) => {
            VolumeBuilder::new(PLATFORM_ACCESS_VOLUME_NAME)
                .ephemeral(
                    SecretOperatorVolumeSourceBuilder::new(
                        secret_class.as_ref(),
                        // The agent needs both the certificate and the private key.
                        SecretClassVolumeProvisionParts::PublicPrivate,
                    )
                    .with_pod_scope()
                    .with_format(SecretFormat::TlsPem)
                    .with_auto_tls_cert_lifetime(AGENT_SECRET_LIFETIME)
                    .build()
                    .context(BuildCredentialVolumeSnafu)?,
                )
                .build()
        }
        ZookeeperPlatformAccessCredential::Secret(secret_name) => Volume {
            name: PLATFORM_ACCESS_VOLUME_NAME.to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(secret_name.clone()),
                // A missing Secret then mounts empty instead of wedging the Pod; the agent starts,
                // finds no tls.crt, and reports through the normal path (spike step 4c).
                optional: Some(true),
                ..SecretVolumeSource::default()
            }),
            ..Volume::default()
        },
    };
    Ok(volume)
}

/// Mounts **only** the CA (`ca.crt`) of the ZooKeeper server SecretClass, so the agent can verify the
/// server it connects to. Public parts only — no client key.
fn build_server_ca_volume(secret_class_name: &str) -> Result<Volume> {
    let volume = VolumeBuilder::new(SERVER_CA_VOLUME_NAME)
        .ephemeral(
            SecretOperatorVolumeSourceBuilder::new(
                secret_class_name,
                SecretClassVolumeProvisionParts::Public,
            )
            .with_format(SecretFormat::TlsPem)
            .with_auto_tls_cert_lifetime(AGENT_SECRET_LIFETIME)
            .build()
            .context(BuildCredentialVolumeSnafu)?,
        )
        .build();
    Ok(volume)
}

#[cfg(test)]
mod tests {
    use stackable_operator::v2::types::{kubernetes::SecretClassName, operator::RoleGroupName};

    use super::*;
    use crate::{
        crd::platform_access::v1alpha1::ZookeeperPlatformAccess,
        zk_controller::test_support::{cluster_info, minimal_zk, validated_cluster},
    };

    fn platform_access_cluster() -> ValidatedCluster {
        let mut zk = minimal_zk(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: simple-zookeeper
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleGroups:
                  default:
                    replicas: 1
            "#,
        );
        // Set in Rust: serde_yaml can't represent the externally-tagged credential enum as a map.
        let tls = SecretClassName::from_str("tls").expect("valid SecretClass name");
        zk.spec.cluster_config.platform_access = Some(ZookeeperPlatformAccess {
            trust_anchor_secret_class: tls.clone(),
            credential: ZookeeperPlatformAccessCredential::SecretClass(tls),
        });
        validated_cluster(&zk)
    }

    fn plain_cluster() -> ValidatedCluster {
        validated_cluster(&minimal_zk(
            r#"
            apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: simple-zookeeper
            spec:
              image:
                productVersion: "3.9.5"
              servers:
                roleGroups:
                  default:
                    replicas: 1
            "#,
        ))
    }

    #[test]
    fn agent_is_built_only_with_platform_access() {
        // No platformAccess ⇒ no agent.
        assert!(
            build_znode_agent(&plain_cluster(), Some("img"), "repo", &cluster_info())
                .expect("build ok")
                .is_none()
        );
        // platformAccess but no OPERATOR_IMAGE ⇒ no agent (nothing to run).
        assert!(
            build_znode_agent(&platform_access_cluster(), None, "repo", &cluster_info())
                .expect("build ok")
                .is_none()
        );
    }

    #[test]
    fn agent_resources_share_the_name_and_bind_the_agent_clusterrole() {
        let agent = build_znode_agent(
            &platform_access_cluster(),
            Some("oci.example.org/op:0.0.0-dev"),
            "oci.example.org",
            &cluster_info(),
        )
        .expect("build ok")
        .expect("agent is built when platformAccess is set and an image is provided");

        assert_eq!(
            agent.deployment.metadata.name.as_deref(),
            Some("simple-zookeeper-znode-agent")
        );
        assert_eq!(
            agent.service_account.metadata.name.as_deref(),
            Some("simple-zookeeper-znode-agent")
        );
        assert_eq!(
            agent.role_binding.role_ref.name,
            "zookeeper-znode-agent-clusterrole"
        );
    }

    /// The agent Deployment's selector must NOT equal the server Services' selector, otherwise the
    /// agent Pods are load-balanced as if they were ZooKeeper servers. Distinct roles guarantee this.
    #[test]
    fn agent_selector_differs_from_server_selector() {
        let cluster = platform_access_cluster();
        let default_rg = RoleGroupName::from_str("default").expect("valid role group name");

        let server_selector: std::collections::BTreeMap<String, String> =
            cluster.role_group_selector(&default_rg).into();
        let agent_selector: std::collections::BTreeMap<String, String> = cluster
            .role_group_selector_for(&AGENT_ROLE_NAME, &AGENT_ROLE_GROUP_NAME)
            .into();

        assert_ne!(
            server_selector, agent_selector,
            "agent and server selectors must differ so agent pods are not picked up by server Services"
        );
    }
}
