//! Ensures that `Pod`s are configured and running for each [`v1alpha1::ZookeeperCluster`]
use std::{hash::Hasher, str::FromStr, sync::Arc};

use const_format::concatcp;
use fnv::FnvHasher;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::ClusterResourceApplyStrategy,
    commons::rbac::build_rbac_resources,
    kube::{
        api::DynamicObject,
        core::{DeserializeGuard, error_boundary},
        runtime::controller,
    },
    kvp::LabelError,
    logging::controller::ReconcilerError,
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    v2::{
        cluster_resources::cluster_resources_new,
        types::operator::{ControllerName, RoleGroupName},
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    APP_NAME, OPERATOR_NAME, ObjectRef,
    crd::{ZookeeperRole, v1alpha1},
    zk_controller::{
        build::resource::{
            config_map, discovery,
            listener::build_role_listener,
            pdb::build_pdb,
            service::{
                build_server_rolegroup_headless_service, build_server_rolegroup_metrics_service,
            },
            statefulset::build_server_rolegroup_statefulset,
        },
        validate::{operator_name, product_name},
    },
};

pub(crate) mod build;
mod dereference;
pub(crate) mod validate;

pub const ZK_CONTROLLER_NAME: &str = "zookeepercluster";
pub const ZK_FULL_CONTROLLER_NAME: &str = concatcp!(ZK_CONTROLLER_NAME, '.', OPERATOR_NAME);
pub const LISTENER_VOLUME_NAME: &str = "listener";
pub const LISTENER_VOLUME_DIR: &str = "/stackable/listener";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub operator_environment: OperatorEnvironmentOptions,
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("ZookeeperCluster object is invalid"))]
    InvalidZookeeperCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to dereference resources"))]
    Dereference { source: dereference::Error },

    #[snafu(display("failed to validate cluster"))]
    ValidateCluster { source: validate::Error },

    #[snafu(display("crd validation failure"))]
    CrdValidationFailure { source: crate::crd::Error },

    #[snafu(display("internal operator failure"))]
    InternalOperatorFailure { source: crate::crd::Error },

    #[snafu(display("failed to apply Service for role group {rolegroup}"))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupName,
    },

    #[snafu(display("failed to build ConfigMap for role group {rolegroup}"))]
    BuildRoleGroupConfigMap {
        source: config_map::Error,
        rolegroup: RoleGroupName,
    },

    #[snafu(display("failed to apply ConfigMap for role group {rolegroup}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupName,
    },

    #[snafu(display("failed to build StatefulSet for role group {rolegroup}"))]
    BuildRoleGroupStatefulSet {
        source: build::resource::statefulset::Error,
        rolegroup: RoleGroupName,
    },

    #[snafu(display("failed to apply StatefulSet for role group {rolegroup}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupName,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig { source: discovery::Error },

    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to create RBAC service account"))]
    ApplyServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to create RBAC role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphans {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply PodDisruptionBudget"))]
    ApplyPdb {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build label"))]
    BuildLabel { source: LabelError },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to apply group listener"))]
    ApplyGroupListener {
        source: stackable_operator::cluster_resources::Error,
    },
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::InvalidZookeeperCluster { .. } => None,
            Error::Dereference { .. } => None,
            Error::ValidateCluster { .. } => None,
            Error::CrdValidationFailure { .. } => None,
            Error::InternalOperatorFailure { .. } => None,
            Error::ApplyRoleGroupService { .. } => None,
            Error::BuildRoleGroupConfigMap { .. } => None,
            Error::ApplyRoleGroupConfig { .. } => None,
            Error::BuildRoleGroupStatefulSet { .. } => None,
            Error::ApplyRoleGroupStatefulSet { .. } => None,
            Error::ObjectMissingMetadataForOwnerRef { .. } => None,
            Error::BuildDiscoveryConfig { .. } => None,
            Error::ApplyDiscoveryConfig { .. } => None,
            Error::ApplyStatus { .. } => None,
            Error::ApplyServiceAccount { .. } => None,
            Error::ApplyRoleBinding { .. } => None,
            Error::BuildRbacResources { .. } => None,
            Error::DeleteOrphans { .. } => None,
            Error::ApplyPdb { .. } => None,
            Error::BuildLabel { .. } => None,
            Error::ObjectMeta { .. } => None,
            Error::ApplyGroupListener { .. } => None,
        }
    }
}

pub async fn reconcile_zk(
    zk: Arc<DeserializeGuard<v1alpha1::ZookeeperCluster>>,
    ctx: Arc<Ctx>,
) -> Result<controller::Action> {
    tracing::info!("Starting reconcile");
    let zk =
        zk.0.as_ref()
            .map_err(error_boundary::InvalidObject::clone)
            .context(InvalidZookeeperClusterSnafu)?;
    let client = &ctx.client;

    // dereference (client required)
    let dereferenced_objects = dereference::dereference(client, zk)
        .await
        .context(DereferenceSnafu)?;

    // validate (no client required)
    let validated_cluster =
        validate::validate(zk, &dereferenced_objects, &ctx.operator_environment)
            .context(ValidateClusterSnafu)?;

    // Names are derived from compile-time constants.
    let mut cluster_resources = cluster_resources_new(
        &product_name(),
        &operator_name(),
        &ControllerName::from_str(ZK_CONTROLLER_NAME)
            .expect("ZK_CONTROLLER_NAME should be a valid controller name"),
        &validated_cluster.name,
        &validated_cluster.namespace,
        &validated_cluster.uid,
        ClusterResourceApplyStrategy::from(&validated_cluster.cluster_operation),
        &validated_cluster.object_overrides,
    );

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        zk,
        APP_NAME,
        cluster_resources
            .get_required_labels()
            .context(BuildLabelSnafu)?,
    )
    .context(BuildRbacResourcesSnafu)?;

    cluster_resources
        .add(client, rbac_sa.clone())
        .await
        .context(ApplyServiceAccountSnafu)?;

    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    let zk_role = ZookeeperRole::Server;
    let server_role_group_configs = validated_cluster
        .role_group_configs
        .get(&zk_role)
        .into_iter()
        .flatten();
    for (rolegroup_name, rolegroup_config) in server_role_group_configs {
        // Resource naming, labels and owner references are derived from the `ValidatedCluster` and
        // the type-safe `RoleGroupName`.
        let rg_headless_service =
            build_server_rolegroup_headless_service(&validated_cluster, rolegroup_name);
        let rg_metrics_service = build_server_rolegroup_metrics_service(
            &validated_cluster,
            rolegroup_name,
            rolegroup_config,
        );
        let rg_configmap = config_map::build_server_rolegroup_config_map(
            &validated_cluster,
            &client.kubernetes_cluster_info,
            rolegroup_name,
            rolegroup_config,
        )
        .context(BuildRoleGroupConfigMapSnafu {
            rolegroup: rolegroup_name.clone(),
        })?;
        let rg_statefulset = build_server_rolegroup_statefulset(
            &validated_cluster,
            rolegroup_name,
            rolegroup_config,
        )
        .with_context(|_| BuildRoleGroupStatefulSetSnafu {
            rolegroup: rolegroup_name.clone(),
        })?;

        cluster_resources
            .add(client, rg_headless_service)
            .await
            .with_context(|_| ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup_name.clone(),
            })?;
        cluster_resources
            .add(client, rg_metrics_service)
            .await
            .with_context(|_| ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup_name.clone(),
            })?;
        cluster_resources
            .add(client, rg_configmap)
            .await
            .with_context(|_| ApplyRoleGroupConfigSnafu {
                rolegroup: rolegroup_name.clone(),
            })?;

        // Note: The StatefulSet needs to be applied after all ConfigMaps and Secrets it mounts
        // to prevent unnecessary Pod restarts.
        // See https://github.com/stackabletech/commons-operator/issues/111 for details.
        ss_cond_builder.add(
            cluster_resources
                .add(client, rg_statefulset)
                .await
                .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                    rolegroup: rolegroup_name.clone(),
                })?,
        );
    }

    if let Some(role_config) = &validated_cluster.role_config
        && let Some(pdb) = build_pdb(&role_config.pdb, &validated_cluster, &zk_role)
    {
        cluster_resources
            .add(client, pdb)
            .await
            .context(ApplyPdbSnafu)?;
    }

    let listener = build_role_listener(&validated_cluster, &zk_role);
    let applied_listener = cluster_resources
        .add(client, listener)
        .await
        .context(ApplyGroupListenerSnafu)?;

    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    let mut discovery_hash = FnvHasher::with_key(0);
    let discovery_cm = discovery::build_discovery_configmap(
        &validated_cluster,
        ZK_CONTROLLER_NAME,
        applied_listener,
    )
    .context(BuildDiscoveryConfigSnafu)?;

    let discovery_cm = cluster_resources
        .add(client, discovery_cm)
        .await
        .context(ApplyDiscoveryConfigSnafu)?;
    if let Some(generation) = discovery_cm.metadata.resource_version {
        discovery_hash.write(generation.as_bytes())
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&zk.spec.cluster_operation);

    let status = v1alpha1::ZookeeperClusterStatus {
        // Serialize as a string to discourage users from trying to parse the value,
        // and to keep things flexible if we end up changing the hasher at some point.
        discovery_hash: Some(discovery_hash.finish().to_string()),
        conditions: compute_conditions(zk, &[&ss_cond_builder, &cluster_operation_cond_builder]),
    };

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphansSnafu)?;
    client
        .apply_patch_status(OPERATOR_NAME, zk, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(controller::Action::await_change())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::ZookeeperCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> controller::Action {
    match error {
        // root object is invalid, will be requeued when modified anyway
        Error::InvalidZookeeperCluster { .. } => controller::Action::await_change(),

        _ => controller::Action::requeue(*Duration::from_secs(5)),
    }
}

/// Shared helpers for building validated test clusters from minimal YAML fixtures.
#[cfg(test)]
pub(crate) mod test_support {
    use stackable_operator::{
        cli::OperatorEnvironmentOptions, commons::networking::DomainName,
        utils::cluster_info::KubernetesClusterInfo,
    };

    use crate::{
        crd::{authentication::DereferencedAuthenticationClasses, v1alpha1},
        zk_controller::{
            dereference::DereferencedObjects,
            validate::{ValidatedCluster, validate},
        },
    };

    /// Parses a minimal `ZookeeperCluster` test fixture, defaulting `namespace`/`uid` so the
    /// validate step can build a [`ValidatedCluster`].
    pub fn minimal_zk(yaml: &str) -> v1alpha1::ZookeeperCluster {
        let mut zk: v1alpha1::ZookeeperCluster =
            serde_yaml::from_str(yaml).expect("invalid test ZookeeperCluster YAML");
        zk.metadata
            .namespace
            .get_or_insert_with(|| "default".to_owned());
        zk.metadata
            .uid
            .get_or_insert_with(|| "c27b3971-ca72-42c1-80a4-abdfc1db0ddd".to_owned());
        zk
    }

    pub fn cluster_info() -> KubernetesClusterInfo {
        KubernetesClusterInfo {
            cluster_domain: DomainName::try_from("cluster.local").expect("valid domain"),
        }
    }

    fn operator_environment() -> OperatorEnvironmentOptions {
        OperatorEnvironmentOptions {
            operator_namespace: "stackable-operators".to_owned(),
            operator_service_name: "zookeeper-operator".to_owned(),
            image_repository: "oci.example.org".to_owned(),
        }
    }

    /// Runs the real validate step against a minimal (auth-free) fixture.
    pub fn validated_cluster(zk: &v1alpha1::ZookeeperCluster) -> ValidatedCluster {
        validate(
            zk,
            &DereferencedObjects {
                authentication_classes: DereferencedAuthenticationClasses::new_for_tests(),
            },
            &operator_environment(),
        )
        .expect("validate should succeed for the test fixture")
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use stackable_operator::{
        k8s_openapi::api::core::v1::ConfigMap, v2::types::operator::RoleGroupName,
    };

    use super::*;
    use crate::zk_controller::test_support::{cluster_info, minimal_zk, validated_cluster};

    #[test]
    fn test_default_config() {
        let zookeeper_yaml = r#"
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
                replicas: 3
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();
        let config = cm.get("zoo.cfg").unwrap();
        assert!(config.contains(
            "authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider"
        ));
        assert!(config.contains("ssl.hostnameVerification=true"));
        // Default value
        assert!(config.contains("ssl.quorum.hostnameVerification=true"));

        assert!(cm.contains_key("security.properties"));
    }

    #[test]
    fn test_config_overrides() {
        let zookeeper_yaml = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.5"
          servers:
            configOverrides:
              zoo.cfg:
                foo: bar
                level: role
                hello-from-role: "true"
            roleGroups:
              default:
                configOverrides:
                  zoo.cfg:
                    foo: bar
                    level: role-group
                    ssl.quorum.hostnameVerification: "false"
                    hello-from-role-group: "true"
                replicas: 3
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();
        let config = cm.get("zoo.cfg").unwrap();
        assert!(config.contains("foo=bar"));
        assert!(config.contains("level=role-group"));
        assert!(config.contains("hello-from-role=true"));
        assert!(config.contains("hello-from-role-group=true"));
        assert!(config.contains(
            "authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider"
        ));
        assert!(config.contains("ssl.hostnameVerification=true"));
        // Overwritten by configOverride
        assert!(config.contains("ssl.quorum.hostnameVerification=false"));

        assert!(cm.contains_key("security.properties"));
    }

    #[test]
    fn test_seeded_operator_defaults() {
        // These values used to be injected by product-config from
        // `deploy/config-spec/properties.yaml`. They are now seeded directly by the
        // ConfigMap builder and must stay byte-identical (pinned by the kuttl
        // snapshot `tests/templates/kuttl/smoke/14-assert.yaml.j2`).
        let zookeeper_yaml = r#"
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
                replicas: 3
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();

        // `security.properties` is fully operator-injected; assert it byte-for-byte.
        assert_eq!(
            cm.get("security.properties").unwrap(),
            "networkaddress.cache.negative.ttl=0\nnetworkaddress.cache.ttl=5\n"
        );

        let zoo_cfg = cm.get("zoo.cfg").unwrap();
        for expected in [
            "admin.serverPort=8080",
            // new_for_tests() enables server TLS, so the secure client port is used.
            "clientPort=2282",
            "dataDir=/stackable/data",
            "initLimit=5",
            "syncLimit=2",
            "tickTime=3000",
            "metricsProvider.className=org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider",
            "metricsProvider.httpPort=7000",
        ] {
            assert!(
                zoo_cfg.contains(expected),
                "missing {expected:?} in:\n{zoo_cfg}"
            );
        }
    }

    #[test]
    fn test_user_config_overrides_seeded_default() {
        // A value set on the typed config must win over the seeded default.
        let zookeeper_yaml = r#"
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
                replicas: 3
                config:
                  tickTime: 4000
                  initLimit: 7
        "#;
        let cm = build_config_map(zookeeper_yaml).data.unwrap();
        let zoo_cfg = cm.get("zoo.cfg").unwrap();
        assert!(zoo_cfg.contains("tickTime=4000"), "{zoo_cfg}");
        assert!(zoo_cfg.contains("initLimit=7"), "{zoo_cfg}");
        // Untouched default stays.
        assert!(zoo_cfg.contains("syncLimit=2"), "{zoo_cfg}");
    }

    fn build_config_map(zookeeper_yaml: &str) -> ConfigMap {
        let zookeeper = minimal_zk(zookeeper_yaml);
        let validated_cluster = validated_cluster(&zookeeper);
        let role_group_name = RoleGroupName::from_str("default").expect("valid role group name");
        let rolegroup_config =
            &validated_cluster.role_group_configs[&ZookeeperRole::Server][&role_group_name];

        config_map::build_server_rolegroup_config_map(
            &validated_cluster,
            &cluster_info(),
            &role_group_name,
            rolegroup_config,
        )
        .unwrap()
    }
}
