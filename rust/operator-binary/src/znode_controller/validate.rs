//! The validate step in the ZookeeperZnode controller.
//!
//! Synchronously validates inputs that don't require a Kubernetes client. Produces
//! [`ValidatedZnode`], consumed by the rest of `reconcile_znode`.

use std::str::FromStr;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::{cluster_operation::ClusterOperation, product_image_selection},
    deep_merger::ObjectOverrides,
    k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    kube::Resource,
    kvp::LabelValue,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        controller_utils::{get_namespace, get_uid},
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::ProductVersion,
        },
    },
};

use crate::{
    crd::{
        CONTAINER_IMAGE_BASE_NAME, ZOOKEEPER_SERVER_PORT_NAME, authentication,
        security::ZookeeperSecurity, v1alpha1,
    },
    listener_addresses::{self, ListenerAddresses, listener_addresses},
    znode_controller::dereference::DereferencedObjects,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to validate authentication classes"))]
    InvalidAuthenticationClassConfiguration { source: authentication::Error },

    #[snafu(display("object has no name"))]
    ObjectMissingName,

    #[snafu(display("the object name {name:?} is not a valid label value"))]
    InvalidNameLabelValue {
        source: stackable_operator::kvp::LabelValueError,
        name: String,
    },

    #[snafu(display("failed to get the namespace"))]
    GetNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to get the UID"))]
    GetUid {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to parse the product version {product_version:?}"))]
    ParseProductVersion {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        product_version: String,
    },

    #[snafu(display("failed to read the addresses published by the ZooKeeper role Listener"))]
    ReadRoleListenerAddresses { source: listener_addresses::Error },

    #[snafu(display(
        "the ZooKeeper role Listener does not exist yet, or has not published any addresses yet"
    ))]
    NoRoleListenerAddresses,
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The validated [`v1alpha1::ZookeeperZnode`]. Carries the synchronous inputs the rest of
/// `reconcile_znode` needs after dereferencing, plus the znode's identity so it can act as the
/// owner [`Resource`] of the discovery ConfigMap (mirroring the cluster controller's
/// `ValidatedCluster`).
pub struct ValidatedZnode {
    /// Mirrors the znode's [`ObjectMeta`] (name, namespace, UID) so it can be used as the owner
    /// [`Resource`] for the discovery ConfigMap without reaching back into the raw
    /// [`v1alpha1::ZookeeperZnode`].
    metadata: ObjectMeta,
    /// The znode name, validated to be a valid label value (used for the `app.kubernetes.io/instance`
    /// label and the owner reference / ConfigMap name).
    pub name: String,
    pub namespace: NamespaceName,
    pub uid: Uid,
    /// The product version as a valid label value, used for the recommended
    /// `app.kubernetes.io/version` label.
    pub product_version: ProductVersion,
    pub zookeeper_security: ZookeeperSecurity,
    /// The parent cluster's operation settings (pause/stop), from which the
    /// [`ClusterResourceApplyStrategy`](stackable_operator::cluster_resources::ClusterResourceApplyStrategy)
    /// for the znode's resources is derived. Carried here so the apply step does not reach into the
    /// cluster spec.
    pub cluster_operation: ClusterOperation,
    /// Object overrides applied to the znode's resources, carried so the apply step does not reach
    /// into the raw [`v1alpha1::ZookeeperZnode`].
    pub object_overrides: ObjectOverrides,
    /// The client addresses published by the referenced cluster's role Listener, which the znode's
    /// discovery ConfigMap advertises.
    ///
    /// Unlike the cluster controller, the znode controller cannot produce anything without them,
    /// so validation fails while they are missing and the reconciliation is retried.
    pub discovery_addresses: ListenerAddresses,
}

impl HasName for ValidatedZnode {
    fn to_name(&self) -> String {
        self.name.clone()
    }
}

impl HasUid for ValidatedZnode {
    fn to_uid(&self) -> Uid {
        self.uid.clone()
    }
}

impl NameIsValidLabelValue for ValidatedZnode {
    fn to_label_value(&self) -> String {
        self.name.clone()
    }
}

impl Resource for ValidatedZnode {
    type DynamicType = <v1alpha1::ZookeeperZnode as Resource>::DynamicType;
    type Scope = <v1alpha1::ZookeeperZnode as Resource>::Scope;

    fn kind(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::ZookeeperZnode::kind(dt)
    }

    fn group(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::ZookeeperZnode::group(dt)
    }

    fn version(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::ZookeeperZnode::version(dt)
    }

    fn plural(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::ZookeeperZnode::plural(dt)
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

/// Validates the dereferenced inputs.
pub fn validate(
    znode: &v1alpha1::ZookeeperZnode,
    dereferenced_objects: &DereferencedObjects,
    operator_environment: &OperatorEnvironmentOptions,
) -> Result<ValidatedZnode> {
    let image = dereferenced_objects
        .zk
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            &operator_environment.image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let resolved_authentication_classes = dereferenced_objects
        .authentication_classes
        .validate()
        .context(InvalidAuthenticationClassConfigurationSnafu)?;

    let zookeeper_security =
        ZookeeperSecurity::new(&dereferenced_objects.zk, resolved_authentication_classes);

    // Scoped to this function so the `Lookup` metadata accessors don't collide with `Resource`'s
    // in the `impl Resource for ValidatedZnode` block.
    use stackable_operator::kube::runtime::reflector::Lookup;
    let name = znode.name().context(ObjectMissingNameSnafu)?.into_owned();
    // The name is used as the `app.kubernetes.io/instance` label of the discovery ConfigMap, so it
    // must be a valid label value. Validate it here to fail gracefully rather than panic later.
    LabelValue::from_str(&name)
        .with_context(|_| InvalidNameLabelValueSnafu { name: name.clone() })?;

    let namespace = get_namespace(znode).context(GetNamespaceSnafu)?;
    let uid = get_uid(znode).context(GetUidSnafu)?;
    let product_version =
        ProductVersion::from_str(&image.app_version_label_value).with_context(|_| {
            ParseProductVersionSnafu {
                product_version: image.app_version_label_value.to_string(),
            }
        })?;

    let discovery_addresses = dereferenced_objects
        .maybe_role_listener
        .as_ref()
        .map(|listener| listener_addresses(listener, ZOOKEEPER_SERVER_PORT_NAME))
        .transpose()
        .context(ReadRoleListenerAddressesSnafu)?
        .flatten()
        .context(NoRoleListenerAddressesSnafu)?;

    Ok(ValidatedZnode {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(namespace.to_string()),
            uid: Some(uid.to_string()),
            ..ObjectMeta::default()
        },
        name,
        namespace,
        uid,
        product_version,
        zookeeper_security,
        cluster_operation: dereferenced_objects.zk.spec.cluster_operation.clone(),
        object_overrides: znode.spec.object_overrides.clone(),
        discovery_addresses,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        listener_addresses::test_support::{ingress_address, role_listener},
        zk_controller::test_support::app_version_label,
        znode_controller::test_support::{minimal_znode, try_validate, validated_znode},
    };

    const ZNODE_YAML: &str = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperZnode
        metadata:
          name: simple-znode
        spec:
          clusterRef:
            name: simple-zookeeper
        "#;

    /// Locks the values the validate step derives from the znode itself and from the referenced
    /// cluster, including the addresses that the znode's discovery ConfigMap advertises.
    #[test]
    fn validate_ok_derives_expected_values() {
        let validated = validated_znode(&minimal_znode(ZNODE_YAML));

        assert_eq!(validated.name, "simple-znode");
        assert_eq!(validated.namespace.to_string(), "default");
        assert_eq!(
            validated.uid.to_string(),
            "e5dbf9c2-d8b0-4c1e-9f4a-1d2e3f4a5b6c"
        );
        // The product version comes from the referenced cluster, not from the znode.
        assert_eq!(
            validated.product_version.to_string(),
            app_version_label("3.9.5")
        );
        assert!(validated.zookeeper_security.tls_enabled());
        assert_eq!(
            validated.discovery_addresses.to_connection_string(),
            "node-0:2282"
        );
    }

    /// The znode's discovery ConfigMap is the only resource this controller produces, so a role
    /// Listener that publishes addresses the znode cannot use must fail validation rather than
    /// advertise nothing.
    #[test]
    fn role_listener_without_the_expected_port_fails_validation() {
        let listener = role_listener(Some(vec![ingress_address(
            "node-0",
            "not-the-zk-port",
            2181,
        )]));

        assert!(matches!(
            try_validate(&minimal_znode(ZNODE_YAML), Some(listener)),
            Err(Error::ReadRoleListenerAddresses { .. })
        ));
    }

    /// The znode controller runs on its own schedule, so it can observe the referenced cluster
    /// before the cluster controller has created the role Listener at all.
    #[test]
    fn missing_role_listener_fails_validation() {
        assert!(matches!(
            try_validate(&minimal_znode(ZNODE_YAML), None),
            Err(Error::NoRoleListenerAddresses)
        ));
    }

    /// The Listener exists, but the listener operator has not published its addresses yet.
    #[test]
    fn role_listener_without_addresses_fails_validation() {
        assert!(matches!(
            try_validate(&minimal_znode(ZNODE_YAML), Some(role_listener(None))),
            Err(Error::NoRoleListenerAddresses)
        ));
    }
}
