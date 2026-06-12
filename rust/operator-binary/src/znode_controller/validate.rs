//! The validate step in the ZookeeperZnode controller.
//!
//! Synchronously validates inputs that don't require a Kubernetes client. Produces
//! [`ValidatedZnode`], consumed by the rest of `reconcile_znode`.

use std::str::FromStr;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection,
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
    crd::{CONTAINER_IMAGE_BASE_NAME, authentication, security::ZookeeperSecurity, v1alpha1},
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
    })
}
