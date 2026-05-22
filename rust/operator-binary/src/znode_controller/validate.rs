//! The validate step in the ZookeeperZnode controller.
//!
//! Synchronously validates inputs that don't require a Kubernetes client. Produces
//! [`ValidatedInputs`], consumed by the rest of `reconcile_znode`.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
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
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Synchronous inputs the rest of `reconcile_znode` needs after dereferencing.
pub struct ValidatedInputs {
    pub resolved_product_image: ResolvedProductImage,
    pub zookeeper_security: ZookeeperSecurity,
}

/// Validates the dereferenced inputs.
pub fn validate(
    _znode: &v1alpha1::ZookeeperZnode,
    dereferenced_objects: &DereferencedObjects,
    operator_environment: &OperatorEnvironmentOptions,
) -> Result<ValidatedInputs> {
    let resolved_product_image = dereferenced_objects
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
        .resolved_authentication_classes
        .validate()
        .context(InvalidAuthenticationClassConfigurationSnafu)?;

    let zookeeper_security =
        ZookeeperSecurity::new(&dereferenced_objects.zk, resolved_authentication_classes);

    Ok(ValidatedInputs {
        resolved_product_image,
        zookeeper_security,
    })
}
