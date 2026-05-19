//! The validate step in the ZookeeperCluster controller.
//!
//! Synchronously validates inputs that don't require a Kubernetes client. Produces
//! [`ValidatedInputs`], consumed by the rest of `reconcile_zk`.

use product_config::{ProductConfigManager, types::PropertyNameKind};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
    product_config_utils::{
        ValidatedRoleConfigByPropertyKind, transform_all_roles_to_config,
        validate_all_roles_and_groups_config,
    },
};

use crate::{
    crd::{
        CONTAINER_IMAGE_BASE_NAME, JVM_SECURITY_PROPERTIES_FILE, ZOOKEEPER_PROPERTIES_FILE,
        ZookeeperRole, security::ZookeeperSecurity, v1alpha1,
    },
    zk_controller::dereference::DereferencedObjects,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("object defines no server role"))]
    NoServerRole,

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Synchronous inputs the rest of `reconcile_zk` needs after dereferencing.
pub struct ValidatedInputs {
    pub resolved_product_image: ResolvedProductImage,
    pub zookeeper_security: ZookeeperSecurity,
    pub validated_role_config: ValidatedRoleConfigByPropertyKind,
}

/// Validates the cluster spec and the dereferenced inputs.
pub fn validate(
    zk: &v1alpha1::ZookeeperCluster,
    dereferenced_objects: &DereferencedObjects,
    operator_environment: &OperatorEnvironmentOptions,
    product_config: &ProductConfigManager,
) -> Result<ValidatedInputs> {
    let resolved_product_image = zk
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            &operator_environment.image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let zookeeper_security = ZookeeperSecurity::new(
        zk,
        dereferenced_objects.resolved_authentication_classes.clone(),
    );

    let validated_role_config =
        validated_product_config(zk, &resolved_product_image.product_version, product_config)?;

    Ok(ValidatedInputs {
        resolved_product_image,
        zookeeper_security,
        validated_role_config,
    })
}

fn validated_product_config(
    zk: &v1alpha1::ZookeeperCluster,
    product_version: &str,
    product_config: &ProductConfigManager,
) -> Result<ValidatedRoleConfigByPropertyKind> {
    let server_role = zk.spec.servers.clone().context(NoServerRoleSnafu)?;

    let role_config = transform_all_roles_to_config(
        zk,
        &[(
            ZookeeperRole::Server.to_string(),
            (
                vec![
                    PropertyNameKind::Env,
                    PropertyNameKind::File(ZOOKEEPER_PROPERTIES_FILE.to_string()),
                    PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
                ],
                server_role,
            ),
        )]
        .into(),
    )
    .context(GenerateProductConfigSnafu)?;

    validate_all_roles_and_groups_config(
        product_version,
        &role_config,
        product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)
}
