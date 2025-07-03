//! Types and functions for exposing product endpoints via [listener::v1alpha1::Listener].

use snafu::{ResultExt as _, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder, commons::product_image_selection::ResolvedProductImage,
    crd::listener, kube::ResourceExt as _,
};

use crate::{
    crd::{ZOOKEEPER_SERVER_PORT_NAME, ZookeeperRole, security::ZookeeperSecurity, v1alpha1},
    utils::build_recommended_labels,
    zk_controller::ZK_CONTROLLER_NAME,
};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Role {zk_role:?} is not defined in the ZooKeeperCluster spec"))]
    InvalidRole {
        source: crate::crd::Error,
        zk_role: String,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build recommended labels"))]
    RecommendedLabels {
        source: stackable_operator::builder::meta::Error,
    },
}

pub fn build_role_listener(
    zk: &v1alpha1::ZookeeperCluster,
    zk_role: &ZookeeperRole,
    resolved_product_image: &ResolvedProductImage,
    zookeeper_security: &ZookeeperSecurity,
) -> Result<listener::v1alpha1::Listener> {
    let listener_name = role_listener_name(zk, zk_role);
    let listener_class = &zk
        .role(zk_role)
        .with_context(|_| InvalidRoleSnafu {
            zk_role: zk_role.to_string(),
        })?
        .role_config
        .listener_class;

    let listener = listener::v1alpha1::Listener {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(zk)
            .name(listener_name)
            .ownerreference_from_resource(zk, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            // Since we only make a listener for the role, which labels should we use?
            // We can't use with_recommended_labels because it requires an ObjectLabels which
            // in turn requires RoleGroup stuff)
            // TODO (@NickLarsenNZ): Make separate functions for with_recommended_labels with/without rolegroups
            // .with_labels(manual_labels).build()
            .with_recommended_labels(build_recommended_labels(
                zk,
                ZK_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &zk_role.to_string(),
                "global", // TODO (@NickLarsenNZ): update build_recommended_labels to have an optional role_group
            ))
            .context(RecommendedLabelsSnafu)?
            .build(),
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(listener_class.to_owned()),
            ports: Some(listener_ports(zookeeper_security)),
            ..listener::v1alpha1::ListenerSpec::default()
        },
        status: None,
    };

    Ok(listener)
}

// TODO (@NickLarsenNZ): This could be a method we can put on a Resource that takes a role_name
pub fn role_listener_name(zk: &v1alpha1::ZookeeperCluster, zk_role: &ZookeeperRole) -> String {
    // TODO (@NickLarsenNZ): Make a convention, do we use name_any() and allow empty string? or metadata.name.expect, or handle the error?
    format!("{zk}-{zk_role}", zk = zk.name_any())
}

// We only use the server port here and intentionally omit the metrics one.
fn listener_ports(zookeeper_security: &ZookeeperSecurity) -> Vec<listener::v1alpha1::ListenerPort> {
    vec![listener::v1alpha1::ListenerPort {
        name: ZOOKEEPER_SERVER_PORT_NAME.to_string(),
        port: zookeeper_security.client_port().into(),
        protocol: Some("TCP".to_string()),
    }]
}
