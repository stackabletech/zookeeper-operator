//! Build steps for the ZookeeperCluster controller.
//!
//! Each submodule turns the [`ValidatedCluster`](super::validate::ValidatedCluster)
//! into a Kubernetes resource. The [`v1alpha1::ZookeeperCluster`](crate::crd::v1alpha1::ZookeeperCluster)
//! is only used for the owner reference and object metadata, never for
//! configuration.
//!
//! Builders that emit a complete cluster resource live under [`resource`]; the
//! remaining submodules ([`command`], [`graceful_shutdown`], [`jvm`],
//! [`properties`]) produce fragments that those resource builders assemble.

use std::str::FromStr;

use stackable_operator::v2::types::operator::{ProductVersion, RoleGroupName};

// Placeholder role-group name used for the recommended labels of the role-level discovery
// `ConfigMap` (which is not tied to a single role group).
stackable_operator::constant!(pub(crate) PLACEHOLDER_DISCOVERY_ROLE_GROUP: RoleGroupName = "discovery");

// Placeholder role-group name used for the recommended labels of the role-level `Listener`
// (which is not tied to a single role group).
stackable_operator::constant!(pub(crate) PLACEHOLDER_LISTENER_ROLE_GROUP: RoleGroupName = "none");

// Placeholder product version used for labels on PVC templates, which cannot be modified once
// deployed. A constant value keeps the labels stable across version upgrades.
stackable_operator::constant!(pub(crate) UNVERSIONED_PRODUCT_VERSION: ProductVersion = "none");

pub mod command;
pub mod graceful_shutdown;
pub mod jvm;
pub mod properties;
pub mod resource;
