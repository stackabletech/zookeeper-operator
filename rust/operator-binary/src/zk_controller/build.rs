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

pub mod command;
pub mod graceful_shutdown;
pub mod jvm;
pub mod properties;
pub mod resource;
