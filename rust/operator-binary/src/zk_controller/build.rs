//! Build steps for the ZookeeperCluster controller.
//!
//! Each submodule turns the [`ValidatedCluster`](super::validate::ValidatedCluster)
//! into a Kubernetes resource. The [`v1alpha1::ZookeeperCluster`](crate::crd::v1alpha1::ZookeeperCluster)
//! is only used for the owner reference and object metadata, never for
//! configuration.

pub mod config_map;
pub mod discovery;
pub mod properties;
