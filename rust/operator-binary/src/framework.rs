//! Vendored framework code shared between Stackable operators.
//!
//! This module mirrors `stackable_operator::v2` (currently only available on the
//! `smooth-operator` branch of operator-rs). It is vendored here so the
//! zookeeper-operator can act as a second consumer that validates whether the
//! abstractions generalize, before they are promoted into operator-rs proper.
//! Once the upstream `v2` API has stabilized, these modules should be replaced
//! by direct usage of `stackable_operator::v2`.

pub mod role_utils;
