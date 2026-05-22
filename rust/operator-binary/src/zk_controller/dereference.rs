//! The dereference step in the ZookeeperCluster controller.
//!
//! Fetches all Kubernetes objects referenced by the [`v1alpha1::ZookeeperCluster`] spec and
//! returns them in [`DereferencedObjects`]. Synchronous validation of the fetched objects
//! (image resolution, product-config validation, security struct assembly) happens in the
//! validate step.

use snafu::{ResultExt, Snafu};
use stackable_operator::client::Client;

use crate::crd::{
    authentication::{self, ResolvedAuthenticationClasses},
    v1alpha1,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to fetch authentication classes"))]
    FetchAuthenticationClasses { source: authentication::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Kubernetes objects referenced from the [`v1alpha1::ZookeeperCluster`] spec, already fetched but
/// not yet validated.
pub struct DereferencedObjects {
    pub authentication_classes: ResolvedAuthenticationClasses,
}

/// Fetches all Kubernetes objects referenced from the [`v1alpha1::ZookeeperCluster`] spec.
pub async fn dereference(
    client: &Client,
    zk: &v1alpha1::ZookeeperCluster,
) -> Result<DereferencedObjects> {
    let authentication_classes = ResolvedAuthenticationClasses::fetch_references(
        client,
        &zk.spec.cluster_config.authentication,
    )
    .await
    .context(FetchAuthenticationClassesSnafu)?;

    Ok(DereferencedObjects {
        authentication_classes,
    })
}
