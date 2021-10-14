use std::time::Duration;

use crate::crd::ZookeeperCluster;
use kube_runtime::controller::{Context, ReconcilerAction};
use snafu::Snafu;

pub struct Ctx {}

#[derive(Snafu, Debug)]
pub enum Error {}

pub async fn reconcile_zk(
    zk: ZookeeperCluster,
    ctx: Context<Ctx>,
) -> Result<ReconcilerAction, Error> {
    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

pub fn error_policy(error: &Error, ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}
