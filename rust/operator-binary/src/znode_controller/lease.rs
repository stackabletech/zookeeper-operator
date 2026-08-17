//! The agent's liveness Lease (spike step 4c).
//!
//! The agent renews a `coordination.k8s.io/v1` Lease from inside its process on a short period — the
//! same primitive kubelet uses for node heartbeats. The operator watches that Lease: staleness
//! beyond [`LEASE_DURATION_SECONDS`] means "no functioning agent", which is one signal covering every
//! cause (crashloop, OOM, unschedulable, bad image, dead node, …) rather than the brittleness
//! treadmill of inferring health from Deployment readiness.
//!
//! Honest boundary (spike): a Lease renewed by a timer proves the *process* is alive and its API
//! connection works, not that the reconcile loop is healthy. Gating renewal on controller health
//! would close that; here the timer is enough.

use std::{future::Future, time::Duration};

use stackable_operator::{
    client::Client,
    k8s_openapi::{
        api::coordination::v1::{Lease, LeaseSpec},
        apimachinery::pkg::apis::meta::v1::MicroTime,
        jiff::Timestamp,
    },
    kube::api::ObjectMeta,
};

use crate::crd::FIELD_MANAGER;

/// How long a renewal is valid. The operator treats a Lease older than this as "agent not running".
pub const LEASE_DURATION_SECONDS: i32 = 30;

/// How often the agent renews — comfortably inside [`LEASE_DURATION_SECONDS`].
const RENEW_PERIOD: Duration = Duration::from_secs(10);

/// The Lease name for a cluster's agent (shares the `<cluster>-znode-agent` naming).
pub fn agent_lease_name(cluster_name: &str) -> String {
    format!("{cluster_name}-znode-agent")
}

/// Renews the agent's liveness Lease until `shutdown` fires.
pub async fn renew_forever(
    client: Client,
    namespace: String,
    cluster_name: String,
    holder: String,
    shutdown: impl Future<Output = ()>,
) {
    let name = agent_lease_name(&cluster_name);
    tracing::info!(lease = name, namespace, "Starting agent liveness lease renewal");
    tokio::pin!(shutdown);
    loop {
        if let Err(error) = renew_once(&client, &namespace, &name, &holder).await {
            tracing::warn!(%error, lease = name, "Failed to renew the agent liveness lease");
        }
        tokio::select! {
            _ = tokio::time::sleep(RENEW_PERIOD) => {}
            _ = &mut shutdown => break,
        }
    }
    tracing::info!(lease = name, "Stopping agent liveness lease renewal");
}

async fn renew_once(
    client: &Client,
    namespace: &str,
    name: &str,
    holder: &str,
) -> Result<(), stackable_operator::client::Error> {
    let lease = Lease {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..ObjectMeta::default()
        },
        spec: Some(LeaseSpec {
            holder_identity: Some(holder.to_string()),
            lease_duration_seconds: Some(LEASE_DURATION_SECONDS),
            renew_time: Some(MicroTime(Timestamp::now())),
            ..LeaseSpec::default()
        }),
    };
    // Server-side apply create-or-updates the Lease and (re)sets `renew_time` each period.
    client.apply_patch(FIELD_MANAGER, &lease, &lease).await?;
    Ok(())
}

/// Whether the agent for `cluster_name` in `namespace` is alive: its Lease exists and was renewed
/// within [`LEASE_DURATION_SECONDS`].
pub async fn is_agent_alive(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
) -> Result<bool, stackable_operator::client::Error> {
    let name = agent_lease_name(cluster_name);
    let Some(lease) = client.get_opt::<Lease>(&name, namespace).await? else {
        return Ok(false);
    };
    let fresh = lease
        .spec
        .and_then(|spec| spec.renew_time)
        .is_some_and(|renew| {
            let age_seconds = Timestamp::now().as_second() - renew.0.as_second();
            age_seconds <= LEASE_DURATION_SECONDS as i64
        });
    Ok(fresh)
}
