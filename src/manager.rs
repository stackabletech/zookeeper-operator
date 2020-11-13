use crate::{Error, ZooKeeperClusterPatchFailed, Result, SerializationFailed};
use chrono::prelude::*;
use futures::{future::BoxFuture, FutureExt, StreamExt};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1beta1::CustomResourceDefinition;
use kube::{
    api::{Api, ListParams, Meta, PatchParams},
    client::Client,
    CustomResource,
};
use kube_runtime::controller::{Context, Controller, ReconcilerAction};
use prometheus::{default_registry, proto::MetricFamily, register_int_counter, IntCounter};
use serde::{Deserialize, Serialize};
use serde_json::json;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use std::sync::Arc;
use tokio::{sync::RwLock, time::Duration};
use tracing::{debug, error, info, instrument, trace, warn};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug)]
#[kube(group = "zookeeper.stackable.de", version = "v1", kind = "ZooKeeperCluster", shortname = "zk", namespaced)]
#[kube(status = "ZooKeeperClusterStatus")]
pub struct ZooKeeperClusterSpec {
    version: String
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct ZooKeeperClusterStatus {
    is_bad: bool,
}

// Context for our reconciler
#[derive(Clone)]
struct Data {
    /// kubernetes client
    client: Client,
    /// In memory state
    state: Arc<RwLock<State>>,
    /// Various prometheus metrics
    metrics: Metrics,
}

#[instrument(skip(ctx))]
async fn reconcile(zk_cluster: ZooKeeperCluster, ctx: Context<Data>) -> Result<ReconcilerAction, Error> {
    let client = ctx.get_ref().client.clone();

    ctx.get_ref().state.write().await.last_event = Utc::now();
    let name = Meta::name(&zk_cluster);
    let ns = Meta::namespace(&zk_cluster).expect("ZooKeeperCluster is namespaced");
    debug!("Reconcile ZooKeeperCluster [{}]: {:?}", name, zk_cluster);
    let zookeeper_clusters: Api<ZooKeeperCluster> = Api::namespaced(client, &ns);


    let ps = PatchParams::default(); //TODO: fix default_apply().force()
    if let Some(deletion_timestamp) = zk_cluster.metadata.deletion_timestamp {
        println!("Object being deleted {:?}", deletion_timestamp);
        if let Some(finalizers) = zk_cluster.metadata.finalizers {
            let mut finalizers: Vec<String> = finalizers;
            let index = finalizers.iter().position(|finalizer| finalizer == "zookeeper.stackable.de/check-stuff");
            if let Some(index) = index {

                // TODO: Do deletion stuff

                finalizers.swap_remove(index);
                let new_metadata = serde_json::to_vec(&json!({
                    "metadata": {
                        "finalizers": finalizers
                    }
                })).context(SerializationFailed)?;
                let _o = zookeeper_clusters
                    .patch(&name, &ps, new_metadata)
                    .await
                    .context(ZooKeeperClusterPatchFailed)?;
            }
        }
    } else {
        let mut finalizers: Vec<String> = zk_cluster.metadata.finalizers.unwrap_or_default();

        if !finalizers.contains(&"zookeeper.stackable.de/check-stuff".to_string()) {
            finalizers.push("zookeeper.stackable.de/check-stuff".to_string());

            let new_metadata = serde_json::to_vec(&json!({
                "metadata": {
                    "finalizers": finalizers
                }
            })).context(SerializationFailed)?;
            let _o = zookeeper_clusters
                .patch(&name, &ps, new_metadata)
                .await
                .context(ZooKeeperClusterPatchFailed)?;
        }
    }

    let new_status = serde_json::to_vec(&json!({
        "status": ZooKeeperClusterStatus {
            is_bad: false,
        }
    }))
        .context(SerializationFailed)?;


    let _o = zookeeper_clusters
        .patch_status(&name, &ps, new_status)
        .await
        .context(ZooKeeperClusterPatchFailed)?;

    ctx.get_ref().metrics.handled_events.inc();

    // If no events were received, check back every 30 minutes
    Ok(ReconcilerAction {
        requeue_after: Some(Duration::from_secs(3600 / 2)),
    })
}

fn error_policy(error: &Error, _ctx: Context<Data>) -> ReconcilerAction {
    warn!("reconcile failed: {}", error);
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(360)),
    }
}

/// Metrics exposed on /metrics
#[derive(Clone)]
pub struct Metrics {
    pub handled_events: IntCounter,
}

impl Metrics {
    fn new() -> Self {
        Metrics {
            handled_events: register_int_counter!("handled_events", "handled events").unwrap(),
        }
    }
}

/// In-memory reconciler state exposed on /
#[derive(Clone, Serialize)]
pub struct State {
    #[serde(deserialize_with = "from_ts")]
    pub last_event: DateTime<Utc>,
}

impl State {
    fn new() -> Self {
        State {
            last_event: Utc::now(),
        }
    }
}

/// Data owned by the Manager
#[derive(Clone)]
pub struct Manager {
    /// In memory state
    state: Arc<RwLock<State>>,
    /// Various prometheus metrics
    metrics: Metrics,
}

impl Manager {
    /// Lifecycle initialization interface for app
    ///
    /// This returns a `Manager` that drives a `Controller` + a future to be awaited
    /// It is up to `main` to wait for the controller stream.
    pub async fn new(client: Client) -> (Self, BoxFuture<'static, ()>) {
        // Check whether the ZooKeeperClusters CRD has been registered and fail if not
        let crds: Api<CustomResourceDefinition> = Api::all(client.clone());
        crds.get("zookeeperclusters.zookeeper.stackable.de").await.expect("ZooKeeperCluster CRD is missing!");

        let metrics = Metrics::new();
        let state = Arc::new(RwLock::new(State::new()));
        let context = Context::new(Data {
            client: client.clone(),
            metrics: metrics.clone(),
            state: state.clone(),
        });

        let zookeeper_clusters = Api::<ZooKeeperCluster>::all(client);

        // It does not matter what we do with the stream returned from `run` what we do need
        // to consume it, that's why we return a future.
        let drainer = Controller::new(zookeeper_clusters, ListParams::default())
            .run(reconcile, error_policy, context)
            .filter_map(|x| async move { std::result::Result::ok(x) })
            .for_each(|_| futures::future::ready(()))
            .boxed();

        (Self { state, metrics }, drainer)
    }

    /// Metrics getter
    pub fn metrics(&self) -> Vec<MetricFamily> {
        default_registry().gather()
    }

    /// State getter
    pub async fn state(&self) -> State {
        self.state.read().await.clone()
    }
}
