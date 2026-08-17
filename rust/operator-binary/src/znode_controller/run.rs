//! Wires up and runs the ZookeeperZnode [`Controller`], shared by the operator (fallback) and the
//! per-cluster agent.
//!
//! Extracted from `main.rs` so both entry points build the identical controller chain. The only
//! differences between the two live in the [`Ctx`] (its [`Mode`](super::Mode)) and in what the
//! caller wraps this future with — the operator gates it behind `crd_established`, the agent does
//! not (it has no cluster-scoped CRD list/watch permission).

use std::{future::Future, sync::Arc};

use futures::StreamExt;
use stackable_operator::{
    client::Client,
    namespace::WatchNamespace,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{
        Resource,
        core::DeserializeGuard,
        runtime::{
            Controller,
            events::{Recorder, Reporter},
            reflector::ObjectRef,
            watcher,
        },
    },
    logging::controller::report_controller_reconciled,
};

use crate::{
    crd::v1alpha1,
    znode_controller::{self, Ctx, ZNODE_FULL_CONTROLLER_NAME},
};

/// Builds the ZookeeperZnode [`Controller`] and returns a future that runs it to completion.
///
/// `watch_namespace` scopes the watches: the operator passes its configured watch namespace, the
/// agent passes its own single namespace. The `.store()`-before-`.watches()` ordering is preserved
/// so the ZookeeperCluster watch can map back to the stored znodes.
pub fn run(
    client: Client,
    watch_namespace: &WatchNamespace,
    ctx: Arc<Ctx>,
    shutdown: impl Future<Output = ()> + Send + Sync + 'static,
) -> impl Future<Output = ()> {
    let znode_controller = Controller::new(
        watch_namespace.get_api::<DeserializeGuard<v1alpha1::ZookeeperZnode>>(&client),
        watcher::Config::default(),
    );
    let znode_event_recorder = Arc::new(Recorder::new(
        client.as_kube_client(),
        Reporter {
            controller: ZNODE_FULL_CONTROLLER_NAME.to_string(),
            instance: None,
        },
    ));

    let znode_store = znode_controller.store();
    znode_controller
        .owns(
            watch_namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
            watcher::Config::default(),
        )
        .watches(
            watch_namespace.get_api::<DeserializeGuard<v1alpha1::ZookeeperCluster>>(&client),
            watcher::Config::default(),
            move |zk| {
                znode_store
                    .state()
                    .into_iter()
                    .filter(move |znode| {
                        let Ok(znode) = &znode.0 else {
                            return false;
                        };
                        let zk_meta = zk.meta();
                        zk_meta.namespace == znode.spec.cluster_ref.namespace
                            && zk_meta.name == znode.spec.cluster_ref.name
                    })
                    .map(|znode| ObjectRef::from_obj(&*znode))
            },
        )
        .graceful_shutdown_on(shutdown)
        .run(
            znode_controller::reconcile_znode,
            znode_controller::error_policy,
            ctx,
        )
        // We can let the reporting happen in the background
        .for_each_concurrent(
            16, // concurrency limit
            move |result| {
                // The event_recorder needs to be shared across all invocations, so that events are
                // correctly aggregated.
                let event_recorder = znode_event_recorder.clone();
                async move {
                    report_controller_reconciled(
                        &event_recorder,
                        ZNODE_FULL_CONTROLLER_NAME,
                        &result,
                    )
                    .await;
                }
            },
        )
}
