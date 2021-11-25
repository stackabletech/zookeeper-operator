use futures::Future;
use pin_project::pin_project;
use serde::{de::DeserializeOwned, Serialize};
use stackable_operator::kube::{
    self,
    api::{Patch, PatchParams},
    Resource,
};
use std::fmt::Debug;

/// Server-side applies an object that our controller "owns" (is the primary controller for)
///
/// Compared to [`Api::patch`], this automatically reads the kind, namespace, and name from the `object` rather than
/// requiring them to be duplicated manually.
pub async fn apply_owned<K>(kube: &kube::Client, field_manager: &str, obj: &K) -> kube::Result<K>
where
    K: Resource<DynamicType = ()> + Serialize + DeserializeOwned + Clone + Debug,
{
    let api = if let Some(ns) = &obj.meta().namespace {
        kube::Api::<K>::namespaced(kube.clone(), ns)
    } else {
        kube::Api::<K>::all(kube.clone())
    };
    api.patch(
        // Name is required, but K8s API will fail if this is not provided
        obj.meta().name.as_deref().unwrap_or(""),
        &PatchParams {
            force: true,
            field_manager: Some(field_manager.to_string()),
            ..PatchParams::default()
        },
        &Patch::Apply(obj),
    )
    .await
}

#[pin_project]
pub struct WithTokio01Executor<F, E> {
    #[pin]
    inner: F,
    executor: E,
}

impl<F: Future, E: tokio01::executor::Executor> Future for WithTokio01Executor<F, E> {
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut enter = tokio_executor::enter().unwrap();
        let this = self.project();
        tokio_executor::with_default(this.executor, &mut enter, |_| this.inner.poll(cx))
    }
}

pub trait Tokio01ExecutorExt {
    fn run_in_ctx<F: Future>(self, future: F) -> WithTokio01Executor<F, Self>
    where
        Self: Sized,
    {
        WithTokio01Executor {
            inner: future,
            executor: self,
        }
    }
}

impl<E: tokio01::executor::Executor> Tokio01ExecutorExt for E {}
