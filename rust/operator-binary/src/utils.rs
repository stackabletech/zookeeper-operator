use crate::{APP_NAME, OPERATOR_NAME};
use futures::Future;
use pin_project::pin_project;
use stackable_operator::labels::ObjectLabels;

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

/// Creates recommended `ObjectLabels` to be used in deployed resources
pub fn build_recommended_labels<'a, T>(
    owner: &'a T,
    controller_name: &'a str,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, T> {
    ObjectLabels {
        owner,
        app_name: APP_NAME,
        app_version,
        operator_name: OPERATOR_NAME,
        controller_name,
        role,
        role_group,
    }
}
