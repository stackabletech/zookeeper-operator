use std::time::Duration;

use crate::crd::ZookeeperCluster;
use k8s_openapi::{
    api::apps::v1::{StatefulSet, StatefulSetSpec},
    apimachinery::pkg::apis::meta::v1::OwnerReference,
};
use kube::api::{ObjectMeta, Patch, PatchParams};
use kube_runtime::controller::{Context, ReconcilerAction};
use snafu::Snafu;
use stackable_operator::builder::OwnerReferenceBuilder;

pub struct Ctx {
    kube: kube::Client,
}

#[derive(Snafu, Debug)]
pub enum Error {}

pub async fn reconcile_zk(
    zk: ZookeeperCluster,
    ctx: Context<Ctx>,
) -> Result<ReconcilerAction, Error> {
    let stses = kube::Api::<StatefulSet>::namespaced(
        ctx.get_ref().kube.clone(),
        zk.metadata.namespace.as_deref().unwrap(),
    );
    for (role_group_name, role_group) in &zk.spec.servers.role_groups {
        let name = format!(
            "{}-{}",
            zk.metadata.name.as_deref().unwrap(),
            role_group_name
        );
        stses.patch(
            &name,
            &PatchParams {
                force: true,
                field_manager: Some("zookeeper.stackable.tech/zookeepercluster".to_string()),
                ..PatchParams::default()
            },
            &Patch::Apply(StatefulSet {
                metadata: ObjectMeta {
                    name: Some(name),
                    owner_references: Some(vec![OwnerReferenceBuilder::new()
                        .initialize_from_resource(&zk)
                        .controller(true)
                        .build()
                        .unwrap()]),
                    ..ObjectMeta::default()
                },
                spec: Some(StatefulSetSpec {
                    pod_management_policy: Some("Parallel".to_string()),
                    replicas: role_group.replicas.map(Into::into),
                    selector: todo!(),
                    service_name: todo!(),
                    template: todo!(),
                    update_strategy: todo!(),
                    volume_claim_templates: todo!(),
                    ..StatefulSetSpec::default()
                }),
                status: None,
            }),
        );
    }

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

pub fn error_policy(error: &Error, ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}
