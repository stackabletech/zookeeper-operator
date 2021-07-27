use stackable_operator::crd::CustomResourceExt;
use stackable_operator::logging;
use stackable_operator::{client, error};
use stackable_zookeeper_crd::ZookeeperCluster;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    logging::initialize_logging("ZOOKEEPER_OPERATOR_LOG");

    info!("Starting Stackable Operator for Apache ZooKeeper");
    let client = client::create_client(Some("zookeeper.stackable.tech".to_string())).await?;

    if let Err(error) = stackable_operator::crd::wait_until_crds_present(
        &client,
        vec![&ZookeeperCluster::crd_name()],
        None,
    )
    .await
    {
        error!("Required CRDs missing, aborting: {:?}", error);
        return Err(error);
    };

    stackable_zookeeper_operator::create_controller(client).await?;
    Ok(())
}
