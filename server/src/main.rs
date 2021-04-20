use stackable_operator::{client, error};
use stackable_operator::logging;
use stackable_zookeeper_crd::ZooKeeperCluster;

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    logging::initialize_logging("ZOOKEEPER_OPERATOR_LOG");
    let client = client::create_client(Some("zookeeper.stackable.tech".to_string())).await?;

    stackable_operator::crd::ensure_crd_created::<ZooKeeperCluster>(&client).await?;

    stackable_zookeeper_operator::create_controller(client).await;
    Ok(())
}
