use stackable_operator::logging;
use stackable_operator::{client, error};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    logging::initialize_logging("ZOOKEEPER_OPERATOR_LOG");

    info!("Starting Stackable Operator for Apache ZooKeeper");
    let client = client::create_client(Some("zookeeper.stackable.tech".to_string())).await?;
    stackable_zookeeper_operator::create_controller(client).await;
    Ok(())
}
