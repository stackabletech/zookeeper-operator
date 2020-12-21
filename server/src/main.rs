use stackable_operator::error;
use stackable_zookeeper_crd::ZooKeeperCluster;
use stackable_zookeeper_operator::create_controller;

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::initialize_logging("ZOOKEEPER_OPERATOR_LOG");
    let client =
        stackable_operator::create_client(Some("zookeeper.stackable.de".to_string())).await?;

    stackable_operator::crd::ensure_crd_created::<ZooKeeperCluster>(client.clone()).await?;

    create_controller(client).await;
    Ok(())
}
