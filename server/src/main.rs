mod error;

use stackable_zookeeper_operator::create_controller;

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    initialize_logging();
    let client = create_client().await?;

    stackable_zookeeper_crd::ensure_crd_created(client.clone()).await;

    create_controller(client).await;
    Ok(())
}

fn initialize_logging() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
}

async fn create_client() -> Result<kube::Client, error::Error> {
    return Ok(kube::Client::try_default().await?);
}
