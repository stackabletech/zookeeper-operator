use stackable_operator::builder::{ContainerBuilder, PodBuilder};
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperClusterSpec};
use std::collections::BTreeMap;

pub fn xyz(zk: &ZookeeperCluster, cb: &mut ContainerBuilder, pb: &mut PodBuilder) {
    let spec: &ZookeeperClusterSpec = &zk.spec;

    //spec.config.map(|c| c.tls_config.map(|t| t.tls))
}

pub fn quorum_auth_config_for_cm(
    product_config: &mut BTreeMap<String, String>,
    directory: &str,
    password: &str,
) {
    product_config.extend(quorum_auth_properties(directory, password));
}

pub fn client_auth_config_for_cm(
    product_config: &mut BTreeMap<String, String>,
    zk: &ZookeeperCluster,
    directory: &str,
    password: &str,
) {
    // Remove clientPort in favor for secureClientPort
    product_config.remove("clientPort");
    product_config.extend(client_auth_properties(zk, directory, password));
}

pub fn create_key_and_trust_store_cmd(directory: &str, password: &str) -> Vec<String> {
    vec![
        "echo Storing password".to_string(),
        format!("echo {pw} > {dir}/password", pw = password, dir = directory),
        "echo Cleaning up truststore - just in case".to_string(),
        format!("rm -f {dir}/truststore.p12",  dir = directory),
        "echo Creating truststore".to_string(),
        format!("keytool -importcert -file {dir}/ca.crt -keystore {dir}/truststore.p12 -storetype pkcs12 -noprompt -alias ca_cert -storepass {pw}", pw = password, dir = directory),
        "echo Creating keystore".to_string(),
        format!("openssl pkcs12 -export -in {dir}/chain.crt -inkey {dir}/tls.key -out {dir}/keystore.p12 --passout file:{dir}/password",
                 dir = directory),
        "echo Cleaning up password".to_string(),
        format!("rm -f {dir}/password", dir = directory),
        "echo chowning store directory".to_string(),
        format!("chown -R stackable:stackable {dir}", dir = directory),
        "echo chmodding store directory".to_string(),
        format!("chmod -R a=,u=rwX {dir}", dir = directory),
    ]
}

fn quorum_auth_properties(directory: &str, password: &str) -> BTreeMap<String, String> {
    vec![
        ("sslQuorum".to_string(), "true".to_string()),
        (
            "serverCnxnFactory".to_string(),
            "org.apache.zookeeper.server.NettyServerCnxnFactory".to_string(),
        ),
        (
            "ssl.quorum.keyStore.location".to_string(),
            format!("{dir}/keystore.p12", dir = directory),
        ),
        (
            "ssl.quorum.keyStore.password".to_string(),
            password.to_string(),
        ),
        (
            "ssl.quorum.trustStore.location".to_string(),
            format!("{dir}/truststore.p12", dir = directory),
        ),
        (
            "ssl.quorum.trustStore.password".to_string(),
            password.to_string(),
        ),
        (
            "ssl.quorum.hostnameVerification".to_string(),
            "true".to_string(),
        ),
        (
            "authProvider.x509".to_string(),
            "org.apache.zookeeper.server.auth.X509AuthenticationProvider".to_string(),
        ),
    ]
    .into_iter()
    .collect::<BTreeMap<String, String>>()
}

fn client_auth_properties(
    zk: &ZookeeperCluster,
    directory: &str,
    password: &str,
) -> BTreeMap<String, String> {
    vec![
        ("secureClientPort".to_string(), zk.client_port().to_string()),
        (
            "serverCnxnFactory".to_string(),
            "org.apache.zookeeper.server.NettyServerCnxnFactory".to_string(),
        ),
        (
            "ssl.keyStore.location".to_string(),
            format!("{dir}/keystore.p12", dir = directory),
        ),
        ("ssl.keyStore.password".to_string(), password.to_string()),
        (
            "ssl.trustStore.location".to_string(),
            format!("{dir}/truststore.p12", dir = directory),
        ),
        ("ssl.trustStore.password".to_string(), password.to_string()),
        (
            "authProvider.x509".to_string(),
            "org.apache.zookeeper.server.auth.X509AuthenticationProvider".to_string(),
        ),
    ]
    .into_iter()
    .collect::<BTreeMap<String, String>>()
}
