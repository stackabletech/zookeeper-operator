use stackable_zookeeper_crd::{
    ZookeeperCluster, CLIENT_TLS_DIR, QUORUM_TLS_DIR, STACKABLE_DATA_DIR, TLS_STORE_SECRET,
};

pub fn create_init_container_command_args(zk: &ZookeeperCluster) -> String {
    let mut args = vec![];
    // TODO: we need to check if another secret / authentication class is used
    if zk.is_quorum_secure() {
        args.extend(create_key_and_trust_store_cmd(
            QUORUM_TLS_DIR,
            TLS_STORE_SECRET,
        ));
    }

    if zk.is_client_secure() {
        args.extend(create_key_and_trust_store_cmd(
            CLIENT_TLS_DIR,
            TLS_STORE_SECRET,
        ));
    }

    args.extend([
        format!("chown stackable:stackable {dir}", dir = STACKABLE_DATA_DIR),
        format!("chmod a=,u=rwX {dir}", dir = STACKABLE_DATA_DIR),
        format!(
            "expr $MYID_OFFSET + $(echo $POD_NAME | sed 's/.*-//') > {dir}/myid",
            dir = STACKABLE_DATA_DIR
        ),
    ]);

    args.join(" && ")
}

fn create_key_and_trust_store_cmd(directory: &str, password: &str) -> Vec<String> {
    vec![
        format!("echo [{dir}] Storing password", dir = directory),
        format!("echo {pw} > {dir}/password", pw = password, dir = directory),
        format!("echo [{dir}] Cleaning up truststore - just in case", dir = directory),
        format!("rm -f {dir}/truststore.p12",  dir = directory),
        format!("echo [{dir}] Creating truststore", dir = directory),
        format!("keytool -importcert -file {dir}/ca.crt -keystore {dir}/truststore.p12 -storetype pkcs12 -noprompt -alias ca_cert -storepass {pw}", pw = password, dir = directory),
        format!("echo [{dir}] Creating certificate chain", dir = directory),
        format!("cat {dir}/ca.crt {dir}/tls.crt > {dir}/chain.crt", dir = directory),
        format!("echo [{dir}] Creating keystore", dir = directory),
        format!("openssl pkcs12 -export -in {dir}/chain.crt -inkey {dir}/tls.key -out {dir}/keystore.p12 --passout file:{dir}/password",
                 dir = directory),
        format!("echo [{dir}] Cleaning up password", dir = directory),
        format!("rm -f {dir}/password", dir = directory),
        format!("echo [{dir}] Chowning store directory", dir = directory),
        format!("chown -R stackable:stackable {dir}", dir = directory),
        format!("echo [{dir}] Chmodding store directory", dir = directory),
        format!("chmod -R a=,u=rwX {dir}", dir = directory),
    ]
}
