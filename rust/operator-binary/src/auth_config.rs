use stackable_zookeeper_crd::{
    ZookeeperCluster, ZookeeperConfig, CLIENT_TLS_DIR, QUORUM_TLS_DIR, STACKABLE_CONFIG_DIR,
    STACKABLE_DATA_DIR, STACKABLE_RW_CONFIG_DIR,
};

const STORE_PASSWORD_ENV: &str = "STORE_PASSWORD";

pub fn create_init_container_command_args(zk: &ZookeeperCluster) -> String {
    let mut args = vec![];

    // copy config files to a writeable empty folder
    // in order to set key and truststore pws later
    args.extend(vec![
        format!(
            "echo copying {conf} to {rw_conf}",
            conf = STACKABLE_CONFIG_DIR,
            rw_conf = STACKABLE_RW_CONFIG_DIR
        ),
        format!(
            "cp -RL {conf}/* {rw_conf}",
            conf = STACKABLE_CONFIG_DIR,
            rw_conf = STACKABLE_RW_CONFIG_DIR
        ),
    ]);

    args.push(generate_password());
    args.extend(create_key_and_trust_store_cmd(QUORUM_TLS_DIR));
    args.extend(vec![
        write_store_password_to_config(ZookeeperConfig::SSL_QUORUM_KEY_STORE_PASSWORD),
        write_store_password_to_config(ZookeeperConfig::SSL_QUORUM_TRUST_STORE_PASSWORD),
    ]);

    if zk.is_client_secure() {
        args.push(generate_password());
        args.extend(create_key_and_trust_store_cmd(CLIENT_TLS_DIR));

        args.extend(vec![
            write_store_password_to_config(ZookeeperConfig::SSL_KEY_STORE_PASSWORD),
            write_store_password_to_config(ZookeeperConfig::SSL_TRUST_STORE_PASSWORD),
        ]);
    }

    args.extend([
        format!("chown stackable:stackable {dir}", dir = STACKABLE_DATA_DIR),
        format!("chmod a=,u=rwX {dir}", dir = STACKABLE_DATA_DIR),
        format!(
            "chown -R stackable:stackable {rwconf_directory}",
            rwconf_directory = STACKABLE_RW_CONFIG_DIR
        ),
        format!(
            "chmod -R a=,u=rwX {rwconf_directory}",
            rwconf_directory = STACKABLE_RW_CONFIG_DIR
        ),
        format!(
            "expr $MYID_OFFSET + $(echo $POD_NAME | sed 's/.*-//') > {dir}/myid",
            dir = STACKABLE_DATA_DIR
        ),
    ]);

    args.join(" && ")
}

fn generate_password() -> String {
    format!("export {STORE_PASSWORD_ENV}=$(echo $RANDOM | md5sum | head -c 20)",)
}

fn write_store_password_to_config(property: &str) -> String {
    format!(
        "echo {property}=${STORE_PASSWORD_ENV} >> {rwconf}/zoo.cfg",
        property = property,
        rwconf = STACKABLE_RW_CONFIG_DIR
    )
}

fn create_key_and_trust_store_cmd(directory: &str) -> Vec<String> {
    vec![
        format!("echo [{dir}] Storing password", dir = directory),
        format!("echo ${STORE_PASSWORD_ENV} > {dir}/password", dir = directory),
        format!("echo [{dir}] Cleaning up truststore - just in case", dir = directory),
        format!("rm -f {dir}/truststore.p12",  dir = directory),
        format!("echo [{dir}] Creating truststore", dir = directory),
        format!("keytool -importcert -file {dir}/ca.crt -keystore {dir}/truststore.p12 -storetype pkcs12 -noprompt -alias ca_cert -storepass ${STORE_PASSWORD_ENV}", dir = directory),
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
