use stackable_zookeeper_crd::{
    ZookeeperCluster, ZookeeperConfig, CLIENT_TLS_DIR, CLIENT_TLS_MOUNT_DIR, QUORUM_TLS_DIR,
    QUORUM_TLS_MOUNT_DIR, STACKABLE_CONFIG_DIR, STACKABLE_DATA_DIR, STACKABLE_RW_CONFIG_DIR,
    ZOOKEEPER_PROPERTIES_FILE,
};

const STORE_PASSWORD_ENV: &str = "STORE_PASSWORD";

pub fn create_init_container_command_args(zk: &ZookeeperCluster) -> String {
    let mut args = vec![
        // copy config files to a writeable empty folder in order to set key and
        // truststore passwords in the init container via script
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
    ];

    // Quorum
    args.push(generate_password());
    args.extend(create_key_and_trust_store_cmd(
        QUORUM_TLS_MOUNT_DIR,
        QUORUM_TLS_DIR,
        "quorum-tls",
    ));
    args.extend(vec![
        write_store_password_to_config(ZookeeperConfig::SSL_QUORUM_KEY_STORE_PASSWORD),
        write_store_password_to_config(ZookeeperConfig::SSL_QUORUM_TRUST_STORE_PASSWORD),
    ]);

    // client-tls and client-auth-tls (only the certificates specified are accepted)
    if zk.client_tls_enabled() {
        args.push(generate_password());

        args.extend(create_key_and_trust_store_cmd(
            CLIENT_TLS_MOUNT_DIR,
            CLIENT_TLS_DIR,
            "client-tls",
        ));

        args.extend(vec![
            write_store_password_to_config(ZookeeperConfig::SSL_KEY_STORE_PASSWORD),
            write_store_password_to_config(ZookeeperConfig::SSL_TRUST_STORE_PASSWORD),
        ]);
    }

    args.push(format!(
        "expr $MYID_OFFSET + $(echo $POD_NAME | sed 's/.*-//') > {dir}/myid",
        dir = STACKABLE_DATA_DIR
    ));

    args.join(" && ")
}

/// Generates the shell script to retrieve a random 20 character password
fn generate_password() -> String {
    format!("export {STORE_PASSWORD_ENV}=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 20 ; echo '')",)
}

/// Generates the shell script to append the generated password from `generate_password()`
/// to the zoo.cfg to set key and truststore passwords.
fn write_store_password_to_config(property: &str) -> String {
    format!(
        "echo {property}=${STORE_PASSWORD_ENV} >> {rwconf}/{ZOOKEEPER_PROPERTIES_FILE}",
        property = property,
        rwconf = STACKABLE_RW_CONFIG_DIR
    )
}

/// Generates the shell script to create key and trust stores from the certificates provided
/// by the secret operator
fn create_key_and_trust_store_cmd(
    mount_directory: &str,
    stackable_directory: &str,
    alias_name: &str,
) -> Vec<String> {
    vec![
        format!("echo [{stackable_directory}] Cleaning up truststore - just in case"),
        format!("rm -f {stackable_directory}/truststore.p12"),
        format!("echo [{stackable_directory}] Creating truststore"),
        format!("keytool -importcert -file {mount_directory}/ca.crt -keystore {stackable_directory}/truststore.p12 -storetype pkcs12 -noprompt -alias {alias_name} -storepass ${STORE_PASSWORD_ENV}"),
        format!("echo [{stackable_directory}] Creating certificate chain"),
        format!("cat {mount_directory}/ca.crt {mount_directory}/tls.crt > {stackable_directory}/chain.crt"),
        format!("echo [{stackable_directory}] Creating keystore"),
        format!("openssl pkcs12 -export -in {stackable_directory}/chain.crt -inkey {mount_directory}/tls.key -out {stackable_directory}/keystore.p12 --passout pass:${STORE_PASSWORD_ENV}"),
    ]
}
