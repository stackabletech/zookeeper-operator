use stackable_zookeeper_crd::{STACKABLE_CONFIG_DIR, STACKABLE_DATA_DIR, STACKABLE_RW_CONFIG_DIR};

pub fn create_init_container_command_args() -> String {
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

    args.push(format!(
        "expr $MYID_OFFSET + $(echo $POD_NAME | sed 's/.*-//') > {dir}/myid",
        dir = STACKABLE_DATA_DIR
    ));

    args.join(" && ")
}
