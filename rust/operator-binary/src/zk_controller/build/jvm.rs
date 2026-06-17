use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::memory::{BinaryMultiple, MemoryQuantity};

use super::properties::ConfigFileName;
use crate::{
    crd::{JMX_METRICS_PORT, STACKABLE_CONFIG_DIR, STACKABLE_LOG_CONFIG_DIR},
    zk_controller::validate::{ValidatedZookeeperConfig, ZookeeperRoleGroupConfig},
};

const JAVA_HEAP_FACTOR: f32 = 0.8;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("invalid memory resource configuration - missing default or value in crd?"))]
    MissingMemoryResourceConfig,

    #[snafu(display("invalid memory config"))]
    InvalidMemoryConfig {
        source: stackable_operator::memory::Error,
    },
}

/// All JVM arguments.
fn construct_jvm_args(rolegroup_config: &ZookeeperRoleGroupConfig) -> Vec<String> {
    let jvm_args = vec![
        format!(
            "-Djava.security.properties={STACKABLE_CONFIG_DIR}/{}",
            ConfigFileName::SecurityProperties
        ),
        format!(
            "-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={JMX_METRICS_PORT}:/stackable/jmx/server.yaml"
        ),
        format!(
            "-Dlogback.configurationFile={STACKABLE_LOG_CONFIG_DIR}/{config_file}",
            config_file = ConfigFileName::LogbackXml
        ),
    ];

    // Apply the already-merged (role + role group) JVM argument overrides on top of the
    // operator-generated base arguments.
    rolegroup_config
        .product_specific_common_config
        .jvm_argument_overrides
        .apply_to(jvm_args)
}

/// Arguments that go into `SERVER_JVMFLAGS`, so *not* the heap settings (which you can get using
/// [`construct_zk_server_heap_env`]).
pub fn construct_non_heap_jvm_args(rolegroup_config: &ZookeeperRoleGroupConfig) -> String {
    let mut jvm_args = construct_jvm_args(rolegroup_config);
    jvm_args.retain(|arg| !is_heap_jvm_argument(arg));

    jvm_args.join(" ")
}

/// This will be put into `ZK_SERVER_HEAP`, which is just the heap size in megabytes (*without* the `m`
/// unit prepended).
pub fn construct_zk_server_heap_env(
    merged_config: &ValidatedZookeeperConfig,
) -> Result<String, Error> {
    let heap_size_in_mb = (MemoryQuantity::try_from(
        merged_config
            .resources
            .memory
            .limit
            .as_ref()
            .context(MissingMemoryResourceConfigSnafu)?,
    )
    .context(InvalidMemoryConfigSnafu)?
        * JAVA_HEAP_FACTOR)
        .scale_to(BinaryMultiple::Mebi);

    Ok((heap_size_in_mb.value.floor() as u32).to_string())
}

fn is_heap_jvm_argument(jvm_argument: &str) -> bool {
    let lowercase = jvm_argument.to_lowercase();

    lowercase.starts_with("-xms") || lowercase.starts_with("-xmx")
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use stackable_operator::v2::types::operator::RoleGroupName;

    use super::*;
    use crate::{
        crd::{ZookeeperRole, v1alpha1::ZookeeperCluster},
        zk_controller::test_support::{minimal_zk, validated_cluster},
    };

    /// The validated, merged config for the `default` server role group.
    fn server_default(zk: &ZookeeperCluster) -> ZookeeperRoleGroupConfig {
        let default_group = RoleGroupName::from_str("default").expect("valid role group name");
        validated_cluster(zk)
            .role_group_configs
            .get(&ZookeeperRole::Server)
            .and_then(|groups| groups.get(&default_group))
            .expect("server default role group should exist")
            .clone()
    }

    #[test]
    fn test_construct_jvm_arguments_defaults() {
        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.5"
          servers:
            roleGroups:
              default:
                replicas: 1
        "#;
        let zk = minimal_zk(input);
        let rg = server_default(&zk);
        let non_heap_jvm_args = construct_non_heap_jvm_args(&rg);
        let zk_server_heap_env =
            construct_zk_server_heap_env(&rg.config).expect("test: function must pass");

        assert_eq!(
            non_heap_jvm_args,
            "-Djava.security.properties=/stackable/config/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=9505:/stackable/jmx/server.yaml \
            -Dlogback.configurationFile=/stackable/log_config/logback.xml"
        );
        assert_eq!(zk_server_heap_env, "409");
    }

    #[test]
    fn test_construct_jvm_argument_overrides() {
        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.5"
          servers:
            config:
              resources:
                memory:
                  limit: 42Gi
            jvmArgumentOverrides:
              add:
                - -Dhttps.proxyHost=proxy.my.corp
                - -Dhttps.proxyPort=8080
                - -Djava.net.preferIPv4Stack=true
            roleGroups:
              default:
                replicas: 1
                jvmArgumentOverrides:
                  # We need more memory!
                  removeRegex:
                    - -Xmx.*
                    - -Dhttps.proxyPort=.*
                  add:
                    - -Xmx40000m
                    - -Dhttps.proxyPort=1234
        "#;
        let zk = minimal_zk(input);
        let rg = server_default(&zk);
        let non_heap_jvm_args = construct_non_heap_jvm_args(&rg);
        let zk_server_heap_env =
            construct_zk_server_heap_env(&rg.config).expect("test: function must pass");

        assert_eq!(
            non_heap_jvm_args,
            "-Djava.security.properties=/stackable/config/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=9505:/stackable/jmx/server.yaml \
            -Dlogback.configurationFile=/stackable/log_config/logback.xml \
            -Dhttps.proxyHost=proxy.my.corp \
            -Djava.net.preferIPv4Stack=true \
            -Dhttps.proxyPort=1234"
        );
        assert_eq!(zk_server_heap_env, "34406");
    }
}
