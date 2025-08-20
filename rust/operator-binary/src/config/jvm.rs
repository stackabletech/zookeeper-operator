use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    role_utils::{self, JavaCommonConfig, JvmArgumentOverrides, Role},
};

use crate::crd::{
    JMX_METRICS_PORT, JVM_SECURITY_PROPERTIES_FILE, LOG4J_CONFIG_FILE, LOGBACK_CONFIG_FILE,
    LoggingFramework, STACKABLE_CONFIG_DIR, STACKABLE_LOG_CONFIG_DIR,
    v1alpha1::{
        ZookeeperCluster, ZookeeperConfig, ZookeeperConfigFragment, ZookeeperServerRoleConfig,
    },
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

    #[snafu(display("failed to merge jvm argument overrides"))]
    MergeJvmArgumentOverrides { source: role_utils::Error },
}

/// All JVM arguments.
fn construct_jvm_args(
    zk: &ZookeeperCluster,
    role: &Role<ZookeeperConfigFragment, ZookeeperServerRoleConfig, JavaCommonConfig>,
    role_group: &str,
    product_version: &str,
) -> Result<Vec<String>, Error> {
    let logging_framework = zk.logging_framework(product_version);

    let jvm_args = vec![
        format!("-Djava.security.properties={STACKABLE_CONFIG_DIR}/{JVM_SECURITY_PROPERTIES_FILE}"),
        format!(
            "-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={JMX_METRICS_PORT}:/stackable/jmx/server.yaml"
        ),
        match logging_framework {
            LoggingFramework::LOG4J => {
                format!("-Dlog4j.configuration=file:{STACKABLE_LOG_CONFIG_DIR}/{LOG4J_CONFIG_FILE}")
            }
            LoggingFramework::LOGBACK => format!(
                "-Dlogback.configurationFile={STACKABLE_LOG_CONFIG_DIR}/{LOGBACK_CONFIG_FILE}"
            ),
        },
    ];

    let operator_generated = JvmArgumentOverrides::new_with_only_additions(jvm_args);
    let merged = role
        .get_merged_jvm_argument_overrides(role_group, &operator_generated)
        .context(MergeJvmArgumentOverridesSnafu)?;
    Ok(merged
        .effective_jvm_config_after_merging()
        // Sorry for the clone, that's how operator-rs is currently modelled :P
        .clone())
}

/// Arguments that go into `SERVER_JVMFLAGS`, so *not* the heap settings (which you can get using
/// [`construct_zk_server_heap_env`]).
pub fn construct_non_heap_jvm_args(
    zk: &ZookeeperCluster,
    role: &Role<ZookeeperConfigFragment, ZookeeperServerRoleConfig, JavaCommonConfig>,
    role_group: &str,
    product_version: &str,
) -> Result<String, Error> {
    let mut jvm_args = construct_jvm_args(zk, role, role_group, product_version)?;
    jvm_args.retain(|arg| !is_heap_jvm_argument(arg));

    Ok(jvm_args.join(" "))
}

/// This will be put into `ZK_SERVER_HEAP`, which is just the heap size in megabytes (*without* the `m`
/// unit prepended).
pub fn construct_zk_server_heap_env(merged_config: &ZookeeperConfig) -> Result<String, Error> {
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
    use super::*;
    use crate::crd::{
        ZookeeperRole,
        v1alpha1::{ZookeeperConfig, ZookeeperServerRoleConfig},
    };

    #[test]
    fn test_construct_jvm_arguments_defaults() {
        let input = r#"
        apiVersion: zookeeper.stackable.tech/v1alpha1
        kind: ZookeeperCluster
        metadata:
          name: simple-zookeeper
        spec:
          image:
            productVersion: "3.9.3"
          servers:
            roleGroups:
              default:
                replicas: 1
        "#;
        let (zookeeper, merged_config, role, rolegroup) = construct_boilerplate(input);
        let non_heap_jvm_args = construct_non_heap_jvm_args(
            &zookeeper,
            &role,
            &rolegroup,
            zookeeper.spec.image.product_version(),
        )
        .unwrap();
        let zk_server_heap_env = construct_zk_server_heap_env(&merged_config).unwrap();

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
            productVersion: "3.9.3"
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
        let (zookeeper, merged_config, role, rolegroup) = construct_boilerplate(input);
        let non_heap_jvm_args = construct_non_heap_jvm_args(
            &zookeeper,
            &role,
            &rolegroup,
            zookeeper.spec.image.product_version(),
        )
        .unwrap();
        let zk_server_heap_env = construct_zk_server_heap_env(&merged_config).unwrap();

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

    fn construct_boilerplate(
        zookeeper_cluster: &str,
    ) -> (
        ZookeeperCluster,
        ZookeeperConfig,
        Role<ZookeeperConfigFragment, ZookeeperServerRoleConfig, JavaCommonConfig>,
        String,
    ) {
        let zookeeper: ZookeeperCluster =
            serde_yaml::from_str(zookeeper_cluster).expect("illegal test input");

        let zookeeper_role = ZookeeperRole::Server;
        let rolegroup_ref = zookeeper.server_rolegroup_ref("default");
        let merged_config = zookeeper
            .merged_config(&zookeeper_role, &rolegroup_ref)
            .unwrap();
        let role = zookeeper.spec.servers.clone().unwrap();

        (zookeeper, merged_config, role, "default".to_owned())
    }
}
