//! Ensures that `Pod`s are configured and running for each [`ZookeeperCluster`]

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    hash::Hasher,
    sync::Arc,
    time::Duration,
};

use crate::{
    discovery::{self, build_discovery_configmaps},
    APP_NAME, APP_PORT,
};
use fnv::FnvHasher;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder},
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, EmptyDirVolumeSource, EnvVar, EnvVarSource,
                ExecAction, ObjectFieldSelector, PersistentVolumeClaim, PersistentVolumeClaimSpec,
                PodSecurityContext, Probe, ResourceRequirements, SecurityContext, Service,
                ServicePort, ServiceSpec, Volume,
            },
        },
        apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
    },
    kube::{
        api::ObjectMeta,
        runtime::controller::{Context, ReconcilerAction},
    },
    labels::{role_group_selector_labels, role_selector_labels},
    logging::controller::ReconcilerError,
    product_config::{
        types::PropertyNameKind, writer::to_java_properties_string, ProductConfigManager,
    },
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::RoleGroupRef,
};
use stackable_zookeeper_crd::{ZookeeperCluster, ZookeeperClusterStatus, ZookeeperRole};
use strum::{EnumDiscriminants, IntoStaticStr};

const FIELD_MANAGER_SCOPE: &str = "zookeepercluster";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,
    #[snafu(display("object defines no server role"))]
    NoServerRole,
    #[snafu(display("failed to calculate global service name"))]
    GlobalServiceNameNotFound,
    #[snafu(display("failed to calculate service name for role {}", rolegroup))]
    RoleGroupServiceNameNotFound {
        rolegroup: RoleGroupRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to apply ConfigMap for {}", rolegroup))]
    ApplyRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to serialize zoo.cfg for {}", rolegroup))]
    SerializeZooCfg {
        source: stackable_operator::product_config::writer::PropertiesWriterError,
        rolegroup: RoleGroupRef<ZookeeperCluster>,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig { source: discovery::Error },
    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

const PROPERTIES_FILE: &str = "zoo.cfg";

pub async fn reconcile_zk(
    zk: Arc<ZookeeperCluster>,
    ctx: Context<Ctx>,
) -> Result<ReconcilerAction> {
    tracing::info!("Starting reconcile");
    let client = &ctx.get_ref().client;

    let validated_config = validate_all_roles_and_groups_config(
        zk_version(&zk)?,
        &transform_all_roles_to_config(
            &*zk,
            [(
                ZookeeperRole::Server.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::File(PROPERTIES_FILE.to_string()),
                    ],
                    zk.spec.servers.clone().context(NoServerRoleSnafu)?,
                ),
            )]
            .into(),
        )
        .context(GenerateProductConfigSnafu)?,
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;
    let role_server_config = validated_config
        .get(&ZookeeperRole::Server.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let server_role_service = build_server_role_service(&zk)?;
    let server_role_service = client
        .apply_patch(
            FIELD_MANAGER_SCOPE,
            &server_role_service,
            &server_role_service,
        )
        .await
        .context(ApplyRoleServiceSnafu)?;
    for (rolegroup_name, rolegroup_config) in role_server_config.iter() {
        let rolegroup = zk.server_rolegroup_ref(rolegroup_name);

        let rg_service = build_server_rolegroup_service(&rolegroup, &zk)?;
        let rg_configmap = build_server_rolegroup_config_map(&rolegroup, &zk, rolegroup_config)?;
        let rg_statefulset = build_server_rolegroup_statefulset(&rolegroup, &zk, rolegroup_config)?;
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_service, &rg_service)
            .await
            .with_context(|_| ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup.clone(),
            })?;
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_configmap, &rg_configmap)
            .await
            .with_context(|_| ApplyRoleGroupConfigSnafu {
                rolegroup: rolegroup.clone(),
            })?;
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_statefulset, &rg_statefulset)
            .await
            .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                rolegroup: rolegroup.clone(),
            })?;
    }

    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    let mut discovery_hash = FnvHasher::with_key(0);
    for discovery_cm in build_discovery_configmaps(client, &*zk, &zk, &server_role_service, None)
        .await
        .context(BuildDiscoveryConfigSnafu)?
    {
        let discovery_cm = client
            .apply_patch(FIELD_MANAGER_SCOPE, &discovery_cm, &discovery_cm)
            .await
            .context(ApplyDiscoveryConfigSnafu)?;
        if let Some(generation) = discovery_cm.metadata.resource_version {
            discovery_hash.write(generation.as_bytes())
        }
    }

    let status = ZookeeperClusterStatus {
        // Serialize as a string to discourage users from trying to parse the value,
        // and to keep things flexible if we end up changing the hasher at some point.
        discovery_hash: Some(discovery_hash.finish().to_string()),
    };
    client
        .apply_patch_status(FIELD_MANAGER_SCOPE, &*zk, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
///
/// Note that you should generally *not* hard-code clients to use these services; instead, create a [`ZookeeperZnode`](`stackable_zookeeper_crd::ZookeeperZnode`)
/// and use the connection string that it gives you.
pub fn build_server_role_service(zk: &ZookeeperCluster) -> Result<Service> {
    let role_name = ZookeeperRole::Server.to_string();
    let role_svc_name = zk
        .server_role_service_name()
        .context(GlobalServiceNameNotFoundSnafu)?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(zk)
            .name(&role_svc_name)
            .ownerreference_from_resource(zk, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(zk, APP_NAME, zk_version(zk)?, &role_name, "global")
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("zk".to_string()),
                port: APP_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
            selector: Some(role_selector_labels(zk, APP_NAME, &role_name)),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_server_rolegroup_config_map(
    rolegroup: &RoleGroupRef<ZookeeperCluster>,
    zk: &ZookeeperCluster,
    server_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<ConfigMap> {
    let mut zoo_cfg = server_config
        .get(&PropertyNameKind::File(PROPERTIES_FILE.to_string()))
        .cloned()
        .unwrap_or_default();
    zoo_cfg.extend(zk.pods().into_iter().flatten().map(|pod| {
        (
            format!("server.{}", pod.zookeeper_myid),
            format!("{}:2888:3888;{}", pod.fqdn(), APP_PORT),
        )
    }));
    let zoo_cfg = zoo_cfg
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect::<Vec<_>>();
    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(zk)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(zk, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(
                    zk,
                    APP_NAME,
                    zk_version(zk)?,
                    &rolegroup.role,
                    &rolegroup.role_group,
                )
                .build(),
        )
        .add_data(
            "zoo.cfg",
            to_java_properties_string(zoo_cfg.iter().map(|(k, v)| (k, v))).with_context(|_| {
                SerializeZooCfgSnafu {
                    rolegroup: rolegroup.clone(),
                }
            })?,
        )
        .add_data(
            "log4j.properties",
            r#"
# Copyright 2012 The Apache Software Foundation
# 
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Define some default values that can be overridden by system properties
zookeeper.root.logger=INFO, VECTOR
#zookeeper.root.logger=INFO, CONSOLE, VECTOR
#zookeeper.root.logger=INFO, CONSOLE

zookeeper.console.threshold=INFO

zookeeper.log.dir=.
zookeeper.log.file=zookeeper.log
zookeeper.log.threshold=INFO
zookeeper.log.maxfilesize=256MB
zookeeper.log.maxbackupindex=20

zookeeper.tracelog.dir=${zookeeper.log.dir}
zookeeper.tracelog.file=zookeeper_trace.log

#log4j.rootLogger=${zookeeper.root.logger}
log4j.rootLogger=INFO, VECTOR

#
# console
# Add "console" to rootlogger above if you want to use this 
#
log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
log4j.appender.CONSOLE.Threshold=${zookeeper.console.threshold}
log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n

#
# Add ROLLINGFILE to rootLogger to get log file output
#
log4j.appender.ROLLINGFILE=org.apache.log4j.RollingFileAppender
log4j.appender.ROLLINGFILE.Threshold=${zookeeper.log.threshold}
log4j.appender.ROLLINGFILE.File=${zookeeper.log.dir}/${zookeeper.log.file}
log4j.appender.ROLLINGFILE.MaxFileSize=${zookeeper.log.maxfilesize}
log4j.appender.ROLLINGFILE.MaxBackupIndex=${zookeeper.log.maxbackupindex}
log4j.appender.ROLLINGFILE.layout=org.apache.log4j.PatternLayout
log4j.appender.ROLLINGFILE.layout.ConversionPattern=%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n

#
# Add TRACEFILE to rootLogger to get log file output
#    Log TRACE level and above messages to a log file
#
log4j.appender.TRACEFILE=org.apache.log4j.FileAppender
log4j.appender.TRACEFILE.Threshold=TRACE
log4j.appender.TRACEFILE.File=${zookeeper.tracelog.dir}/${zookeeper.tracelog.file}

log4j.appender.TRACEFILE.layout=org.apache.log4j.PatternLayout
### Notice we are including log4j's NDC here (%x)
log4j.appender.TRACEFILE.layout.ConversionPattern=%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L][%x] - %m%n

log4j.appender.VECTOR=org.apache.log4j.RollingFileAppender
log4j.appender.VECTOR.File=/stackable/logs/zoo.log
log4j.appender.VECTOR.layout=org.apache.log4j.xml.XMLLayout
# log4j.appender.VECTOR.layout=org.apache.log4j.PatternLayout
# log4j.appender.VECTOR.layout.ConversionPattern=%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n
# log4j.appender.VECTOR=org.apache.log4j.net.TelnetAppender
# log4j.appender.VECTOR.host=127.0.0.1
# log4j.appender.VECTOR.port=9997
# log4j.appender.VECTOR.layout=org.apache.log4j.JsonLayout
# log4j.appender.VECTOR.layout.compact=true
# log4j.appender.VECTOR=org.apache.log4j.SocketAppender
# log4j.appender.VECTOR.host=127.0.0.1
# log4j.appender.VECTOR.port=9997
# log4j.appender.VECTOR.layout=org.apache.log4j.JsonLayout
# log4j.appender.VECTOR.layout.compact=true
"#,
        )
        .add_data(
            "vector.toml",
            r#"
# [sources.in]
# type = "stdin"

# [sources.in]
# type = "socket"
# address = "127.0.0.1:9997"
# mode = "tcp"
# framing = { method = "newline_delimited" }
[sources.in]
type = "file"
include = ["/stackable/logs/*"]
oldest_first = true
[sources.in.multiline]
mode = "halt_with"
condition_pattern = "</log4j:event>$"
start_pattern = "^<log4j:event"
timeout_ms = 100

[transforms.parse]
inputs = ["in"]
type = "remap"
source = '''
# Add a dummy event during parsing, to work around https://github.com/vectordotdev/vector/issues/11901
wrapped_message = "<root xmlns:log4j=\"http://jakarta.apache.org/log4j/\">"+string!(.message)+"<log4j:event level=\"FOO\"/></root>";
parsed = parse_xml!(wrapped_message, attr_prefix: "");
# Delete the dummy event
del(parsed.root.event[-1])
.event = array(parsed.root.event) ?? [parsed.root.event];
. = unnest(.event)
'''

[transforms.process]
inputs = ["parse"]
type = "remap"
source = '''
._file = del(.file);
._host = del(.host);
del(.source_type);
event = del(.event);
if exists(event.timestamp) {
  event.timestamp = parse_timestamp(event.timestamp, "%s%3f") ?? event.timestamp;
}
. = merge(., event)
'''

[sinks.stdout]
inputs = ["process"]
type = "console"
encoding.codec = "json"

[sinks.opensearch]
inputs = ["process"]
type = "elasticsearch"
endpoint = "https://192.168.1.147:9200"
mode = "bulk"
auth = { strategy = "basic", user = "admin", password = "admin" }
tls = { verify_hostname = false, ca_file = "/stackable/config/opensearch-ca.pem" }
"#,
        )
        .add_data(
            "opensearch-ca.pem",
            r#"
-----BEGIN CERTIFICATE-----
MIID/jCCAuagAwIBAgIBATANBgkqhkiG9w0BAQsFADCBjzETMBEGCgmSJomT8ixk
ARkWA2NvbTEXMBUGCgmSJomT8ixkARkWB2V4YW1wbGUxGTAXBgNVBAoMEEV4YW1w
bGUgQ29tIEluYy4xITAfBgNVBAsMGEV4YW1wbGUgQ29tIEluYy4gUm9vdCBDQTEh
MB8GA1UEAwwYRXhhbXBsZSBDb20gSW5jLiBSb290IENBMB4XDTE4MDQyMjAzNDM0
NloXDTI4MDQxOTAzNDM0NlowgY8xEzARBgoJkiaJk/IsZAEZFgNjb20xFzAVBgoJ
kiaJk/IsZAEZFgdleGFtcGxlMRkwFwYDVQQKDBBFeGFtcGxlIENvbSBJbmMuMSEw
HwYDVQQLDBhFeGFtcGxlIENvbSBJbmMuIFJvb3QgQ0ExITAfBgNVBAMMGEV4YW1w
bGUgQ29tIEluYy4gUm9vdCBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
ggEBAK/u+GARP5innhpXK0c0q7s1Su1VTEaIgmZr8VWI6S8amf5cU3ktV7WT9SuV
TsAm2i2A5P+Ctw7iZkfnHWlsC3HhPUcd6mvzGZ4moxnamM7r+a9otRp3owYoGStX
ylVTQusAjbq9do8CMV4hcBTepCd+0w0v4h6UlXU8xjhj1xeUIz4DKbRgf36q0rv4
VIX46X72rMJSETKOSxuwLkov1ZOVbfSlPaygXIxqsHVlj1iMkYRbQmaTib6XWHKf
MibDaqDejOhukkCjzpptGZOPFQ8002UtTTNv1TiaKxkjMQJNwz6jfZ53ws3fh1I0
RWT6WfM4oeFRFnyFRmc4uYTUgAkCAwEAAaNjMGEwDwYDVR0TAQH/BAUwAwEB/zAf
BgNVHSMEGDAWgBSSNQzgDx4rRfZNOfN7X6LmEpdAczAdBgNVHQ4EFgQUkjUM4A8e
K0X2TTnze1+i5hKXQHMwDgYDVR0PAQH/BAQDAgGGMA0GCSqGSIb3DQEBCwUAA4IB
AQBoQHvwsR34hGO2m8qVR9nQ5Klo5HYPyd6ySKNcT36OZ4AQfaCGsk+SecTi35QF
RHL3g2qffED4tKR0RBNGQSgiLavmHGCh3YpDupKq2xhhEeS9oBmQzxanFwWFod4T
nnsG2cCejyR9WXoRzHisw0KJWeuNlwjUdJY0xnn16srm1zL/M/f0PvCyh9HU1mF1
ivnOSqbDD2Z7JSGyckgKad1Omsg/rr5XYtCeyJeXUPcmpeX6erWJJNTUh6yWC/hY
G/dFC4xrJhfXwz6Z0ytUygJO32bJG4Np2iGAwvvgI9EfxzEv/KP+FGrJOvQJAq4/
BU36ZAa80W/8TBnqZTkNnqZV
-----END CERTIFICATE-----
"#,
        )
        .build()
        .with_context(|_| BuildRoleGroupConfigSnafu {
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_server_rolegroup_service(
    rolegroup: &RoleGroupRef<ZookeeperCluster>,
    zk: &ZookeeperCluster,
) -> Result<Service> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(zk)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(zk, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                zk,
                APP_NAME,
                zk_version(zk)?,
                &rolegroup.role,
                &rolegroup.role_group,
            )
            .with_label("prometheus.io/scrape", "true")
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(vec![
                ServicePort {
                    name: Some("zk".to_string()),
                    port: APP_PORT.into(),
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                },
                ServicePort {
                    name: Some("metrics".to_string()),
                    port: 9505,
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                },
            ]),
            selector: Some(role_group_selector_labels(
                zk,
                APP_NAME,
                &rolegroup.role,
                &rolegroup.role_group,
            )),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_server_rolegroup_service`]).
fn build_server_rolegroup_statefulset(
    rolegroup_ref: &RoleGroupRef<ZookeeperCluster>,
    zk: &ZookeeperCluster,
    server_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet> {
    let rolegroup = zk
        .spec
        .servers
        .as_ref()
        .context(NoServerRoleSnafu)?
        .role_groups
        .get(&rolegroup_ref.role_group);
    let zk_version = zk_version(zk)?;
    let image = format!(
        "docker.stackable.tech/stackable/zookeeper:{}-stackable0",
        zk_version
    );
    let env = server_config
        .get(&PropertyNameKind::Env)
        .iter()
        .flat_map(|env_vars| env_vars.iter())
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect::<Vec<_>>();
    let mut container_prepare = ContainerBuilder::new("prepare")
        .image(&image)
        .args(vec![
            "sh".to_string(),
            "-c".to_string(),
            [
                "chown stackable:stackable /stackable/data /stackable/logs",
                "chmod a=,u=rwX /stackable/data /stackable/logs",
                "expr $MYID_OFFSET + $(echo $POD_NAME | sed 's/.*-//') > /stackable/data/myid",
            ]
            .join(" && "),
        ])
        .add_env_vars(env.clone())
        .add_env_vars(vec![EnvVar {
            name: "POD_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_string()),
                    field_path: "metadata.name".to_string(),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        }])
        .add_volume_mount("data", "/stackable/data")
        .add_volume_mount("logs", "/stackable/logs")
        .build();
    container_prepare
        .security_context
        .get_or_insert_with(SecurityContext::default)
        .run_as_user = Some(0);
    let container_zk = ContainerBuilder::new("zookeeper")
        .image(image)
        .args(vec![
            "bin/zkServer.sh".to_string(),
            "start-foreground".to_string(),
            "/stackable/config/zoo.cfg".to_string(),
        ])
        // .add_env_var(
        //     "SERVER_JVMFLAGS",
        //     "-Dlog4j.configuration=file:/stackable/config/log4j.properties",
        // )
        .add_env_vars(env)
        // Only allow the global load balancing service to send traffic to pods that are members of the quorum
        // This also acts as a hint to the StatefulSet controller to wait for each pod to enter quorum before taking down the next
        .readiness_probe(Probe {
            exec: Some(ExecAction {
                command: Some(vec![
                    "bash".to_string(),
                    "-c".to_string(),
                    // We don't have telnet or netcat in the container images, but
                    // we can use Bash's virtual /dev/tcp filesystem to accomplish the same thing
                    format!(
                        "exec 3<>/dev/tcp/localhost/{} && echo srvr >&3 && grep '^Mode: ' <&3",
                        APP_PORT
                    ),
                ]),
            }),
            period_seconds: Some(1),
            ..Probe::default()
        })
        .add_container_port("zk", APP_PORT.into())
        .add_container_port("zk-leader", 2888)
        .add_container_port("zk-election", 3888)
        .add_container_port("metrics", 9505)
        .add_volume_mount("data", "/stackable/data")
        .add_volume_mount("config", "/stackable/config")
        .add_volume_mount("logs", "/stackable/logs")
        .build();
    let container_vector = ContainerBuilder::new("vector")
        .image("timberio/vector:0.20.0-distroless-static")
        .args(vec![
            "--config".to_string(),
            "/stackable/config/vector.toml".to_string(),
        ])
        .add_volume_mount("config", "/stackable/config")
        .add_volume_mount("logs", "/stackable/logs")
        .build();
    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(zk)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(zk, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                zk,
                APP_NAME,
                zk_version,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            )
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: if zk.spec.stopped.unwrap_or(false) {
                Some(0)
            } else {
                rolegroup.and_then(|rg| rg.replicas).map(i32::from)
            },
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    zk,
                    APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )),
                ..LabelSelector::default()
            },
            service_name: rolegroup_ref.object_name(),
            template: PodBuilder::new()
                .metadata_builder(|m| {
                    m.with_recommended_labels(
                        zk,
                        APP_NAME,
                        zk_version,
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    )
                })
                .add_init_container(container_prepare)
                .add_container(container_zk)
                .add_container(container_vector)
                .add_volume(Volume {
                    name: "config".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: Some(rolegroup_ref.object_name()),
                        ..ConfigMapVolumeSource::default()
                    }),
                    ..Volume::default()
                })
                .add_volume(Volume {
                    name: "logs".to_string(),
                    empty_dir: Some(EmptyDirVolumeSource::default()),
                    ..Volume::default()
                })
                .security_context(PodSecurityContext {
                    fs_group: Some(1000),
                    ..PodSecurityContext::default()
                })
                .build_template(),
            volume_claim_templates: Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some("data".to_string()),
                    ..ObjectMeta::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    resources: Some(ResourceRequirements {
                        requests: Some({
                            let mut map = BTreeMap::new();
                            map.insert("storage".to_string(), Quantity("1Gi".to_string()));
                            map
                        }),
                        ..ResourceRequirements::default()
                    }),
                    ..PersistentVolumeClaimSpec::default()
                }),
                ..PersistentVolumeClaim::default()
            }]),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

pub fn zk_version(zk: &ZookeeperCluster) -> Result<&str> {
    zk.spec.version.as_deref().context(ObjectHasNoVersionSnafu)
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}
