# Access Delegation — session notes (#868 / #865)

Design notes feeding the ADR. Facts were verified against source unless marked *(unverified)*.
Companion to `znode-agent-prototype-plan.md`, which is the implementation plan.

**Problem, narrowly stated:** zookeeper-operator cannot provision znodes against an
authentication-enabled ZooKeeper. Fix that, and learn from it. The paradigm question (#865) is a
second deliverable that this informs and must not block.

**Where it landed:** per-cluster agent (Strimzi Entity Operator pattern), same binary in `agent` mode,
credential mounted at pod start, grant expressed as fields on `ZookeeperCluster`. One-line position:
**copy Strimzi's topology, fix its trust model.**

---

## 1. Verified facts

### This repo
- `client.portUnification=true` is set unconditionally when TLS is on (`crd/security.rs:256`) — this
  is why znode provisioning works today: **the operator connects in plaintext**.
- `get_tls_secret_class()` (`crd/security.rs:305-318`) resolves to **one** SecretClass, and
  `create_server_tls_volume` provisions keystore *and* truststore from it. So the CA proving
  ZooKeeper's identity is also the trust anchor for client certs. **Splitting these is the single
  most important change** — see §4.
- `ensure_znode_exists` creates `world:anyone` / `Permission::ALL` ACLs (`znode_controller.rs:455`).
- The znode path defaults to **root level** — `format!("/znode-{}", uid)` (`znode_controller.rs:198`)
  — so creating it needs CREATE on `/`. Least privilege needs a configurable parent prefix.
- `error_policy` requeues at a flat 5s (`znode_controller.rs:375-381`) — a hot loop for any
  persistently absent dependency.
- Cleanup already treats "ZookeeperCluster gone" as "assume znode gone" and drops the finalizer
  (`znode_controller.rs:234-238`). Precedent for the escape hatch a credential failure needs.
- The znode controller already watches `ZookeeperCluster` and remaps to affected ZNodes
  (`main.rs:188-206`).
- `ZookeeperZnode` may reference a cluster in another namespace
  (`znode_controller/dereference.rs:75`), documented in `isolating_clients_with_znodes.adoc`.
- `znode_controller` imports from `zk_controller::build::resource::discovery` — the module boundary
  between operator and agent does **not** fall cleanly.
- Helm already sets `OPERATOR_IMAGE` from the `internal.stackable.tech/image` pod annotation
  (`deployment.yaml:48`), but **no Rust code reads it**.
- Operator ClusterRole has **no** `secretclasses` and **no** `secrets` RBAC.
- `RESTART_CONTROLLER_ENABLED_LABEL` is applied to the StatefulSet (`statefulset.rs:431`) —
  commons-operator's mechanism for rolling pods ahead of cert expiry.
- `ring 0.17.14` is in the tree (C + asm). "It's all Rust" is not quite true where the network-facing
  crypto lives.
- Nix builds `cargo.allWorkspaceMembers` (`default.nix:128`) and the Dockerfile copies
  `/app/*` (`:193`), so adding a second binary costs almost nothing at the packaging layer.

### ZooKeeper (3.9.4 / 3.9.5 are what SDP ships)
- **`client.portUnification` cannot be removed.** It works around
  [ZOOKEEPER-4276](https://issues.apache.org/jira/browse/ZOOKEEPER-4276), fixed in **3.10.0 only —
  and 3.10.0 is unreleased** (3.9.5 current, 3.8.6 stable, no 3.10 branch). On 3.9.x,
  `secureClientPort` alone hits the NettyServerCnxnFactory double-bind; `clientPort` +
  `secureClientPort` hits "static.config different from dynamic config" because SDP writes `server.N`
  entries and ZooKeeper splits static/dynamic config.
- **Authorization is per-znode ACLs; credentials carry no permissions.** CREATE and DELETE are checked
  on the **parent**, not the child. **ACLs are not inherited** — documented explicitly. Schemes:
  `world`, `auth`, `digest`, `ip`, `x509`.
  → Least privilege = customer pre-creates a parent znode granting the operator CREATE/DELETE/READ
  there and nothing at `/`.
  → Recursive cleanup needs READ on each node and DELETE on each node's parent, so consumer-created
  children with their own ACLs will break deletion.
- **Audit logging exists and is product-native**: `audit.enable=true`, default
  `Slf4jAuditLogger`, fields `session, user, ip, operation, znode, znode_type, acl, result`. With
  `x509`, `user` is the cert's X500 Principal. Logged only on the server the client connected to, so
  it needs aggregation — this repo already has `vectorAggregatorConfigMapName`.
- ACL bypasses that we must **never** require: `DigestAuthenticationProvider.superDigest`,
  `X509AuthenticationProvider.superUser`, `zookeeper.skipACL`.
- Monitoring does **not** use the client protocol — `PrometheusMetricsProvider` on 7000,
  `admin.serverPort=8080` (`zk_controller.rs:488-495`). A "monitoring identity" is moot for ZK.

### tokio-zookeeper
- **No TLS. No `addauth`** — `OpCode::Auth = 100` is declared but there is no `Request::Auth` variant.
- **But** `proto/mod.rs:18` already defines `ZooKeeperTransport: AsyncRead + AsyncWrite` with an
  associated `Addr`, and `Packetizer<S>` is generic over it. Only `connect()`/`handshake()`
  (`lib.rs:276-298`) pin `TcpStream`. One refactor unlocks a rustls **or** a `UnixStream` transport.
- Reconnect calls `S::connect(addr)`, so the client cert must live inside `Addr`.
- **Stackable publishes this crate** — nightkr published 0.4.0, sbernauer 0.3.0. No upstream
  negotiation. The fork's `main` is already ahead of the published 0.4.0; the operator consumes
  crates.io with `[patch.crates-io]` commented out.

### secret-operator (= #865's Layer 3, already built)
- Backends: `auto_tls`, `cert_manager`, `k8s_search`, `kerberos_keytab`.
- `cert_manager` creates cert-manager `Certificate` objects with an `issuerRef` → **can front the
  customer's real PKI, holding no CA key.**
- `kerberos_keytab` takes an **admin keytab** and provisions principals in the KDC — #865's
  "delegated OU / fully-managed" Kerberos cell, shipped.
- `TrustStore` CRD + `ProvisionParts::Public` already does object-in → trust-material-out.
- **`SecretClassSpec` carries only a `backend`.** No `allowedNamespaces`, no consumer-authorization
  concept anywhere. **Any pod that can be created with the right volume annotation obtains a
  credential from any SecretClass.** A dedicated SecretClass scopes *trust*, not *issuance*.
- CSI volumes are provisioned at pod start and **never refreshed in place** — renewal is
  pod restart, via the commons-operator restart controller.

### #865 matrix vs what exists

| Mechanism | Platform-managed | Customer-managed | Rotation-managed | Fully-managed |
|---|---|---|---|---|
| TLS/mTLS | `autoTls` ✅ | `k8sSearch` ✅ | `certManager`+ACME ✅ | `certManager` / supplied CA ✅ |
| Kerberos | gap | `k8sSearch` ✅ | gap | `kerberosKeytab` ✅ |
| LDAP/AD | gap | `k8sSearch` ✅ | gap | gap |
| OIDC/OAuth2 | gap | `k8sSearch` ✅ (client secret) | gap | gap |

Over half implemented. The gaps cluster in the two rows #865 itself marks with question marks, and
they share one shape: **a continuing exchange with a live external system** (token refresh, password
rotation) rather than one-time materialisation.

### SDP product auth (from each operator's source)

| Product | Supported | Client wire | Proxyable |
|---|---|---|---|
| Trino / Superset / Airflow / Druid / NiFi / OpenSearch / OPA | LDAP, OIDC, Static, TLS (varies) | HTTPS + header or mTLS | ✅ ideal |
| ZooKeeper | **TLS only** | binary, transport-level x509 | ✅ |
| Kafka | Kerberos, TLS — **max one auth class** | binary + advertised brokers | ⚠️ hard |
| HDFS / HBase / Hive | **Kerberos only** | RPC + SASL | ❌ |

- ZooKeeper is the platform's **only** transport-level-auth product; 8 of 12 are HTTP. **Assume a
  ZK-derived conclusion does not transfer until checked against an HTTP product.**
- Kafka dropped ZooKeeper at 4.0.0 (KRaft). Druid is deprecated in SDP; HBase/HDFS/Hive deprioritised.
- Superset/Airflow are Flask-AppBuilder → single-valued `AUTH_TYPE`, **no second auth path and no
  client-cert auth**. They are the counterexample to "the platform gets its own mTLS path".
  *(unverified)*

---

## 2. Decision: where resource provisioning runs

The options differ by **whose pod runs the code**, and each inherits that pod's identity.

| | Operator (central) | Product-pod sidecar | Per-cluster agent | Job per action | Consumer initContainer | `pods/exec` |
|---|---|---|---|---|---|---|
| Credential acquisition | Dynamic targets → runtime Secret read | None needed (localhost) | Mount at pod start | Mount at pod start | Already has one | None needed |
| Material location | etcd + operator memory | Pod tmpfs | Pod tmpfs | Pod tmpfs | n/a | n/a |
| Exposure | All clusters, operator lifetime | One cluster | One cluster, pod lifetime | One action, seconds | n/a | n/a |
| Convergence | Level-triggered | Level-triggered | Level-triggered | **Edge-triggered** | On pod start | Level-triggered |
| Cost scales with | clusters | product replicas | clusters | **resources** ❌ | consumers | — |
| Deletion path | Existing escape hatch | Dies with cluster | Agent must outlive last CR | **Blocked in Terminating ns** | Nobody cleans up ❌ | ok |
| Upgrade provisioning logic | Restart operator | **Roll the customer's product** ❌ | Restart small Deployment | Next Job | Roll the consumer | Restart operator |

**Eliminated and why:**
- **`pods/exec`** — bypasses the product's authentication by running inside the trusted container. The
  purest backdoor shape. Reject on the record so it isn't reached for later.
- **Consumer initContainer** — most product resources (NiFi flow, Superset dataset, Airflow
  connection) have **no consuming pod at all**. Works only for znodes.
- **Job** — cost is per resource, and resource proliferation is the point of the paradigm. Strimzi's
  topic operator explicitly batches Admin calls for throughput. Fine as a spike vehicle.
- **Product-pod sidecar** — best credential story for ZooKeeper, disqualified by release coupling
  (a provisioning bugfix rolls the customer's Kafka/NiFi/Superset), and its headline advantage
  ("credential already there") holds only for transport-level-auth products, i.e. ZK alone.
  *The option that looks best for ZooKeeper looks worst as a paradigm.*
- **Central operator** — smallest diff, but the only option that actually needs #865's Layer 2, and it
  makes every product operator a credential holder over time.

**Agent packaging: same binary, `agent` subcommand.** Not a separate binary — verified that the
module boundary doesn't fall cleanly (`znode_controller` imports from
`zk_controller::build::resource::discovery`) and the agent is *mostly* shared code, so a split would
need a shared lib containing most of the crate. The structural-isolation benefit only materialises
after relocating the shared build helpers, which is worth doing on its own merits but isn't urgent.
Precedent: CloudNativePG (`manager controller` / `manager instance`), Strimzi (one
`docker-images/operator`).

---

## 3. Decision: credential shape — material vs channel

| | Deliver material | Agent performs auth (channel) |
|---|---|---|
| Operator sees | cert/password/keytab | plaintext localhost socket |
| Per-mechanism code | in every operator | in one agent |
| Works for | everything | not in-protocol SASL on binary protocols |

Not either/or. **Volume + material for material-based mechanisms; agent for exchange-based ones**
(OIDC token acquisition, LDAP rotation). Keep "material" expressible even if unimplemented — Kafka's
advertised-listeners topology may force delivery even under mTLS.

---

## 4. Decision: how the customer grants access

| | Separate grant CRD | Fields on the product CRD | Trust anchor in product config |
|---|---|---|---|
| Enforced by | our code (a promise) | our code (a promise) | **the product's TLS stack** |
| Enumerable across products | ✅ | ✗ | ✗ |
| Orphan grants possible | ✅ ❌ | no (co-located) | no |
| New RBAC surface | yes | no (= cluster ownership) | no |
| Revocation | instant | edits cluster spec → rolling restart | rolling restart |

**Landed: fields on the product CRD, and the field that matters is the client trust anchor** — you
need it anyway for `ssl.clientAuth` to work, and ZooKeeper enforces it rather than us. A grant field
only our code reads is strictly weaker. This deletes the grant CRD entirely.

**The backdoor property was about implicitness, not about which CA.** Today ZooKeeper accepts client
certs from its own server CA *by construction, with nothing configured*. Splitting trust from identity
is what fixes that. Once the trust anchor is a separate, optional, explicitly written field, pointing
it at the autoTls `tls` SecretClass is a choice the customer made and can delete — which is exactly
what the issue asked for and is not a backdoor. Combined with the secret-operator finding above (no
consumer authorization on *any* SecretClass), a dedicated CA narrows what ZooKeeper *accepts*, not who
can obtain a cert; and once ACLs key on the agent's DN, it is defence in depth rather than the
primary control.

**Enforcement is ACLs, not the port.** Port unification can't be removed (see §1), but it only governs
whether a client can *connect*. An unauthenticated session carries no auth ids, so it fails any ACL
that isn't `world:anyone`. The real hole is the hardcoded `world:anyone`/`ALL`.

**Liveness must be asserted, not inferred.** If the agent can't start — typo'd SecretClass, missing
Secret, unschedulable, image pull, crashloop, OOM, dead node — the sole writer of `ZookeeperZnode`
status is absent and nothing reports it. Deployment readiness is the wrong signal (false positives
mid-rollout, false negatives for a running-but-wedged agent). Use a `coordination.k8s.io` **Lease**
renewed by the agent; staleness is one uniform signal covering every cause, with an explicit
threshold instead of a race.

---

## 5. Precedent

| Project | Shape | Problem it actually solves | Match |
|---|---|---|---|
| **Strimzi** | Entity Operator = Deployment per Kafka cluster; Cluster Operator reads `<cluster>-cluster-operator-certs` at runtime via `PemAuthIdentity.clusterOperator(Secret)` | API-mutable state, product it doesn't own the process of, network + real credential | **closest** |
| Zalando postgres-operator | Central operator, `sql.Open` **as superuser**, plus `pods/exec`. Patroni is HA, *not* resource provisioning | same problem, solved the way we're avoiding | counterexample |
| MongoDB Community | Operator reads password Secret → derives SCRAM → writes automation config Secret. **Never connects to mongod** | config-shaped resources + agent owns the process | doesn't transfer |
| Rook | One prepare Job per node/PVC; results returned via **ConfigMap** | node locality, no credential at all | weak |
| CloudNativePG | One binary, subcommands; injects itself into the product pod; direct operator→agent HTTP on `StatusPort 8000` for *synchronous* ops | in-pod agent + optional RPC supplement | packaging precedent |
| OCM (klusterlet) | Agent per managed cluster, **pull**; hub holds no credentials, agent's key never leaves | cross-cluster network direction + no central credentials | rationale only |

**Strimzi's two-CA split is the model:** cluster CA signs Strimzi component certs; clients CA is what
brokers verify clients against. Exactly the split this repo collapses. But Strimzi's default is
platform-owned CAs and `generateCertificateAuthority: false` requires handing over a CA **private
key** — which no PKI team does. That is the customer objection, and the differentiator is provenance,
not topology.

**Push vs pull does not apply in-cluster.** Operator and agent never talk; both watch the same API
server and the CR is the interface. The only operator→agent "push" is the agent's Deployment spec at
creation. Pull matters only for cross-cluster or non-Kubernetes targets.

---

## 6. #865 re-read

- **Layer 3 (registry): exists.** Extend the LDAP and OIDC rows; don't rebuild.
- **Layer 2 (vendoring service): don't build it as a service.** Needed only for continuing exchanges,
  non-pod requestors, or per-request parameters. Nothing needed for #868. A central component able to
  vend everything is the aggregation shape customers objected to.
- **Layer 1 conflates two goals.** *Provenance*-agnostic is achieved today by SecretClass.
  *Mechanism*-agnostic is mostly unachievable through material delivery. Naming them separately makes
  the layer tractable.
- **Delete "credential of type X"** from the consumption API — it commits every operator to knowing
  how its product authenticates.
- **Kubernetes as OIDC issuer** (`--service-account-issuer`, JWKS) is independent of the API server as
  OIDC *consumer*, so enabling it blocks no customer IdP. Products accepting JWTs (Kafka OAUTHBEARER,
  Trino, OpenSearch) could verify the operator's own SA token, needing no minted credential at all.
  **Must scope on `sub`, not just issuer** — any pod can request any audience. Doesn't help ZooKeeper,
  which is why starting from ZK hides this option.

---

## 7. Harder CR cases (the paradigm, not znodes)

| Case | Example | Why it breaks a naive design |
|---|---|---|
| **Nested credentials** | Superset DB connection, Airflow `Connection`, NiFi `DBCPConnectionPool` | Resource *contains* a credential for a third system → provisioner needs Secret read in the **user's** namespace, must handle rotation, must never log. A second credential flow entirely. Strongest argument for the per-cluster agent. |
| **Operations, not state** | Kafka replication-factor change; OpenSearch reindex | Long-running, throttled. Strimzi uses a separate `KafkaRebalance` CR with proposal→approval. Partition count can only increase. |
| **Drift / two-way sync** | Superset dashboards, NiFi flows | UI editing is the *normal workflow*. Strimzi shipped bidirectional topic sync and replaced it with the **unidirectional** operator. May need "create once, stop reconciling". |
| **Dependency DAG** | database → dataset → chart → dashboard | Four CR levels, deletion ordering, finalizer chains across kinds. |
| **No natural key** | Superset chart ID, NiFi process-group UUID | ID assigned by the product → stored in status. Status loss ⇒ **duplicates**, not convergence. |

Design against `SupersetDatabaseConnection` if you want one case that exercises all of it. Don't let
ZooKeeper set the effort estimate. *(NiFi/Superset API details unverified)*

---

## 8. CRD conventions (checked against zookeeper, trino, druid)

- **One-of variants** → externally-tagged serde enum with `rename_all = "camelCase"`, variant name as
  the YAML key. `TrinoCatalogConnector`, `MetadataDatabaseConnection`, Trino's `Property`. Needs
  `singleton_map_recursive` for YAML round-tripping; this repo uses `yaml_from_str_singleton_map`.
- **SecretClass references** → flat field named `<purpose>SecretClass`, typed
  `v2::types::kubernetes::SecretClassName`, not `String` (`crd/tls.rs`).
- **Reading from a customer Secret** → two precedents: `credentialsSecret: <name>` with well-known
  keys (Trino `googleSheet`), or `valueFromSecret` with a flattened k8s-openapi `SecretKeySelector`
  (`trino-operator/src/crd/catalog/generic.rs`). For this design, well-known `kubernetes.io/tls` keys
  (`tls.crt`/`tls.key`) are better — both sources must produce identical files anyway, and those names
  are what cert-manager already emits.

---

## 9. Open questions

- Does anyone object to a **leaf** credential in etcd? If not, the central-operator option is small
  and boring. The Strimzi objection is about **CA private keys**, which is categorically different.
- Enterprise CA latency: ticket-based issuance with a multi-hour SLA breaks per-action minting and
  pushes toward caching / resident agents.
- Does the target TLS stack enforce X.509 **name constraints**? The "cryptographically verifiable
  scope" claim rests on it.
- Finalizer forfeit policy: orphaned znodes vs wedged objects and hung namespaces.
- Drift semantics per product (Kafka: CR is truth. NiFi/Superset: probably not).
- **Does a pod-scoped secret-operator cert satisfy `X509AuthenticationProvider` under
  `ssl.hostnameVerification=true`?** Untested, gates the whole spike, and produces the DN string the
  ACLs need.

---

## 10. Claims we can defend

> Every credential the platform obtains traces to a grant you authored, is scoped to a principal you
> can see, and can be revoked by you alone without breaking the product.

Do **not** claim zero trust: customers run our operators, often with broad RBAC. An overclaim here is
worse than no claim. And until ACLs land, authentication alone leaves znodes world-writable — say so.
