# Spike: per-cluster ZookeeperZnode agent

> **Handoff note.** Throwaway spike branch for `stackabletech/zookeeper-operator`, executed from the
> repo root. It will never be merged, so code quality is explicitly not the goal — getting to a
> working demonstration is. The steps below are dependency-ordered; the ordering matters even though
> the split into commits does not. Line references are against the tree as of this plan.

## Context

`ZookeeperZnode` provisioning is done by the central operator, which connects to ZooKeeper in
plaintext. That works today only because `client.portUnification=true` is set unconditionally
(`crd/security.rs:256`) — against a genuinely auth-enabled ensemble the operator has no credential
and znode provisioning fails. That is the gap.

The shape being tested is the Strimzi Entity Operator pattern: the operator creates one **agent** per
`ZookeeperCluster` — the same binary in a new `agent` mode — which owns `ZookeeperZnode` end to end
and mounts its own credential at pod start. Because the agent's target is fixed at creation, no
runtime credential-vending API is needed at all. That is the main thing this spike is meant to show.

Two credential sources must work with **identical agent code**: a cert minted by secret-operator, and
a static cert the customer dropped into a Secret. The agent reads PEM files from one directory and
cannot tell which it got — only the pod spec differs.

## Design decisions

### Credential and trust anchor are separate fields

For a minted cert they always name the same SecretClass, which is why one field looks sufficient. The
static source is what makes the second earn its place: for a customer-supplied cert, secret-operator
has no CA to expose, so what ZooKeeper trusts must be stated separately.

```yaml
spec:
  clusterConfig:
    platformAccess:                              # absent = no platform access (default)
      trustAnchorSecretClass: platform-access-tls   # what ZooKeeper trusts for client certs
      credential:                                # exactly one variant
        secretClass: platform-access-tls
        # -- or --
        secret: zk-plat-cert                     # a kubernetes.io/tls Secret
```

This shape follows existing SDP CRD precedent — checked against zookeeper, trino and druid:

- **`credential` is an externally-tagged serde enum** with `#[serde(rename_all = "camelCase")]`, so
  the variant name is the YAML key. Same as Trino's `TrinoCatalogConnector`
  (`connector: { hive: … }`), Druid's `MetadataDatabaseConnection` (`database: { postgresql: … }`)
  and Trino's `Property`. Variants are newtypes: `SecretClass(SecretClassName)` and `Secret(String)`.
  Note these enums need `singleton_map_recursive` for YAML round-tripping — this repo already uses
  `yaml_from_str_singleton_map` in `crd/mod.rs`, so follow that.
- **`trustAnchorSecretClass` is flat and typed `SecretClassName`**, matching `crd/tls.rs`'s
  `quorum_secret_class: SecretClassName` / `server_secret_class: Option<SecretClassName>`. SDP names
  these with a `SecretClass` suffix rather than nesting under a `secretClass` key, and uses the
  `v2::types::kubernetes::SecretClassName` newtype rather than `String`.
- **Credential** → mounted into the agent at `/stackable/platform_access_tls` as `tls.crt` /
  `tls.key`. `secretClass` becomes a CSI ephemeral volume via the existing
  `SecretClassVolume::to_ephemeral_volume_source` (operator-rs `commons/secret_class.rs:39`);
  `secret` becomes a plain `SecretVolumeSource`. Same paths, same files, so agent code is identical
  across both sources — the constraint the spike exists to prove.
- **Well-known keys, not a key selector.** The `secret` variant names a `kubernetes.io/tls` Secret and
  reads `tls.crt` / `tls.key` — no `items` mapping needed, because a plain secret volume then
  produces exactly the layout a secret-operator CSI volume does. Those key names are mandated by the
  `kubernetes.io/tls` type and are what cert-manager already emits, so this costs the customer
  nothing. Precedent: Trino's `googleSheet: { credentialsSecret: … }`.

  A `secretKeyRef`-style selector *does* have SDP precedent —
  `trino-operator/src/crd/catalog/generic.rs` has
  `ValueFromSecret { #[serde(flatten)] secret_key_selector: SecretKeySelector }` using k8s-openapi's
  type. If arbitrary key names turn out to matter, switch the `Secret` variant to two flattened
  `SecretKeySelector`s and build the volume with `items` mapping key→path. It's a contained change,
  but it buys flexibility the design doesn't need, since both sources must land on `tls.crt` /
  `tls.key` regardless.
- **Files, not env vars.** Whichever shape is used, the material must be mounted — credential
  material in env vars leaks via `kubectl describe pod` and `/proc/<pid>/environ`.
- **Trust anchor is SecretClass-only, deliberately.** ZooKeeper's `ssl.trustStore.location` wants a
  PKCS#12 and secret-operator produces those. Accepting a raw PEM `ca.crt` would mean teaching the
  prepare init container (`zk_controller/build/command.rs`) to convert — a whole workstream for no
  spike value. A customer whose CA lives in a Secret writes a one-object `SecretClass` with
  `backend.k8sSearch`.

### autoTls is a first-class option, and needs no new variant

The issue asks specifically for *"minting a certificate (reuse the autoTls CA)"*. That is
`credential.secretClass: tls` with `trustAnchorSecretClass: tls` — the existing platform SecretClass,
no special case in the code. Demo it alongside a dedicated SecretClass and the static Secret; all
three exercise identical agent code, which is the point.

**What made the original suggestion feel like a backdoor was implicitness, not the CA.** Today
`get_tls_secret_class()` returns one SecretClass for both keystore and truststore, so ZooKeeper
accepts client certs from its own server CA *by construction, with nothing configured*. The
truststore split fixes exactly that. Once the trust anchor is a separate, optional, explicitly
written field, pointing it at `tls` is a choice the customer made, wrote down, and can delete — which
is the whole property that was missing.

The remaining difference between `tls` and a dedicated SecretClass is narrower than it looks, and
worth stating accurately in the writeup because it is counter-intuitive: **`SecretClassSpec` carries
only a `backend`. There is no `allowedNamespaces` and no consumer-authorization concept anywhere in
secret-operator**, so any pod that can be created with the right volume annotation obtains a
credential from *any* SecretClass. A dedicated platform SecretClass is therefore not more
access-controlled than `tls`; it only narrows the set of certificates ZooKeeper will accept. And once
step 6's ACLs are keyed on the agent's specific DN, a pod minting its own `tls` cert gets its own
pod identity and fails the ACL regardless. The ACL is the primary control in both configurations.

If issuance scoping is genuinely wanted, that is a secret-operator feature request — not something
this spike can demonstrate, and worth recording as a finding.

### Enforcement comes from ACLs, not from the port

`client.portUnification=true` cannot be removed on the versions SDP ships. It is a workaround for
[ZOOKEEPER-4276](https://issues.apache.org/jira/browse/ZOOKEEPER-4276), whose fix landed in **3.10.0
only — and 3.10.0 is unreleased** (3.9.5 current, 3.8.6 stable, no 3.10 branch exists). Both
alternatives in the code comments fail on 3.9.x: `secureClientPort` alone hits the
NettyServerCnxnFactory double-bind, and `clientPort` + `secureClientPort` hits "static.config
different from dynamic config" because SDP writes `server.N` quorum entries and ZooKeeper therefore
splits static/dynamic config.

The port is the wrong lever anyway. Port unification governs whether a client can *connect*;
per-znode ACLs govern what it can *do*, and an unauthenticated session carries no auth ids so it
fails any ACL that is not `world:anyone`. The actual hole is
`Acl { scheme: "world", id: "anyone", perms: ALL }` at `znode_controller.rs:455` — dropping port
unification without fixing that would still leave every znode writable by anyone with any trusted
cert.

So **x509 ACLs are in scope**, and they are what makes the spike demonstrate something rather than
assert it.

### Same-namespace znodes only, but the operator keeps its controller

The agent claims only `ZookeeperZnode`s in its own cluster's namespace. Cross-namespace references
are allowed by `ClusterRef::namespace_relative_from` and documented in
`docs/modules/zookeeper/pages/usage_guide/isolating_clients_with_znodes.adoc`, but supporting them
from a namespaced agent needs a per-agent `ClusterRoleBinding` — a cluster-scoped object that cannot
carry an ownerReference to a namespaced `ZookeeperCluster`, so its lifecycle would need a
`ZookeeperCluster` finalizer. Out of scope.

**The operator therefore keeps its znode controller as an unchanged catch-all.** This is cheap (it is
today's code path; only the `claims` predicate is new) and it is *mandatory*: it is the only thing
that removes a `ZookeeperZnode` finalizer once its `ZookeeperCluster` — and therefore its
owner-referenced agent — has been deleted. Without it `kubectl delete namespace` hangs forever and
**every kuttl test's teardown breaks**, which makes the spike impossible to iterate on.

## Work, in dependency order

### 0. The experiment — do this before writing any code

ZooKeeper sets `ssl.hostnameVerification=true` unconditionally (`crd/security.rs:262`), which makes
the server verify the *client* certificate. Whether a pod-scoped secret-operator cert satisfies
`X509AuthenticationProvider` is untested, and it is the one thing that can invalidate everything
below.

Bring up an auth-enabled ZooKeeper, exec into a pod, and run `zkCli.sh` with a secret-operator-issued
client cert. Confirm the connection succeeds and read back the `x509` principal ZooKeeper derives
(`getAcl` on a node you create, or the server log). Two things depend on the answer: what identity
the agent must request, and the exact DN string step 6 puts in ACLs.

If this fails, stop and reconsider — it changes the credential shape, not just the code.

### 1. Decouple the znode path from `ZookeeperSecurity`

The agent must not read cluster-scoped `AuthenticationClass` objects, so it cannot construct
`ZookeeperSecurity`. It only ever needs the port.

- `znode_controller/dereference.rs` — drop `authentication_classes` from `DereferencedObjects` and
  the `FetchAuthenticationClasses` variant.
- `znode_controller/validate.rs` — `ValidatedZnode.zookeeper_security` → `client_port: Port`.
- `zk_controller/build/resource/discovery.rs` — `build_discovery_configmap_for_owner` takes
  `client_port: Port` instead of `&ZookeeperSecurity` (it only calls `.client_port()`).
- `znode_controller.rs` — `zk_mgmt_addr(zk, client_port, cluster_info)`; the `ZookeeperSecurity::new`
  reconstruction at `:243-246` disappears.

### 2. Agent mode + the ownership split

`main.rs`: replace `Opts { cmd: Command }` with a custom enum using both documented extension points
of `stackable_operator::cli::Command`:

```rust
enum Command {
    Agent(AgentArguments),
    #[clap(flatten)]
    Framework(FrameworkCommand<ZookeeperRunArguments>),
}
```

- `ZookeeperRunArguments` = `RunArguments` flattened, plus `operator_image` (`#[arg(long, env)]`).
  `OPERATOR_IMAGE` is **already set** by `deploy/helm/zookeeper-operator/templates/deployment.yaml:48`
  from the `internal.stackable.tech/image` pod annotation, but read by no Rust code today. Add
  `image_pull_policy` / `image_pull_secrets` only if your test registry needs them.
- `AgentArguments`: `zookeeper_cluster_name`, `zookeeper_cluster_namespace`, `zookeeper_client_port`,
  `image_repository`, `platform_access_cert_dir: Option<PathBuf>`, `#[clap(flatten)] CommonOptions`.
  Deliberately **not** `RunArguments` — that drags in mandatory `operator_namespace` /
  `operator_service_name`, which exist only for the conversion webhook.
- Extract the controller chain from `main.rs:170-233` into `znode_controller::run(...)`. Preserve the
  `.store()`-before-`.watches()` ordering.
- `znode_controller::Ctx` gains `mode: Mode` (`OperatorFallback` | `Agent { cluster_name, namespace,
  client_port, .. }`).
- **Agent must not call `signal::crd_established`** (`main.rs:241`) — it needs cluster-scoped CRD
  list/watch and hard-fails after 5s. It also must not construct `create_webhook_server`, since
  `ConversionWebhook` owns CRD creation.

`Mode::claims(&self, znode) -> bool`, checked at the top of `reconcile_znode` **before** the
`merge_patch_status` at `:210`, so a foreign object is never written to. Agent claims: znode is in
the agent's namespace *and* `spec.clusterRef` resolves to the agent's cluster (purely syntactic, no
API call). Operator claims the complement, unchanged — which covers cross-namespace znodes and
missing clusters via the existing `ZkDoesNotExist` path at `:234-238`.

Land both predicates together. Exactly one instance must claim each znode; two writers or zero are
both bad, and zero is the one that wedges namespaces.

### 3. The agent Deployment

New `zk_controller/build/resource/znode_agent.rs`, modelled on `statefulset.rs` (`PodBuilder::
build_template()` is documented upstream as Deployment-usable) and `rbac.rs`.

- **Distinct role, or the agent pods get picked up by the server Services.**
  `recommended_labels()` and `role_group_selector()` hardcode `ZookeeperRole::Server`
  (`zk_controller/validate.rs:295`, `:347`). Define
  `constant!(AGENT_ROLE_NAME: RoleName = "znode-agent")` and use the existing
  `recommended_labels_for(&role, &rg)`, plus a `role_group_selector_for` mirror for
  `Deployment.spec.selector`. The selector must **not** contain the version label — Deployment
  selectors are immutable and the version changes on upgrade.
- `v2::rbac::build_service_account` hardcodes `<cluster>-serviceaccount`, so hand-roll metadata with
  `ObjectMetaBuilder` + `ownerreference_from_resource`. Deployment, SA and RoleBinding share one
  name (`<cluster>-znode-agent`) — different kinds, no collision.
- Pod template annotation `internal.stackable.tech/image` so the `Tiltfile`'s
  `k8s_kind('Deployment', image_json_path=...)` rewrite gives live reload. Worth having for a spike.
- Container args: `agent --zookeeper-cluster-name=… --zookeeper-cluster-namespace=…
  --zookeeper-client-port=… --image-repository=…`. `docker/Dockerfile:198` is `CMD ["run"]`; setting
  `args` replaces it.
- Env `KUBERNETES_NODE_NAME` (fieldRef) **and** `KUBERNETES_CLUSTER_DOMAIN` (from the operator's
  already-resolved `client.kubernetes_cluster_info`). The latter is what keeps cluster-scoped
  `nodes/proxy` out of the agent's RBAC — without it the agent hits the kubelet at startup and dies.
- `KubernetesResources` (`zk_controller.rs:139-147`) gains `deployments: Vec<Deployment>`; apply after
  the StatefulSet loop.
- Do **not** wire the agent into `ZookeeperCluster` conditions — every kuttl test gates on
  `condition=available`, so one agent image-pull failure reddens everything and hides real problems.
- **Apply `RESTART_CONTROLLER_ENABLED_LABEL`** to the Deployment metadata, exactly as
  `statefulset.rs:431` does. CSI ephemeral volumes are provisioned at pod start and never refreshed
  in place, so without this the agent's cert simply expires and the connection dies with no
  self-healing. commons-operator's restart controller is what rolls the pod ahead of expiry. Set a
  short `requestedSecretLifetime` on the credential volume (`with_auto_tls_cert_lifetime`) so the
  renewal path is actually exercised inside a spike-length test rather than in theory.

RBAC (nothing works without this — it fails at 403, not at compile time):
- `clusterrole-operator.yaml`: add `deployments` to the `apps` rule, and add the new agent ClusterRole
  name to the `clusterroles: [bind]` `resourceNames` list, or creating the RoleBinding is rejected as
  privilege escalation.
- New `clusterrole-znode-agent.yaml`: `zookeeperznodes` get/list/patch/watch + `/status` patch,
  `zookeeperclusters` get/list/watch, `configmaps` full, `listeners` get, `events` create/patch, and
  `coordination.k8s.io` → `leases` → `create, get, update` for the liveness lease (step 4c). A Helm
  ClusterRole rather than a controller-built `Role`, because the operator has no `roles` RBAC and
  would hit escalation-prevention checks. Add the OpenShift `nonroot-v2` SCC rule only if you intend
  to test on OpenShift.
- Operator ClusterRole also needs `coordination.k8s.io` → `leases` → `get, list, watch` (to observe
  agent liveness) and `secrets.stackable.tech` → `secretclasses` → `get, list, watch` (to validate
  the configured SecretClass before creating the agent). Both are step 4c.

### 4. CRD field, credential volumes, truststore split

- `crd/mod.rs`: `ZookeeperClusterConfig.platform_access: Option<ZookeeperPlatformAccess>` with the
  credential/trustAnchor enums above. Update `cluster_config_default()`, run `make crds`.
- `crd/security.rs`: carry `platform_access` on `ZookeeperSecurity`; add the volume builders (PEM for
  the agent, PKCS#12 `ProvisionParts::Public` for ZooKeeper's truststore); in `config_settings()`
  redirect `ssl.trustStore.location` to the new volume and set `ssl.clientAuth=need`.
- One validation rule is worth having because it fails silently otherwise: reject `platformAccess`
  when `tls.serverSecretClass` is unset. `tls_enabled()` false skips the entire server-TLS block
  including `ssl.trustStore.location`, so the setting would be quietly ignored and you would debug
  the wrong thing. Skip the `platformAccess` + client-auth-`AuthenticationClass` rejection; just
  don't configure one.

### 4b. The condition on `ZookeeperZnode` — an explicit issue requirement

The issue asks for this directly: *"When no credentials can be provided this needs to show up as a
readable condition on the `ZookeeperZnode`."* Do not skip it; it is one of the four "What to build"
bullets and it is what makes the failure mode legible.

- `ZookeeperZnodeStatus` gains `conditions: Vec<ClusterCondition>` plus
  `impl HasStatusCondition for v1alpha1::ZookeeperZnode`. `ClusterConditionType` is a closed enum
  (`Available`, `Degraded`, `Progressing`, `ReconciliationPaused`, `Stopped`) — there is no custom
  type, so map onto `Degraded=True` with a distinguishing `reason`.
- Distinguish at least: no `platformAccess` configured; credential present but rejected by ZooKeeper;
  cert expired and renewal failing. A bare `Degraded` with no reason is what makes this feature
  annoying to operate.
- Write it via `compute_conditions(znode, &[&builder])` merged into the existing
  `merge_patch_status` call in `reconcile_apply`.

### 4c. When the agent is not running — the common case, not an edge case

A typo'd SecretClass, a missing Secret, an unschedulable pod, a failed image pull, a crashloop, an
OOMKill, an evicted pod, a dead node: all end with no agent, and since the agent is the sole writer
of `ZookeeperZnode` status, the naive design reports *nothing* for exactly the errors the condition
requirement exists to surface.

**Do not infer this from Deployment readiness.** It is an inference about another component's health
and it is wrong in both directions: zero ready replicas is normal mid-rollout (false positive), and a
running-but-wedged agent reports one ready replica while doing nothing (false negative). Chasing
individual causes — unschedulable, pull failure, mount failure — is the brittleness treadmill.

**Invert it: the agent asserts its own liveness, and staleness is the signal.**

- The agent maintains a `coordination.k8s.io/v1` `Lease` named after its cluster, in the cluster's
  namespace, renewed on a short period from inside the agent process. Same primitive kubelet uses for
  node heartbeats and leader election uses for liveness — not a bespoke mechanism.
- The operator watches that Lease. Stale beyond `leaseDurationSeconds` ⇒ no functioning agent ⇒ the
  operator patches a `Degraded` / `AgentUnavailable` condition onto the `ZookeeperZnode`s bound to
  that cluster, writing *only* the condition and returning `await_change()` without reconciling them.

Why this is not brittle: one signal covers every cause, including ones nobody enumerated. It has an
explicit threshold rather than a race, so the transition flapping that readiness-watching would
produce goes away. And "two writers" is safe by construction here — the operator writes only when the
lease is stale, which is precisely when the agent is not writing.

Its honest boundary: a lease renewed by a timer proves the *process* is alive and its API connection
works, not that the reconcile loop is healthy. A live process with a wedged watcher still renews.
Gating renewal on controller health closes that too; for a spike the timer is enough, but say which
one you built.

RBAC: agent needs `coordination.k8s.io` → `leases` → `create, get, update` in its own namespace
(add to the agent ClusterRole); operator needs `get, list, watch`.

**Two cheaper checks worth keeping alongside it**, because they give better messages sooner for the
two most likely mistakes:

- **Validate the SecretClass in the operator before creating the agent.** A missing SecretClass is a
  `ZookeeperCluster` config error — the field the user typo'd lives there — so reporting it on
  `ZookeeperCluster.status.conditions` is the *correct* location, and it surfaces immediately on the
  object they just edited rather than after a scheduling attempt. Needs new RBAC:
  `secrets.stackable.tech` → `secretclasses` → `get, list, watch`, mirroring the existing
  `authenticationclasses` grant. Watch them too, so creating the missing one re-reconciles.
- **Set `optional: true` on the static `secret` volume.** A missing Secret then mounts empty instead
  of wedging the pod; the agent starts, finds no `tls.crt`, and reports through the normal path with
  a precise message. No new RBAC. Not available for the CSI ephemeral volume — `CSIVolumeSource` has
  no `optional` field — which is exactly why the lease is the general mechanism.

### 5. mTLS in the client

Fork work in `stackabletech/tokio-zookeeper` — Stackable publishes this crate (nightkr published
0.4.0, sbernauer 0.3.0), so there is no upstream negotiation.

- `impl ZooKeeperTransport for tokio_rustls::client::TlsStream<TcpStream>` with
  `type Addr = (SocketAddr, Arc<ClientConfig>, ServerName<'static>)` — the config must live in `Addr`
  because `Packetizer` re-invokes `connect(addr)` on reconnect.
- Make `ZooKeeperBuilder::connect` / `handshake` (`src/lib.rs:272-320`) generic; the trait at
  `src/proto/mod.rs:18` is already generic over `AsyncRead + AsyncWrite`, so only those two functions
  pin `TcpStream`.

Operator side: uncomment the `[patch.crates-io]` git line in `Cargo.toml`. `rustls` 0.23.41 and
`tokio-rustls` 0.26.4 are already in `Cargo.lock` via `kube-client`, so no new crates. Any
`Cargo.toml` change needs `make regenerate-nix` (`Cargo.nix` is checked in).

### 6. x509 ACLs, and the demonstration

- When `platformAccess` is configured, `ensure_znode_exists` creates znodes with
  `Acl { scheme: "x509", id: <agent principal DN>, perms: ALL }` instead of the hardcoded
  `world:anyone` at `znode_controller.rs:455`. Keep today's ACL otherwise.
- The DN is **read from the mounted cert's subject at startup**, not computed — with a customer CA it
  is whatever their CA issued. Log it once; a mismatch here is the most confusing possible failure.
- Recursive delete will now fail on subtrees the agent doesn't own: `ensure_znode_missing` needs READ
  on each node and DELETE on each node's *parent*, and children created by a consumer product carry
  the consumer's ACLs. Surface it rather than retrying forever.

## Verification

Manual, mostly — this is a spike.

**The demonstration, which is the actual deliverable:** in a cluster with `platformAccess`
configured, exec into a ZooKeeper pod and run `zkCli.sh` *without* client TLS flags. It will connect
(port unification is still on) but must **fail to read the agent-owned znode** with `NoAuth`. Repeat
with the agent's client cert and confirm it succeeds. That pair is what proves credentials gate
access on 3.9.5, and it is the thing to show a security reviewer.
`tests/templates/kuttl/smoke/test_tls.sh.j2` is the model for the exec-and-assert pattern — note it
currently asserts the *opposite* (that an unsecured `ls /` succeeds), so don't copy it wholesale.

Also confirm by hand:
- Both credential sources produce a working agent with the same code path — swap
  `credential.secretClass` for `credential.secret` and observe no behavioural difference. This is the
  issue's core constraint.
- Deleting the `ZookeeperCluster` lets its `ZookeeperZnode`s actually go away (the stranded-finalizer
  case). Easiest check: `kubectl delete namespace` and confirm it completes.

Two unit tests are worth writing because they catch silent breakage that would otherwise cost an
afternoon:
- The agent pod's labels do not match the server Services' selectors.
- `Mode::claims` — table-driven over {same-ns/other-ns} × {matching/other cluster} × {agent/operator}.

Skip the `json!` equality snapshots, the CRD roundtrip data, and a beku-templated kuttl test; a
hand-written manifest applied with `kubectl` is faster to iterate on. Run the existing
`--test znode` suite once at the end to confirm nothing obvious broke.

Chart: `make regenerate-charts` (needed to deploy at all). Local loop: `make run-dev` (Tilt).

## The writeup — half the deliverable

The issue's "done when" is *"a `ZookeeperZnode` works against an auth-enabled ZooKeeper … **and** we
can write down answers to the open questions plus a recommendation for the transport with reasons."*
Code alone does not close it. Budget time for this; it feeds the ADR.

**Two deviations from the issue that must be stated as findings, not left implicit:**

1. **"Mounting a PVC at pod start does not work" — the spike does exactly that, on purpose.** This is
   the headline result. The runtime-vending requirement is an artefact of *where the code runs*: it
   binds only because the central operator is long-lived with a target set that changes. Move
   provisioning into a process whose target is fixed at creation and the requirement dissolves — no
   consumption API, no provider, no transport. Unstated, this reads as ignoring the brief; stated, it
   is the most useful thing the spike learned.
2. **The spike does not reuse the autoTls CA as the *trust anchor* by default**, though it supports
   it. See the section above — the change that matters is splitting trust from identity, not
   avoiding a particular CA.

**Answers to the four open questions, as the spike demonstrates them:**

- *What does the request/response look like?* There isn't one. Neither option 1 (API) nor option 2
  (K8s object round-trip) is needed for this use case. The recommendation is "neither, for
  fixed-target consumers" — with the caveat that dynamic-target consumers (metadata collection,
  Cockpit) are a different problem that this spike does not address.
- *Where does the secret stuff end up and how is it secured?* `secretClass` → pod tmpfs via CSI,
  never in etcd. `secret` → the customer's own Secret, so in etcd by their choice. Note that the
  static path is where "how do we protect this data" actually bites.
- *Renewals, expiry, restarts, provider down?* Renewal is the commons-operator restart controller
  plus `requestedSecretLifetime` (step 3) — the volume is never refreshed in place. Restarts are
  free. **Provider down is the weak spot**: the agent cannot start, and because it is the sole writer
  of `ZookeeperZnode` status, nothing reports why. Record this honestly; it is a real property of the
  agent pattern.
- *How much extracts into op-rs?* Answer it from what was actually built, even though extraction is
  out of scope: agent lifecycle (reconcile a cluster into a Deployment with credential volume, SA and
  RBAC), the mode split and `claims` predicate, the condition vocabulary, and the finalizer escape
  hatch. The per-product remainder is "connect to X and CRUD resource Y".

Also worth recording, because they generalise beyond ZooKeeper: secret-operator has no
consumer-authorization concept; ZooKeeper cannot drop port unification until 3.10 ships; and
authentication without ACLs leaves znodes world-writable.

## Out of scope

- Extracting an agent framework into operator-rs.
- Cross-namespace `ZookeeperZnode` support in the agent, and the per-agent `ClusterRoleBinding` it
  would need. Those znodes keep working via the operator; the agent simply does not claim them.
- Multi-CA truststore merge, and therefore `platformAccess` + client-auth `AuthenticationClass`
  together.
- `ZookeeperZnode.spec.consumerPrincipal`. Without it an agent-owned znode is unusable by a consumer
  product, so step 6's restrictive ACL stays gated behind `platformAccess`.
- **Dropping `client.portUnification` — blocked upstream, not a choice.** ZOOKEEPER-4276 is fixed in
  3.10.0 only and 3.10.0 is unreleased. On 3.9.x, `secureClientPort` alone hits the
  NettyServerCnxnFactory double-bind and `clientPort` + `secureClientPort` hits the static/dynamic
  config mismatch. Revisit when 3.10 ships; until then step 6's ACLs are the enforcement mechanism.
