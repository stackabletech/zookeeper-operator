# Spike findings: per-cluster `ZookeeperZnode` agent

Throwaway spike (`znode-agent-prototype-plan.md`) — the "write down the answers" half of the issue.
Companion to `SPIKE-PROGRESS.md` (what was built) and `access-delegation-session-notes.md` (the fuller
option analysis). Run against auth-enabled ZooKeeper **3.9.5** in kind, 2026-08-11/12.

> **Read §2 first for what is actually demonstrated vs. only built.** The rest of the document
> distinguishes *verified live*, *built but unverified*, and *reasoned/proposed*.

## 1. Approach and result

`zookeeper-operator` can't provision znodes against an auth-enabled ZooKeeper. It works today only
because `client.portUnification=true` is unconditionally on, so **the operator connects in plaintext**
— there is no credential.

**Where should the provisioning run?** The options differ by *whose pod runs the code*; each inherits
that pod's credential exposure. Compared across the dimensions that actually decided it (the
approach-by-approach matrix with two more options — consumer initContainer, `pods/exec` — is in the
session notes §2):

| Option | Credential exposure | Cost scales with | Upgrade of provisioning logic | Deletion path | Verdict |
|---|---|---|---|---|---|
| Central operator | all clusters, operator lifetime (etcd + memory) | clusters | restart operator | existing escape hatch | ✗ needs a runtime credential-vending layer, and makes every product operator a long-lived credential holder |
| Central `sdp-proxy` (one shared proxy in front of every product) | every credential, in the proxy; operators on every request path | — (per request) | restart proxy | n/a | ◐ shines for HTTP+header products (just inject a header); but not standalone — operators still originate every request — and it re-concentrates every credential centrally (the aggregation shape customers rejected) |
| Product-pod sidecar | one cluster (localhost) | product replicas | **rolls the customer's Kafka/NiFi** ❌ | dies with cluster | ✗ release coupling; "credential already there" only holds for transport-auth products, i.e. ZK alone |
| Job per action / reconciliation | one action, seconds | **resources** ❌ | next Job | **blocked in a `Terminating` ns** ❌ | ✗ cost scales with the resource proliferation that is the whole point; can't clean up on delete |
| **Per-cluster agent** | one cluster, pod lifetime (tmpfs via CSI) | clusters | restart small Deployment | agent must outlive last CR | ✅ **chosen** |

**Chosen — a per-cluster agent** (Strimzi's Entity Operator shape): the same binary in `agent` mode,
credential mounted at pod start, target fixed at creation.

- Level-triggered; cost scales with clusters; upgrades = restart a small Deployment.
- The "vend a credential at runtime" requirement (the paradigm's Layer 2) *dissolves* once the target
  is fixed at creation — for **fixed-target** consumers (§3.1).
- One-line: **copy Strimzi's topology, fix its trust model** (§3.1, §4.1).

## 2. What is verified vs. built-but-unverified

**Verified live (3.9.5):**

- The agent authenticates over mTLS on `secureClientPort` (`ssl.clientAuth=need`); an unauthenticated
  connect is rejected.
- A *plaintext* `zkCli get` of the agent-owned znode returns `Insufficient permission` (a `NoAuth`
  denial), while port unification still accepts the plaintext connection. ⇒ **ACLs, not the port,
  gate access on 3.9.x.**
- The znode is created with an `x509` ACL for the agent's principal (a flag, §7).
- The agent writes `Available=True` on the `ZookeeperZnode` (§4 conditions).
- **Cross-CA mTLS** (test ②, `platform-access-cross-ca`): server cert from `serverSecretClass`
  (CA-A), agent client cert + server truststore from `trustAnchorSecretClass` (CA-B). The agent
  verifies the server against CA-A, mounted separately at `--platform-access-server-ca-dir`, while
  presenting a client cert from CA-B — the two directions of mTLS chain to *different* CAs. This
  required a fix: the agent previously verified the server against its own credential CA (CA-B),
  which only works when the CAs coincide. Both single-CA (`platform-access`) and cross-CA tests pass.
- Operator + fork build clean; 51 unit tests pass; CRD generates.

**Built but NOT verified (and why):**

| Feature | Why unverified |
|---|---|
| Static `secret` credential source | Only `secretClass` was run — so the issue's core "two sources, **identical behaviour**" is only half-proven (the *code* is identical by construction) |
| Liveness lease → `AgentUnavailable` (§4.5) | The lease **failed to create** live (missing `patch` verb, fixed in source); the operator staleness→condition path was never observed |
| Cert renewal / restart-controller roll | Can't be exercised in a short run (secret-operator's ~6h floor, §4.3); the agent's renewal was not observed |
| `Degraded` condition *reasons* | Only `Available` was observed; the failure paths weren't forced |
| `#708` / SA-based identity (§5) | Analysis only — never wired into the running spike |
| Finalizer teardown of a platformAccess cluster | Reasoned from the existing `ZkDoesNotExist` path, not run |

The test plan to close these is §9.

## 3. Design decisions & the four open questions

### 3.1 — Two deviations from the issue, stated as findings

**"Mounting a PVC at pod start doesn't work" — the spike does exactly that, deliberately.**

- The runtime-vending requirement is an artefact of *where the code runs*: it only binds because the
  central operator is long-lived with a changing target set.
- Fix the target at pod-creation (one agent per cluster) and it dissolves — no vending API, provider,
  or transport.
- Holds only for **fixed-target** consumers; dynamic-target ones (metadata collection, Cockpit) are a
  different, unsolved problem.

**Trust and identity are split; the autoTls CA is not avoided.**

- The perceived backdoor was implicitness: `get_tls_secret_class()` uses one SecretClass for keystore
  *and* truststore, so ZooKeeper trusts its own server CA by construction.
- `platformAccess.trustAnchorSecretClass` is a separate, optional, explicit field feeding
  `ssl.trustStore.location` with `ssl.clientAuth=need`. Pointing it at `tls` is then a written,
  deletable choice.
- `credential` is a second field because a customer static cert has no SecretClass CA to expose.
- **Symmetric on the client side (the fix behind test ②):** the agent verifies the ZK *server*
  against the `serverSecretClass` CA — mounted separately at `--platform-access-server-ca-dir` — not
  against its own credential CA. So the server-identity CA and the platform trust anchor are fully
  independent: the two mTLS directions can chain to different CAs (server keystore = CA-A; server
  truststore + agent credential = CA-B; agent's server-trust anchor = CA-A). Verifying the server
  against the credential CA (the pre-fix behaviour) only worked when the two happened to coincide.

### 3.2 — The four open questions

- **Request/response shape?** None. Neither a vending API nor a K8s-object round-trip is needed for
  fixed targets — recommend *neither*. (Dynamic targets: still open, §3.1.)
- **Where does secret material land / how secured?**
  - `credential.secretClass` → pod tmpfs via CSI, never etcd.
  - `credential.secret` → the customer's own Secret, i.e. etcd by their choice (where "how do we
    protect this" actually bites).
  - Both resolve to identical `tls.crt`/`tls.key`, so the agent code is byte-identical across sources
    **by construction**. (The `secret` path itself is unverified — §2, §9.)
- **Renewal / expiry / restart / provider down?**
  - Renewal = commons-operator restart controller + `requestedSecretLifetime` (CSI volumes never
    refresh in place; the agent Deployment carries `RESTART_CONTROLLER_ENABLED_LABEL`). *Mechanism, not
    yet exercised — §2.*
  - **Provider-down is the weak spot**: the agent can't start, and it's the sole writer of
    `ZookeeperZnode` status — the liveness lease (§4.5) is meant to cover it, *also unverified*.
- **op-rs extraction surface?** Agent lifecycle (cluster → Deployment + credential volume + SA +
  RBAC), the mode/ownership predicate, the condition vocabulary, the lease/staleness mechanism, the
  finalizer escape-hatch. Per-product remainder: "connect to X, CRUD Y".

## 4. Findings that need a decision

### 4.1 — autoTls subjects are generic, so x509 ACLs are not an identity *(verified)*

- Every autoTls cert has subject `CN=generated certificate for pod`, so `x509:CN=generated certificate
  for pod` authorises *any* holder of a cert from that CA (ZK servers included).
- ZK's default `X509AuthenticationProvider` keys on `getSubjectX500Principal().getName()` — the subject
  **DN** — and ignores the SANs where secret-operator already puts pod identity.
- ⇒ The ACL isn't a workload identity. The fix is a secret-operator capability — **§5**.

### 4.2 — secret-operator has no consumer-authorization concept *(verified from source)*

- `SecretClassSpec` carries only a `backend`; no `allowedNamespaces`, nothing.
- Any pod that can mount the right annotation gets a cert from *any* SecretClass, so a dedicated
  platform SecretClass is no more access-controlled than `tls` — it only narrows which certs ZooKeeper
  *accepts*.
- The **ACL is the primary control** everywhere. Issuance scoping = a secret-operator FR.

### 4.3 — `requestedSecretLifetime` below the restart buffer (~6h) is rejected *(verified)*

- Symptom: server pods stuck `FailedMount`, `certificate expiring at … would schedule the pod to be
  restarted … in the past`.
- So the plan's "short lifetime to observe renewal in-test" is infeasible; keep lifetimes ≥ ~1d.

### 4.4 — port unification can't be dropped before 3.10 *(verified)*

- `client.portUnification=true` works around
  [ZOOKEEPER-4276](https://issues.apache.org/jira/browse/ZOOKEEPER-4276), fixed only in the unreleased
  3.10 (3.9.5 current).
- On 3.9.x, `secureClientPort` alone hits the NettyServerCnxnFactory double-bind; `clientPort` +
  `secureClientPort` hits the static/dynamic-config mismatch.
- Hence ACLs are the enforcement mechanism, and auth *without* ACLs leaves znodes world-writable — the
  hardcoded `world:anyone` was the actual hole.

### 4.5 — provider-down needs an asserted liveness signal, not an inferred one *(built, unverified)*

- Deployment readiness is the wrong signal: false positives mid-rollout, false negatives for a
  running-but-wedged agent.
- Built: the agent renews a `coordination.k8s.io` Lease; the operator treats staleness beyond the lease
  duration as "no agent" and writes `AgentUnavailable` on the bound znodes.
- Not yet proven end-to-end — see §2 and §9.

## 5. Proposal — secret-operator workload-identity certs *(reasoned; not built)*

Follows directly from §4.1. secret-operator should mint the subject DN from a **verified pod
attribute**, so the DN is a real identity to ACL on. Two flavours, split by *role*:

| Verified attribute → DN | Granularity | Role | Examples | Status |
|---|---|---|---|---|
| Pod FQDN | per-pod (StatefulSet-stable) | product member / node | ZK servers; distributed members | landed in [#708](https://github.com/stackabletech/secret-operator/pull/708) |
| ServiceAccount | per-workload (restart-stable, Deployment-ok) | SDP agent / control-plane actor | znode agent, a future nifi-agent | **proposed** |

- **#708** appends the pod FQDN as domain components (`…DC=<pod>,DC=<svc>,DC=<ns>,…`) from the
  CSI-resolved Pod — unique and non-forgeable, but **per-pod**, so stable only for StatefulSets.
- The znode **agent runs as a Deployment** (random, restart-changing pod names), and ZK matches the
  full DN — so a #708 ACL would break on every renewal restart. Keying on the **ServiceAccount**
  instead is restart-stable and Deployment-friendly, and the agent already gets a dedicated
  `<cluster>-znode-agent` SA.
- A single product can use both: e.g. NiFi authorizes on cert DNs, so its *nodes* would take per-pod
  DNs while a *nifi-agent* used its SA identity. *(NiFi's authz model is inferred, not verified here.)*

**The extension (in the #708 shape):** add `service_account_name` to `PodInfo`, a new opt-in
annotation, write the SA into the subject DN. The one real decision is the DN **encoding** (CN vs O vs
DC; optionally a SPIFFE URI SAN for non-DN consumers) — a `stackabletech/decisions` entry, as #708
gated on decisions#81. **Not** caller-chosen subjects: those are forgeable impersonation, and audit is
meaningless on a shared DN.

**Why it matters:** the identity is what the product authorizes. An SA-derived DN makes that a durable,
one-time grant instead of a re-grant on every restart, and is the foundation for keying authorization
(and `consumerPrincipal`) on a verified SA — the strongest argument for solving it once in
secret-operator rather than per-product.

## 6. Transport recommendation *(built, verified live)*

**Fork `stackabletech/tokio-zookeeper` for mTLS.**

- `impl ZooKeeperTransport for tokio_rustls::client::TlsStream<TcpStream>` with
  `Addr = (SocketAddr, Arc<ClientConfig>, ServerName)` — the config must live in `Addr` because
  `Packetizer` re-invokes `connect(addr)` on reconnect.
- Make `ZooKeeperBuilder::{connect,handshake}` generic (the trait was already generic; only those two
  pinned `TcpStream`).

Reasons: no new heavy deps (`rustls` 0.23 + `tokio-rustls` 0.26 already via `kube-client`; PEM via
`rustls-pki-types`' `PemObject`); contained (`ZooKeeper`/`Enqueuer` stay non-generic); proven live;
reconnects reuse the `ClientConfig`, so rotation is a pod roll, not a transport concern.

Caveat: the operator currently `[patch]`es the fork to a local path (fine for `cargo`/Tilt, not
CI/nix) — push the branch and use a `git` patch for a real build.

## 7. Gaps & deviations

- **`ensure_znode_exists` doesn't reconcile an existing node's ACL** (early-returns on `NodeExists`) —
  a node created world-writable first would never be upgraded.
- **Status writes must be idempotent** — writing `status.conditions` every reconcile re-fires the
  primary watch → reconcile storm. Patch only on change. *(observed + fixed)*
- **The lease renews via SSA (HTTP PATCH)** — the agent ClusterRole needs `patch` on `leases`, not just
  `create`/`get`/`update`. *(observed + fixed)*
- **`kubectl auth can-i --as=<sa>` gave false negatives** here — read the operator's own error instead.
- **A kuttl `commands`-only step whose file is named `NN-assert-*` must be `kind: TestAssert`, not
  `kind: TestStep`** — otherwise kuttl treats the assert-named file as an object and polls for a
  nonexistent `TestStep` resource until the step times out (a silent 300s hang). `TestAssert` also
  retries the script until it exits 0, which absorbs the znode-provisioning race. *(observed + fixed in
  both platform-access tests)*
- **Deliberate plan deviations:**
  - `main.rs` uses its own `Command` enum, not `FrameworkCommand` (couldn't confirm the generic).
  - The agent principal is a **flag** set from the cert via `openssl` in the test rather than parsed
    in-process (no X.509 parser vendored; §5 is the value it should ultimately carry).

## 8. Out of scope

- op-rs framework extraction; cross-namespace znodes from the agent; multi-CA truststore *merge* — the
  server trusting **two** client CAs at once (`platformAccess` + a client-auth `AuthenticationClass`
  together); `consumerPrincipal`; dropping `client.portUnification`.
- Note the merge is distinct from **cross-CA mTLS**, which *does* work (§2, §3.1): there the server
  identity CA and the single client-trust CA simply differ — no merge needed.

## 9. Test plan — closing the unverified items

Ordered by value. Each extends the `platform-access` kuttl test or adds a sibling.

**P0 — the issue's core constraint (#1, open) and the cross-CA gap (#2, now closed)**

1. **Static `secret` source is behaviourally identical.** Add a `kubernetes.io/tls` Secret (cert-manager
   or hand-rolled), switch `credential` to `secret: <name>`, re-run the exact same assertions (agent
   authenticates, znode provisioned, plaintext read → NoAuth). *Pass = the issue's "two sources, one
   code path" is proven, not just asserted.*
2. **Cross-CA mTLS.** ✅ **Done** — `platform-access-cross-ca` kuttl test (passes). Two SecretClasses:
   `zk-server-tls` (CA-A) for `serverSecretClass`, `zk-platform-tls` (CA-B) for `trustAnchorSecretClass`
   + `credential.secretClass`. The initial run surfaced the predicted gap — the agent validated the
   server against its own credential CA (B) and would fail when B ≠ A — so the agent was **fixed** to
   verify the server against the server SecretClass CA, mounted separately at
   `--platform-access-server-ca-dir` (server-CA volume in the agent Deployment). The two mTLS directions
   now chain to independent CAs and the test asserts the full agent-provisions + plaintext-NoAuth flow.

**P1 — the built-but-unproven controller behaviour**

3. **Liveness lease end-to-end** (after the `patch`-verb fix). Assert the `<cluster>-znode-agent` Lease
   exists and `renewTime` advances. Then kill the agent (`kubectl scale deploy/<agent> --replicas=0`)
   and assert the operator writes `Degraded/AgentUnavailable` on the znode within ~1 lease period; scale
   back up and assert it clears.
4. **Finalizer teardown.** `kubectl delete namespace <ns>` on a live platformAccess cluster + agent +
   znode; assert it completes (the operator fallback removes the znode finalizer once the cluster — and
   its owner-referenced agent — is gone). Guards against the stranded-finalizer hang.
5. **`Degraded` reasons are distinguishable.** Force each: typo'd `trustAnchorSecretClass`; missing
   static `secret`; a cert ZooKeeper rejects (wrong CA). Assert the condition `reason` differs and is
   legible on the `ZookeeperZnode`.

**P2 — longer / identity**

6. **Cert renewal / restart roll.** On a long-running cluster, set `requestedSecretLifetime` just above
   the ~6h floor (e.g. `7h`); over hours, assert commons-operator rolls the agent pod before expiry and
   the agent reconnects with the new cert. Multi-hour, out-of-band from CI.
7. **`#708` pod-FQDN identity (interim, if pursued before §5).** Opt into
   `domain-components-in-subject-dn`, run the agent as a 1-replica StatefulSet, ACL on the full DN,
   restart the pod, assert the ACL still matches. Confirms the StatefulSet path and motivates the SA
   extension.
8. **Operator fallback / `claims`.** A non-platformAccess cluster's znode is still provisioned in
   plaintext by the operator; a cross-namespace znode to a platformAccess cluster stays with the
   operator (agent doesn't claim it). The live counterpart to the existing `disposition` unit test.

**Not automatable here:** anything at hour-scale (#6) runs out-of-band; everything else is kuttl-shaped
and belongs in the `platform-access` suite (#2 is the `platform-access-cross-ca` sibling; #1 would be a
`platform-access-static` sibling).
