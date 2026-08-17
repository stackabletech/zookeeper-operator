# Znode-agent spike — progress handoff

Autonomous work session against `znode-agent-prototype-plan.md`.

**Status: the crate compiles clean, all 51 unit tests pass, and `cargo run -- crd` produces a valid
CRD** (the `credential` enum renders as `oneOf: [{required:[secretClass]}, {required:[secret]}]`).
Build via the direct stable toolchain + a writable `CARGO_HOME` (see the `cargo-build-sandbox-workaround`
memory) until a session restart makes the `~/.cargo/git` grant live. The compiler shook out ~11 small
fixes from the first blind draft (WatchNamespace path, `crate::ObjectRef` re-export, `Port` clone,
`FromStr` for the `constant!` macro, `RoleRef.api_group: Option`, a `+ Sync` bound, and test fixtures
that set `platformAccess` in Rust because serde_yaml can't represent the externally-tagged enum).

## Environment blockers (why some steps are deferred, not skipped)

- **Compilation is blocked.** cargo can't write `~/.cargo/git` (only `~/.cargo/registry` is writable),
  `~/.rustup/tmp` is read-only (the rustup proxy tries to sync the pinned `1.95.0` toolchain), and
  network is blocked, so the git dependency `stackable-operator` can't be fetched. The
  `dangerouslyDisableSandbox` override did not reliably apply. To unblock, restart the session with
  the paths granted **and** network, e.g.:
  `nono run --allow /home/benedikt/.cargo --allow /home/benedikt/.rustup -- claude` (plus network).
  Then build with the direct stable toolchain to avoid re-downloading 1.95.0:
  `RUSTUP_TOOLCHAIN=stable-x86_64-unknown-linux-gnu PATH="$HOME/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin:$PATH" cargo check`
- **The kind cluster is blocked** (apiserver on 127.0.0.1 refused by the sandbox), so **Step 0's
  experiment and all kubectl/Tilt/kuttl verification are deferred** to a session with cluster access.

## What was implemented (code-complete, unverified)

| Step | What | Files |
|---|---|---|
| 1 | Decouple znode path from `ZookeeperSecurity` → carries `client_port: Port` | `crd/security.rs` (`client_port()` fn), `znode_controller/{dereference,validate}.rs`, `znode_controller.rs`, `zk_controller/build/resource/discovery.rs` |
| 4 | CRD `clusterConfig.platformAccess` (trust anchor + credential enum), truststore split, `ssl.clientAuth=need`, validation | `crd/platform_access.rs` (new), `crd/mod.rs`, `crd/security.rs`, `zk_controller/validate.rs` |
| 2 | Agent mode + ownership split (`Mode`, `Mode::claims` + table test), controller-chain extraction, custom `Command` enum + `AgentArguments` | `znode_controller/mode.rs` (new), `znode_controller/run.rs` (new), `znode_controller.rs`, `main.rs` |
| 3 | Agent Deployment + SA + RoleBinding, distinct `znode-agent` role, restart-controller label, credential volume (CSI *or* static Secret), RBAC | `zk_controller/build/resource/znode_agent.rs` (new), `zk_controller/{build.rs,build/resource/mod.rs}`, `zk_controller.rs`, `deploy/helm/.../clusterrole-znode-agent.yaml` (new), `clusterrole-operator.yaml` |
| 4b | *Partial:* `ZookeeperZnodeStatus.conditions` + `HasStatusCondition for ZookeeperZnode` | `crd/mod.rs` |
| 6 | x509 ACL from the agent principal (else `world:anyone`) | `znode_controller.rs` (`desired_acl`, `ensure_znode_exists`) |

## Deviations from the plan (deliberate, flagged)

1. **`main.rs` uses its own `Command` enum** (`Crd` / `Run(ZookeeperRunArguments)` / `Agent`) instead of
   the plan's `FrameworkCommand<ZookeeperRunArguments>` extension point. I could not verify that
   generic exists in operator-rs 0.114 and cannot compile, so I reused the already-working inline
   `Crd`/`Run` handling and added `Agent` alongside. If `FrameworkCommand` does exist, switching back
   is mechanical.
2. **The agent's x509 principal is a `--platform-access-principal` flag, not read from the cert.** The
   plan wants the DN read from the mounted cert's subject at startup — but (a) the *exact* DN string
   ZooKeeper derives is what Step 0's (blocked) experiment pins down, and (b) no X.509 parser is
   vendored (adding one needs `make regenerate-nix`, also blocked). A flag keeps the demo
   deterministic: after Step 0, set `PLATFORM_ACCESS_PRINCIPAL` on the agent Deployment. `--platform-access-cert-dir`
   is still passed (the mTLS client in Step 5 reads `tls.crt`/`tls.key` from it).

## Not done / deferred

- **Step 4b — DONE (compiles + tests pass).** `ZnodeConditionBuilder` (`znode_controller/condition.rs`)
  maps the znode's provisioning state onto `Available`/`Degraded` with a distinguishing `reason`; the
  Apply path writes `Provisioned` on success and `Degraded{reason = error category, message = error}`
  on failure, via `compute_conditions` + `merge_patch_status`. The status fields got
  `skip_serializing_if` so the `znode_path` and `conditions` writers don't null each other out.
- **Step 4c — DONE (compiles + tests pass).** The agent renews a `coordination.k8s.io` Lease
  (`znode_controller/lease.rs`, spawned via `futures::join!` in the agent path; 10 s period, 30 s
  duration). `Mode::claims` became a 3-way `Mode::disposition`
  (`Reconcile`/`Ignore`/`ReportAgentLiveness`): for a platformAccess znode the operator checks
  `is_agent_alive` and, when the lease is stale/absent, writes an `AgentUnavailable` condition and
  requeues one lease-period to keep polling (no Lease watch needed — which also avoids giving the
  agent list/watch RBAC). Deferred sub-part: operator-side SecretClass pre-validation (nice-to-have).
- **Step 5 (mTLS client) — DONE (compiles + tests pass, not yet run against a live ZK).** Fork in
  `~/stackable/tokio-zookeeper`: a `ZooKeeperTransport` impl for tokio-rustls `TlsStream` (the
  `ClientConfig`+`ServerName` live in `Addr` so reconnects reuse them), a generic `handshake`, a
  `connect_tls`, all behind a `tls` feature (tokio-rustls 0.26 / ring). Operator: `[patch.crates-io]`
  points at the local path, `rustls` added, `tls` feature enabled, and `znode_mgmt` builds a
  `ClientConfig` from the mounted `tls.crt`/`tls.key`/`ca.crt` and calls `connect_tls`. Caveats:
  (1) untested against a live auth-enabled ZooKeeper (needs the cluster); (2) the agent trusts the ZK
  *server* cert against the mounted `ca.crt`, which only validates it when the platform trust-anchor CA
  also signs the server cert — true for the autoTls variant (the primary demo), but the dedicated/static
  variants would need the server CA supplied separately; (3) the nix/docker image build needs the fork
  pushed to a git branch + the git `[patch]` + `make regenerate-nix` (nix isn't available in this
  session), so the local `path` patch only works for `cargo`-based dev, not the Tilt image build.
- **`make crds` / `make regenerate-charts` / `make regenerate-nix`** — all need cargo; the generated
  CRD YAML and `Cargo.nix` are therefore stale. Run them once the build works.

## Verified

- `cargo check --workspace` — clean.
- `cargo test --bins` — 51 passed, 0 failed (incl. the `Mode::claims` table test and the
  `znode_agent` name/RBAC-ref/selector-disjointness tests).
- `cargo run -- crd` — CRD generates; `platformAccess`/`credential` schema is correct.

## The demo — now a kuttl test

The demonstration lives in `tests/templates/kuttl/platform-access/` (added to `test-definition.yaml`
as the `platform-access` test, dims `zookeeper-latest` × `openshift`). It installs a `platformAccess`
cluster (one SecretClass backing server + trust anchor + credential), asserts the ZK ensemble **and**
the `test-zk-znode-agent` Deployment come up, reads the agent's cert subject DN and configures it,
creates a `ZookeeperZnode`, and finally execs `zkCli` from a server pod to assert a **plaintext read
of the agent-owned znode is denied with `NoAuth`** — the security property. Run it with the usual
`kubectl kuttl` / `beku` flow once a cluster is reachable; it doubles as the end-to-end check for
Step 5 (mTLS) and the exact-DN question from Step 0 (the `20-configure-agent-principal` step is the
single place to adjust the DN format if `openssl -nameopt RFC2253` differs from what ZooKeeper
derives).

## Still to do

1. **`make regenerate-nix`** (`Cargo.nix` is now stale — `Cargo.toml` changed: `rustls` added + the
   `tokio-zookeeper` `[patch]`) and **`make regenerate-charts`** (CRD into the chart). Both need
   tooling absent from this sandbox (`nix` / `yq`). Also push the fork to a git branch and switch the
   `[patch]` from the local `path` to `git` for a reproducible/docker build.
2. Run the `platform-access` kuttl test against a cluster (blocked here: the kind apiserver is
   unreachable — proxy-only network in this nono profile denies the raw loopback dial).
3. The remaining deferred controller logic (4b write-path, 4c lease) — now iterable against a working
   compile/test loop.
