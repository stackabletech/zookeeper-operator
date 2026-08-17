#!/usr/bin/env bash
# Usage: test_platform_access.sh <namespace> <znode-path>
#
# The spike demonstration. With platformAccess configured, a *plaintext* zkCli connection still
# succeeds (client.portUnification is on — it cannot be removed before ZooKeeper 3.10), but it must
# NOT be able to read the agent-owned znode: the agent created that node with an x509 ACL scoped to
# its own principal, and an unauthenticated session carries no auth ids, so ZooKeeper answers NoAuth.
#
# This is the pair that proves credentials — not the port — gate access on 3.9.x. Contrast with
# smoke/test_tls.sh, which asserts the *opposite* (an unsecured `ls /` succeeds); here we assert an
# unsecured read of the protected node is denied.
set -uo pipefail

NAMESPACE="$1"
ZNODE="$2"
SERVER="test-zk-server.${NAMESPACE}.svc.cluster.local:2282"

# Make sure no TLS client credentials leak in from the environment.
unset CLIENT_JVMFLAGS CLIENT_STORE_SECRET QUORUM_STORE_SECRET

echo "Reading ${ZNODE} over a plaintext (unauthenticated) connection; expecting a NoAuth denial..."
OUTPUT=$(/stackable/zookeeper/bin/zkCli.sh -server "${SERVER}" get "${ZNODE}" 2>&1)
echo "${OUTPUT}"

# zkCli surfaces the KeeperException NoAuth as either "NoAuth"/"KeeperErrorCode = NoAuth" or, in
# 3.9.x, the message "Insufficient permission". Match any of them.
if echo "${OUTPUT}" | grep -qiE "NoAuth|Insufficient permission|Authentication is not valid"; then
  echo "[SUCCESS] Plaintext read of the agent-owned znode was denied — credentials gate access."
  exit 0
fi

# A world:anyone ACL (i.e. the agent ran without a principal) would let the read succeed — in which
# case the feature is not actually gating access and the test must fail loudly.
if echo "${OUTPUT}" | grep -q "cZxid"; then
  echo "[ERROR] Plaintext read succeeded — the znode is world-readable (agent principal not applied)."
else
  echo "[ERROR] Unexpected zkCli output — connection problem rather than an ACL denial?"
fi
exit 1
