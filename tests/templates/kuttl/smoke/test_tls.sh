#!/usr/bin/env bash
# Usage: test_tls.sh namespace

NAMESPACE=$1
SERVER="test-zk-server-primary-1.test-zk-server-primary.${NAMESPACE}.svc.cluster.local:2282"

echo "Start TLS testing..."
############################################################################
# test the plaintext unsecured connection
############################################################################
/stackable/zookeeper/bin/zkCli.sh -server "${SERVER}" ls / &> /dev/null

if [[ $? != 0 ]];
then
  echo "[ERROR] Could not establish unsecure connection!"
  exit 1
fi
echo "[SUCCESS] Unsecure client connection established!"

############################################################################
# we set the correct client tls credentials and expect to be able to connect
############################################################################
export CLIENT_STORE_SECRET=$(cat /stackable/rwconfig/zoo.cfg | grep "ssl.keyStore.password" | cut -d "=" -f2)
export CLIENT_JVMFLAGS="
-Dzookeeper.authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider
-Dzookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty
-Dzookeeper.client.secure=true
-Dzookeeper.ssl.keyStore.location=/stackable/tls/client/keystore.p12
-Dzookeeper.ssl.keyStore.password=${CLIENT_STORE_SECRET}
-Dzookeeper.ssl.trustStore.location=/stackable/tls/client/truststore.p12
-Dzookeeper.ssl.trustStore.password=${CLIENT_STORE_SECRET}"
/stackable/zookeeper/bin/zkCli.sh -server "${SERVER}" ls / &> /dev/null

if [[ $? != 0 ]];
then
  echo "[ERROR] Could not establish secure connection using client certificates!"
  exit 1
fi
echo "[SUCCESS] Secure and authenticated client connection established!"

############################################################################
# We set the (wrong) quorum tls credentials and expect to fail (wrong certificate)
############################################################################
export QUORUM_STORE_SECRET=$(cat /stackable/rwconfig/zoo.cfg | grep "ssl.quorum.keyStore.password" | cut -d "=" -f2)
export CLIENT_JVMFLAGS="
-Dzookeeper.authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider
-Dzookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty
-Dzookeeper.client.secure=true
-Dzookeeper.ssl.keyStore.location=/stackable/tls/quorum/keystore.p12
-Dzookeeper.ssl.keyStore.password=${QUORUM_STORE_SECRET}
-Dzookeeper.ssl.trustStore.location=/stackable/tls/quorum/truststore.p12
-Dzookeeper.ssl.trustStore.password=${QUORUM_STORE_SECRET}"

/stackable/zookeeper/bin/zkCli.sh -server ${SERVER} ls / &> /dev/null

if [[ $? == 0 ]];
then
  echo "[ERROR] Could establish secure connection with quorum certificates (should not be happening)!"
  exit 1
fi
echo "[SUCCESS] Could not establish secure connection with (wrong) quorum certificates!"

echo "All TLS tests successful!"
exit 0