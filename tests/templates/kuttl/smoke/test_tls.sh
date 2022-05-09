#!/bin/bash

NAMESPACE=$1
# test the plaintext unsecured connection
COMMAND="/stackable/zookeeper/bin/zkCli.sh -server test-zk-server-primary-1.test-zk-server-primary.${NAMESPACE}.svc.cluster.local:2282 ls / >> file.log"
$COMMAND

if [[ $? != 0 ]];
then
  echo "Could not establish unsecure connection..."
  exit 1
fi
echo "Unsecure client connection established successfully!"

# we set the correct client tls credentials and expect to be able to connect
export CLIENT_STORE_SECRET=$(cat /stackable/rwconfig/zoo.cfg | grep "ssl.keyStore.password" | cut -d "=" -f2)
export CLIENT_JVMFLAGS="
-Dzookeeper.authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider
-Dzookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty
-Dzookeeper.client.secure=true
-Dzookeeper.ssl.keyStore.location=/stackable/tls/client/keystore.p12
-Dzookeeper.ssl.keyStore.password=${CLIENT_STORE_SECRET}
-Dzookeeper.ssl.trustStore.location=/stackable/tls/client/truststore.p12
-Dzookeeper.ssl.trustStore.password=${CLIENT_STORE_SECRET}"

$COMMAND

if [[ $? != 0 ]];
then
  echo "Could not establish secure connection..."
  exit 1
fi
echo "Secure and authenticated client connection established successfully!"

# We set the (wrong) quorum tls credentials and expect to fail (wrong certificate)
export QUORUM_STORE_SECRET=$(cat /stackable/rwconfig/zoo.cfg | grep "ssl.quorum.keyStore.password" | cut -d "=" -f2)
export CLIENT_JVMFLAGS="
-Dzookeeper.authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider
-Dzookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty
-Dzookeeper.client.secure=true
-Dzookeeper.ssl.keyStore.location=/stackable/tls/quorum/keystore.p12
-Dzookeeper.ssl.keyStore.password=${QUORUM_STORE_SECRET}
-Dzookeeper.ssl.trustStore.location=/stackable/tls/quorum/truststore.p12
-Dzookeeper.ssl.trustStore.password=${QUORUM_STORE_SECRET}"

$COMMAND

if [[ $? == 0 ]];
then
  echo "Could establish secure connection with wrong certificates... this should not work!"
  exit 1
fi

echo "TLS test successful!"
