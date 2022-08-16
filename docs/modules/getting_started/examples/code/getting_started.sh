#!/usr/bin/env bash
set -euo pipefail

# This script contains all the code snippets from the guide, as well as some assert tests
# to test if the instructions in the guide work. The user *could* use it, but it is intended
# for testing only.
# The script will install the operator(s), create a product instance and interact with it.
# No running processes are left behind (i.e. the port-forwarding is closed at the end)

if [ $# -eq 0 ]
then
  echo "Installation method argument ('helm' or 'stackablectl') required."
  exit 1
fi

case "$1" in
"helm")
echo "Adding 'stackable-dev' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-dev https://repo.stackable.tech/repository/helm-dev/
# end::helm-add-repo[]
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait commons-operator stackable-dev/commons-operator --version 0.3.0-nightly
helm install --wait secret-operator stackable-dev/secret-operator --version 0.6.0-nightly
helm install --wait zookeeper-operator stackable-dev/zookeeper-operator --version 0.11.0-nightly
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=0.3.0-nightly \
  secret=0.6.0-nightly \
  zookeeper=0.11.0-nightly
# end::stackablectl-install-operators[]
;;
*)
echo "Need to give 'helm' or 'stackablectl' as an argument for which installation method to use!"
exit 1
;;
esac

echo "Creating ZooKeeper cluster"
# tag::install-zookeeper[]
kubectl apply -f zookeeper.yaml
# end::install-zookeeper[]

sleep 5

### Connect to cluster

echo "Awaiting ZooKeeper rollout finish"
# tag::watch-zookeeper-rollout[]
kubectl rollout status --watch statefulset/simple-zk-server-default
# end::watch-zookeeper-rollout[]

zkCli_ls() {
# tag::zkcli-ls[]
kubectl run my-pod \
  --stdin --tty --quiet --rm --restart=Never \
  --image docker.stackable.tech/stackable/zookeeper:3.8.0-stackable0.7.1 -- \
  bin/zkCli.sh -server simple-zk-server-default:2282 ls /
# end::zkcli-ls[]
}

if zkCli_ls | grep '^\[zookeeper\]$'; then
  echo "works"
else
  echo "doesn't work"
fi

### ZNode

echo "Applying ZNode"
# tag::apply-znode[]
kubectl apply -f znode.yaml
# end::apply-znode[]

sleep 5

if zkCli_ls | grep '^\[znode-.\{8\}-.\{4\}-.\{4\}-.\{4\}-.\{12\}, zookeeper\]$'; then
  echo "works"
else
  echo "doesn't work"
fi
