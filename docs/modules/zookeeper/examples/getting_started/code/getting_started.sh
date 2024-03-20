#!/usr/bin/env bash
set -euo pipefail

# This script contains all the code snippets from the guide, as well as some assert tests
# to test if the instructions in the guide work. The user *could* use it, but it is intended
# for testing only.
# The script will install the operator(s), create a product instance and interact with it.

if [ $# -eq 0 ]
then
  echo "Installation method argument ('helm' or 'stackablectl') required."
  exit 1
fi

cd "$(dirname "$0")"

case "$1" in
"helm")
echo "Adding 'stackable-dev' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-stable https://repo.stackable.tech/repository/helm-stable/
# end::helm-add-repo[]
echo "Updating Helm repo"
helm repo update

echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait commons-operator stackable-stable/commons-operator --version 24.3.0
helm install --wait secret-operator stackable-stable/secret-operator --version 24.3.0
helm install --wait listener-operator stackable-stable/listener-operator --version 24.3.0
helm install --wait zookeeper-operator stackable-stable/zookeeper-operator --version 24.3.0
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=24.3.0 \
  secret=24.3.0 \
  listener=24.3.0 \
  zookeeper=24.3.0
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

sleep 15

### Connect to cluster

echo "Awaiting ZooKeeper rollout finish"
# tag::watch-zookeeper-rollout[]
kubectl rollout status --watch --timeout=5m statefulset/simple-zk-server-default
# end::watch-zookeeper-rollout[]

# kubectl run sometimes misses log output, which is why we use run/logs/delete.
# Issue for reference: https://github.com/kubernetes/kubernetes/issues/27264
zkCli_ls() {
# tag::zkcli-ls[]
kubectl run my-pod \
  --stdin --tty --quiet --restart=Never \
  --image docker.stackable.tech/stackable/zookeeper:3.9.2-stackable24.3.0 -- \
  bin/zkCli.sh -server simple-zk-server-default:2282 ls / > /dev/null && \
  kubectl logs my-pod && \
  kubectl delete pods my-pod
# end::zkcli-ls[]
}

ls_result=$(zkCli_ls) >/dev/null 2>&1


if echo "$ls_result" | grep '^\[zookeeper\]' > /dev/null; then
  echo "zkCli.sh ls command worked"
else
  echo "zkCli.sh ls command did not work. command output:"
  echo "$ls_result"
  exit 1
fi

### ZNode

echo "Applying ZNode"
# tag::apply-znode[]
kubectl apply -f znode.yaml
# end::apply-znode[]

sleep 5

ls_result=$(zkCli_ls) > /dev/null 2>&1

if echo "$ls_result" | grep '^\[znode-.\{8\}-.\{4\}-.\{4\}-.\{4\}-.\{12\}, zookeeper\]' > /dev/null; then
  echo "zkCli.sh ls command worked"
else
  echo "zkCli.sh ls command did not work. command output:"
  echo "$ls_result"
  exit 1
fi

get_configmap() {
# tag::get-znode-cm
kubectl describe configmap simple-znode
# end::get-znode-cm
}

cm_output=$(get_configmap)

if [[ $? == 0 ]]; then
  echo "ConfigMap retrieved."
else
  echo "Could not get ConfigMap 'simple-znode'"
  exit 1
fi

if echo "$cm_output" | grep 2282/znode > /dev/null; then
  echo "ConfigMap contains a reference of the ZNode"
else
  echo "ConfigMap doesn't seem to reference the ZNode"
  exit 1
fi

echo "Script ran successfully!"
