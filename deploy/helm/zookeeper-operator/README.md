# Helm Chart for Stackable Zookeeper Operator

This Helm Chart can be used to install Custom Resource Definitions and the Operator for Zookeeper provided by Stackable.


## Requirements

- Create a [Kubernetes Cluster with Stackable Nodes](../Readme.md)
- Install [Helm](https://helm.sh/docs/intro/install/)


## Install the Zookeeper Operator

```bash
helm install zookeeper-operator ./zookeeper-operator
```


## Test the installed operator

```bash
helm test zookeeper-operator
```

This Helm test will create a Zookeeper Cluster and launch a Pod with kubectl that describes and deletes this cluster.


## Create a Zookeeper Cluster

as described [here](https://docs.stackable.tech/zookeeper/index.html)

```bash
cat <<EOF | kubectl apply -f -
apiVersion: zookeeper.stackable.tech/v1alpha1
kind: ZookeeperCluster
metadata:
  name: simple
spec:
  version: 3.5.8
  servers:
    roleGroups:
      default:
        selector:
          matchLabels:
            kubernetes.io/arch: stackable-linux
        replicas: 1
        config:
          metricsPort: 9505
EOF
```


## Links

https://github.com/stackabletech/zookeeper-operator


