# Helm Chart for Stackable Operator for Apache ZooKeeper

This Helm Chart can be used to install Custom Resource Definitions and the Operator for Apache ZooKeeper provided by Stackable.

## Requirements

- Create a [Kubernetes Cluster](../Readme.md)
- Install [Helm](https://helm.sh/docs/intro/install/)

## Install the Stackable Operator for Apache ZooKeeper

```bash
# From the root of the operator repository
make compile-chart

helm install zookeeper-operator deploy/helm/zookeeper-operator
```

## Usage of the CRDs

The usage of this operator and its CRDs is described in the [documentation](https://docs.stackable.tech/zookeeper/index.html)

The operator has example requests included in the [`/examples`](https://github.com/stackabletech/zookeeper-operator/tree/main/examples) directory.

## Links

https://github.com/stackabletech/zookeeper-operator

