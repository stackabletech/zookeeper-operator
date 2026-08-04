<!-- markdownlint-disable MD034 -->
# Helm Chart for Stackable Operator for Apache ZooKeeper

Kubernetes operator for Apache ZooKeeper. Deploy and run ZooKeeper ensembles with the Stackable Data Platform (SDP).

## Requirements

- A running Kubernetes cluster
- [Helm](https://helm.sh/docs/intro/install/) 3.8 or newer, for OCI support

## Install

```bash
helm install zookeeper-operator oci://oci.stackable.tech/sdp-charts/zookeeper-operator
```

Add `--version` to pin a release, for example `--version 26.7.0`.
Released versions are listed in the [SDP release notes](https://docs.stackable.tech/home/stable/release-notes/) and on the [Stackable Hub](https://hub.stackable.tech/releases).

Since SDP 26.7 the chart is published to two registries:

- `oci://oci.stackable.tech/sdp-charts/zookeeper-operator`
- `oci://quay.io/stackable/sdp-charts/zookeeper-operator`

Both hold the same chart, but the registry you install from also decides where the operator pulls product images from.
Install from quay.io and the operator is configured to use product images from quay.io as well.

Operators are not usually installed on their own.
Most of them need the commons, secret and listener operators alongside them, and `stackablectl` installs a matching set in one step.
See the [documentation](https://docs.stackable.tech/home/stable/zookeeper/) for the full picture.

## Custom resources

This operator installs and manages its own CustomResourceDefinitions, so they are not part of this chart.
The resources it reconciles, and the configuration they accept, are described in the [documentation](https://docs.stackable.tech/home/stable/zookeeper/).
Each CRD is also browsable on the [Stackable Hub](https://hub.stackable.tech/components/zookeeper), with its schema and the API versions served per SDP release.

## Links

- [Documentation](https://docs.stackable.tech/home/stable/zookeeper/)
- [Stackable Hub](https://hub.stackable.tech/)
- [Source](https://github.com/stackabletech/zookeeper-operator)
- [Report an issue](https://github.com/stackabletech/zookeeper-operator/issues)
- [Stackable Data Platform](https://stackable.tech/)

## License

[Open Software License version 3.0](https://github.com/stackabletech/zookeeper-operator/blob/main/LICENSE)
