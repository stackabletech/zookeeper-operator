# OLM installation files

The following steps describe how to install the Stackable operator for Apache Zookeeper using the [Operator Lifecycle Manager](https://olm.operatorframework.io/).

It specifically installs the version 0.10.0 of the operator. Installing additional versions in the future requires generating new bundle images and updating the catalog as described below.

## Usage

The operator is installed if the last paragraph in this document is successful.

The `commons-operator` and `secret-operator` are **required** to manage Zookeeper clusters. They need to be installed using `helm` since it's not possible to install them with `olm`.

The `kuttl` tests don't work because they themselves require SCCs which are not available.

This was successfuly tested:

    kubectl create -f examples/simple-zookeeper-cluster.yaml

## Requirements

- An [OpenShift](https://developers.redhat.com/products/openshift-local/overview) cluster.
- [opm](https://github.com/operator-framework/operator-registry/)
- docker and kubectl
- `kubeadmin` access

It was tested with:

    $ crc version
    WARN A new version (2.5.1) has been published on https://developers.redhat.com/content-gateway/file/pub/openshift-v4/clients/crc/2.5.1/crc-linux-amd64.tar.xz
    CRC version: 2.4.1+b877358
    OpenShift version: 4.10.14
    Podman version: 4.0.2

    $ oc version
    Client Version: 4.10.14
    Server Version: 4.10.14
    Kubernetes Version: v1.23.5+b463d71

    $ opm version
    Version: version.Version{OpmVersion:"v1.23.2", GitCommit:"82505333", BuildDate:"2022-07-04T13:45:39Z", GoOs:"linux", GoArch:"amd64"}

## Open questions

- OLM [doesn't support DaemonSet(s)](https://github.com/operator-framework/operator-lifecycle-manager/issues/1022) and we need them for the secret-operator. Currently we can deploy the secret-operator using Helm but this means we cannot configure the [required](https://olm.operatorframework.io/docs/tasks/creating-operator-manifests/#required-apis) apis of the Zookeeper bundle. What are the consequences for publishing and certification ?
- Here we create a catalog for a single operator. We probably want a catalog for all Stackable operators in the future but this will get large very quickly. Figure out how to handle this. Especially figure out what happens with new versions of the same operator.
- OLM cannot create SecurityContextConstraints objects. The Zookeeper cluster (not the operator) cannot run with the default `restricted` SCC. The current solution is to use the `hostmount-anyuid` SCC for the `zookeeper-clusterrole`. Will this pass the certification process ?
- Everything (catalog, subscription, etc) is installed in the `stackable-operators` namespace. Is this a good idea ?
- The Subscription object uses `installPlanApproval: Automatic` which means the operator is updated automatically for every new version. Is this a good idea?

See the [OLM documentation](https://olm.operatorframework.io/docs/tasks/) for details.

## Build and publish operator bundle image

### Generate operator bundle

    opm alpha bundle generate --directory manifests --package zookeeper-operator --output-dir bundle --channels stable --default stable

### Build bundle image

    docker build -t docker.stackable.tech/stackable/zookeeper-operator-bundle:latest -f bundle.Dockerfile .
    docker push docker.stackable.tech/stackable/zookeeper-operator-bundle:latest

### Validate bundle image

    opm alpha bundle validate --tag docker.stackable.tech/stackable/zookeeper-operator-bundle:latest --image-builder docker

## Create catalog

    mkdir catalog

    opm generate dockerfile catalog

NOTE: with the command below we can add the Stackable logo as icon.

    opm init zookeeper-operator \
    --default-channel=preview \
    --description=./README.md \
    --output yaml > catalog/operator.yaml

    cat << EOF >> catalog/operator.yaml
    ---
    schema: olm.channel
    package: zookeeper-operator
    name: preview
    entries:
    - name: zookeeper-operator.v0.10.0
    EOF

NOTE: the command below generates yaml with broken indentation. I had to manually fix it.

    opm render docker.stackable.tech/stackable/zookeeper-operator-bundle:latest --output=yaml >> catalog/operator.yaml

    opm validate catalog

The catalog is correct if the command above returns successfully without any message. If the catalog doesn't validate, the operator will not install.

    docker build .  -f catalog.Dockerfile -t docker.stackable.tech/stackable/zookeeper-operator-catalog:latest
    docker push docker.stackable.tech/stackable/zookeeper-operator-catalog:latest

## Install catalog

    kubectl apply -f catalog-source.yaml

## List available operators

    kubectl get packagemanifest -n stackable-operators

## Install operator

    kubectl apply -f operator-group.yaml
    kubectl apply -f subscription.yaml
