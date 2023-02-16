# OLM installation files

The following steps describe how to install a Stackable operator - in this, the operator for Apache Zookeeper - using the [Operator Lifecycle Manager](https://olm.operatorframework.io/) (OLM).

It specifically installs the version 23.1.0 of the operator. Installing additional versions in the future requires generating new bundle images and updating the catalog as described below.

## Usage

Prerequisite is of course a running OpenShift cluster.

First, install the operator using OLM:

    kubectl apply -f catalog-source.yaml \
    -f operator-group.yaml \
    -f subscription.yaml

Then, install the operator dependencies with Helm:

    helm install secret-operator stackable/secret-operator
    helm install commons-operator stackable/commons-operator

And finally, create an Apache Zookeeper cluster:

    kubectl create -f examples/simple-zookeeper-cluster.yaml

NOTE: The `kuttl` tests don't work because they themselves require SCCs which are not available.

## OLM packaging requirements

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

Each catalog can contain several operator packages, and each operator package can contain multiple channels, each with its own bundles of different versions of the operator.

### Generate operator bundle (this is operator-specific)

    opm alpha bundle generate --directory manifests --package zookeeper-operator-package --output-dir bundle --channels stable --default stable

### Build bundle image

    docker build -t docker.stackable.tech/stackable/zookeeper-operator-bundle:23.1.0 -f bundle.Dockerfile .
  	docker push docker.stackable.tech/stackable/zookeeper-operator-bundle:23.1.0

### Validate bundle image

  	opm alpha bundle validate --tag docker.stackable.tech/stackable/zookeeper-operator-bundle:23.1.0 --image-builder docker

## Create catalog

    mkdir catalog
    opm generate dockerfile catalog

## Create a package for each operator

    opm init zookeeper-operator-package \
      --default-channel=stable \
      --description=./README.md \
      --output yaml > catalog/zookeeper-operator-package.yaml

    {
        echo "---"
        echo "schema: olm.channel"
        echo "package: zookeeper-operator-package"
        echo "name: stable"
        echo "entries:"
        echo "- name: zookeeper-operator.v23.1.0"
    } >> catalog/zookeeper-operator-package.yaml

NOTE: with the command below we can add the Stackable logo as icon.

    # add for each operator...
    opm render docker.stackable.tech/stackable/zookeeper-operator-bundle:23.1.0 --output=yaml >> catalog/zookeeper-operator-package.yaml

    # ...and then validate the entire catalog
    opm validate catalog

The catalog is correct if the command above returns successfully without any message. If the catalog doesn't validate, the operator will not install. Now build a catalog image and push it to the repository:

    docker build .  -f catalog.Dockerfile -t docker.stackable.tech/stackable/zookeeper-operator-catalog:latest
    docker push docker.stackable.tech/stackable/zookeeper-operator-catalog:latest

## Install catalog and the operator group

    kubectl apply -f catalog-source.yaml
    kubectl apply -f operator-group.yaml

## List available operators

    kubectl get packagemanifest -n stackable-operators

## Install operator

    kubectl apply -f subscription.yaml
