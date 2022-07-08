# OLM installation files

[OLM documentation](https://olm.operatorframework.io/docs/tasks/)

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

    opm init zookeeper-operator \
    --default-channel=preview \
    --description=./README.md \
    --output yaml > catalog/operator.yaml

    opm render docker.stackable.tech/stackable/zookeeper-operator-bundle:latest --output=yaml >> catalog/operator.yaml

    cat << EOF >> catalog/operator.yaml
    ---
    schema: olm.channel
    package: zookeeper-operator
    name: preview
    entries:
    - name: zookeeper-operator.v0.10.0
    EOF

    opm validate catalog

    docker build .  -f catalog.Dockerfile -t docker.stackable.tech/stackable/zookeeper-operator-catalog:latest
    docker push docker.stackable.tech/stackable/zookeeper-operator-catalog:latest

## Install catalog

    kubectl apply -f catalog-source.yaml

## List available operators

    kubectl get packagemanifest -n stackable-operators

## Install operator

    kubectl apply -f operator-group.yaml
    kubectl apply -f subscription.yaml
