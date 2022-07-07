# OLM installation files

## Generate operator bundle

    opm alpha bundle generate --directory manifests --package zookeeper-operator --output-dir bundle --channels stable --default stable

## Build bundle image

    docker build -t docker.stackable.tech/stackable/zookeeper-operator-bundle:latest -f bundle.Dockerfile .
    docker push docker.stackable.tech/stackable/zookeeper-operator-bundle:latest

## Validate bundle image

    opm alpha bundle validate --tag docker.stackable.tech/stackable/zookeeper-operator-bundle:latest --image-builder docker
