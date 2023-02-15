#!/usr/bin/env bash
# what should be the entry point for bundling/deploying - this script?
# How often do we need to create a bundle - every time the manifests change? i.e. every release
# generate-manifests.sh has been removed from templating, can maybe be re-purposed for openshift stuff..
# manifests:
# - roles.yaml --> same as the ClusterRole definition in helm/zookeeper-operator/templates/roles.yaml
# - zookeeper*.crd.yaml --> same as helm/zookeeper-operator/crds/crds.yaml (less one annotation for helm.sh/resource-policy: keep)
# - csv.yaml --> specific to openshift
# - configmap.yaml --> embeds helm/zookeeper-operator/configs/properties.yaml
# is the templates folder used at all? is it in danger of being removed? can we use for openshift?
# how to parameterize the namespace?

set -euo pipefail
set -x

main() {
  VERSION="$1";

  opm alpha bundle generate --directory manifests \
  --package zookeeper-operator --output-dir bundle \
  --channels stable --default stable

  docker build -t "docker.stackable.tech/sandbox/test/zookeeper-operator-bundle:${VERSION}" -f bundle.Dockerfile .
  docker push "docker.stackable.tech/sandbox/test/zookeeper-operator-bundle:${VERSION}"

  opm alpha bundle validate --tag "docker.stackable.tech/sandbox/test/zookeeper-operator-bundle:${VERSION}" --image-builder docker

  echo "Creating Dockerfile..."
  if [ -d "catalog" ]; then
    rm -rf catalog
  fi

  mkdir -p catalog
  rm -f catalog.Dockerfile
  opm generate dockerfile catalog

  echo "Initiating operator..."
  opm init zookeeper-operator \
      --default-channel=preview \
      --description=./README.md \
      --output yaml > catalog/operator.yaml

  # TODO this is ugly. Try cat?
  {
    echo "---"
    echo "schema: olm.channel"
    echo "package: zookeeper-operator"
    echo "name: preview"
    echo "entries: "
    echo "- name: zookeeper-operator.v${VERSION}"
  } >> catalog/operator.yaml

  echo "Rendering operator..."
  opm render "docker.stackable.tech/sandbox/test/zookeeper-operator-bundle:${VERSION}" --output=yaml >> catalog/operator.yaml

  echo "Validating catalog..."
  opm validate catalog

  docker build . -f catalog.Dockerfile -t "docker.stackable.tech/sandbox/test/zookeeper-operator-catalog:${VERSION}"
  docker push "docker.stackable.tech/sandbox/test/zookeeper-operator-catalog:${VERSION}"

  echo "Installing operator..."
  #kubectl apply -f catalog-source.yaml
  #kubectl apply -f operator-group.yaml
  #kubectl apply -f subscription.yaml

  echo "Deployment successful!"
}

main "$@"
