#!/usr/bin/env bash
# what should be the entry point for bundling/deploying - this script?
#   - split into a) dockers and yamls b) installation
#   - "make opm" (to update csv.yaml etc. and tee everything up)
# How often do we need to create a bundle - every time the manifests change? i.e. every release
#   - yes, every new version
# generate-manifests.sh has been removed from templating, can maybe be re-purposed for openshift stuff..
# manifests:
# - roles.yaml --> same as the ClusterRole definition in helm/zookeeper-operator/templates/roles.yaml
# - zookeeper*.crd.yaml --> same as helm/zookeeper-operator/crds/crds.yaml (less one annotation for helm.sh/resource-policy: keep)
# - csv.yaml --> specific to openshift
# - configmap.yaml --> embeds helm/zookeeper-operator/configs/properties.yaml
# is the templates folder used at all? is it in danger of being removed? can we use for openshift?
# how to parameterize the namespace?
# the operator installs even when the source namespace doesn't match in subscription.yaml
# the catalog-source deploys the operator w/o the subscription?
# Tasks:
#   - try crds.yaml instead of individual files
#   -

set -euo pipefail
set -x

main() {
  VERSION="$1";

  # operator-specific, linked to an operator package
  # done each and every time a new release is built
  opm alpha bundle generate --directory manifests \
  --package stackable-operators --output-dir bundle \
  --channels stable --default stable

  docker build -t "docker.stackable.tech/sandbox/test/zookeeper-operator-bundle:${VERSION}" -f bundle.Dockerfile .
  docker push "docker.stackable.tech/sandbox/test/zookeeper-operator-bundle:${VERSION}"

  opm alpha bundle validate --tag "docker.stackable.tech/sandbox/test/zookeeper-operator-bundle:${VERSION}" --image-builder docker


  echo "Creating Dockerfile..."
  if [ -d "catalog" ]; then
    rm -rf catalog
  fi

  # catalog (just creates dockerfile with copy command): this is not done for each release
  mkdir -p catalog
  rm -f catalog.Dockerfile
  opm generate dockerfile catalog

  # operator package: create/init
  echo "Initiating package..."
  opm init stackable-operators \
      --default-channel=preview \
      --description=./README.md \
      --output yaml > catalog/stackable-operators.yaml

  # operator added to operator package: iterate over operator list
  {
    echo "---"
    echo "schema: olm.channel"
    echo "package: stackable-operators"
    echo "name: preview"
    echo "entries: "
    echo "- name: zookeeper-operator.v${VERSION}"
  } >> catalog/stackable-operators.yaml

  # iterate over operator(-bundle) list
  echo "Rendering operator..."
  opm render "docker.stackable.tech/sandbox/test/zookeeper-operator-bundle:${VERSION}" --output=yaml >> catalog/stackable-operators.yaml

  echo "Validating catalog..."
  opm validate catalog

  # build catalog for all operators
  docker build . -f catalog.Dockerfile -t "docker.stackable.tech/sandbox/test/stackable-operators-catalog:${VERSION}"
  docker push "docker.stackable.tech/sandbox/test/stackable-operators-catalog:${VERSION}"

  # install catalog/group for all operators
  echo "Installing operator..."
  kubectl apply -f catalog-source.yaml
  kubectl apply -f operator-group.yaml

  # iterate over operator list to deploy
  kubectl apply -f zookeeper-subscription.yaml

  echo "Deployment successful!"
}

main "$@"
