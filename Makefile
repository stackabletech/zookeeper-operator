# =============
# This file is automatically generated from the templates in stackabletech/operator-templating
# DON'T MANUALLY EDIT THIS FILE
# =============

.PHONY: docker chart-lint compile-chart

TAG    := $(shell git rev-parse --short HEAD)

VERSION := $(shell cargo metadata --format-version 1 | jq '.packages[] | select(.name=="stackable-zookeeper-operator") | .version')

docker:
	docker build --force-rm -t "docker.stackable.tech/stackable/zookeeper-operator:${VERSION}" -t "docker.stackable.tech/stackable/zookeeper-operator:latest" -f docker/Dockerfile .
	echo "${NEXUS_PASSWORD}" | docker login --username github --password-stdin docker.stackable.tech
	docker push --all-tags docker.stackable.tech/stackable/zookeeper-operator

## Chart related targets
compile-chart: version crds config 

chart-clean:
	rm -rf deploy/helm/zookeeper-operator/configs
	rm -rf deploy/helm/zookeeper-operator/templates/crds.yaml

version:
	yq eval -i '.version = ${VERSION} | .appVersion = ${VERSION}' deploy/helm/zookeeper-operator/Chart.yaml


config: deploy/helm/zookeeper-operator/configs

deploy/helm/zookeeper-operator/configs:
	cp -r deploy/config-spec deploy/helm/zookeeper-operator/configs

crds: deploy/helm/zookeeper-operator/crds/crds.yaml

deploy/helm/zookeeper-operator/crds/crds.yaml:
	mkdir -p deploy/helm/zookeeper-operator/crds
	cat deploy/crd/*.yaml | yq e '.metadata.annotations["helm.sh/resource-policy"]="keep"' - > ${@}

chart-lint: compile-chart
	docker run -it -v $(shell pwd):/build/helm-charts -w /build/helm-charts quay.io/helmpack/chart-testing:v3.4.0  ct lint --config deploy/helm/chart_testing.yaml

## Manifest related targets
clean-manifests:
	mkdir -p deploy/manifests
	rm -rf $$(find deploy/manifests -maxdepth 1 -mindepth 1 -not -name kustomization.yaml)

generate-manifests: clean-manifests compile-chart
	set -e ;\
	TMP=$$(mkdir -d -t manifests) ;\
	helm template --output-dir $$TMP --include-crds deploy/helm/zookeeper-operator ;\
	find $$TMP -type f |xargs -L 1 yq eval -i 'del(.. | select(has("app.kubernetes.io/managed-by")) | ."app.kubernetes.io/managed-by")' ;\
	find $$TMP -type f |xargs -L 1 yq eval -i 'del(.. | select(has("helm.sh/chart")) | ."helm.sh/chart")' ;\
	cp -r $$TMP/zookeeper-operator/*/* deploy/manifests/ ;\
	rm -rf $$TMP ;\
