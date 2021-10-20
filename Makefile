.PHONY: docker chart-lint compile-chart

TAG    := $(shell git rev-parse --short HEAD)

docker:
	docker build --force-rm -t "docker.stackable.tech/stackable/zookeeper-operator:${TAG}" -t "docker.stackable.tech/stackable/zookeeper-operator:latest" -f docker/Dockerfile .
	echo "${NEXUS_PASSWORD}" | docker login --username github --password-stdin docker.stackable.tech
	docker push --all-tags docker.stackable.tech/stackable/zookeeper-operator

compile-chart: crds config

config: deploy/helm/zookeeper-operator/configs

deploy/helm/zookeeper-operator/configs:
	cp -r deploy/config-spec deploy/helm/zookeeper-operator/configs

crds: deploy/helm/zookeeper-operator/templates/crds.yaml

deploy/helm/zookeeper-operator/templates/crds.yaml:
	cat deploy/crd/*.yaml > ${@}

chart-lint: compile-chart
	docker run -it -v $(shell pwd):/build/helm-charts -w /build/helm-charts quay.io/helmpack/chart-testing:v3.4.0  ct lint --config deploy/helm/ct.yaml