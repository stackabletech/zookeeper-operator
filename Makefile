.PHONY: docker chart-lint compile-chart

TAG    := $(shell git rev-parse --short HEAD)

docker:
	docker build --force-rm -t "docker.stackable.tech/stackable/zookeeper-operator:${TAG}" -t "docker.stackable.tech/stackable/zookeeper-operator:latest" -f docker/Dockerfile .
	echo "${NEXUS_PASSWORD}" | docker login --username github --password-stdin docker.stackable.tech
	docker push --all-tags docker.stackable.tech/stackable/zookeeper-operator

## Chart related targets
compile-chart: crds config

chart-clean:
	rm -rf deploy/helm/zookeeper-operator/configs
	rm -rf deploy/helm/zookeeper-operator/templates/crds.yaml

config: deploy/helm/zookeeper-operator/configs

deploy/helm/zookeeper-operator/configs:
	cp -r deploy/config-spec deploy/helm/zookeeper-operator/configs

crds: deploy/helm/zookeeper-operator/templates/crds.yaml

deploy/helm/zookeeper-operator/templates/crds.yaml:
	cat deploy/crd/*.yaml | yq e '.metadata.annotations["helm.sh/resource-policy"]="keep"' - > ${@}

chart-lint: compile-chart
	docker run -it -v $(shell pwd):/build/helm-charts -w /build/helm-charts quay.io/helmpack/chart-testing:v3.4.0  ct lint --config deploy/helm/ct.yaml

kind:
	kind create cluster --name chart-test || exit 0
	kubectl --context kind-chart-test config view --minify --flatten > /tmp/kind.yaml
	sleep 10
	kubectl --kubeconfig /tmp/kind.yaml config set-cluster kind-chart-test --server=https://$(shell docker inspect chart-test-control-plane | jq '.[].NetworkSettings.Networks.kind.IPAddress' -r):6443

kind-clean:
	kind delete cluster --name chart-test

chart-test-install: compile-chart
	docker run -it --network kind -v /tmp/kind.yaml:/root/.kube/config -v $(shell pwd):/build/helm-charts -w /build/helm-charts quay.io/helmpack/chart-testing:v3.4.0  ct install --config deploy/helm/ct.yaml
