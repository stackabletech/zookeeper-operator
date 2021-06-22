.PHONY: docker, release

TAG    := $(shell git rev-parse --short HEAD)

docker: release
	docker build --force-rm -t "docker.stackable.tech/stackable/zookeeper-operator:${TAG}" -t "docker.stackable.tech/stackable/zookeeper-operator:latest" -f docker/Dockerfile .
	echo "${NEXUS_PASSWORD}" | docker login --username github --password-stdin docker.stackable.tech
	docker push --all-tags docker.stackable.tech/stackable/zookeeper-operator
