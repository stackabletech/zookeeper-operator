.PHONY: docker

docker:
	mkdir -p docker/build
	cp target/release/stackable-zookeeper-operator-server docker/build
	cd docker && docker build --rm -t "docker.stackable.tech/stackable/zookeeper-operator:${GITHUB_SHA}" -t "docker.stackable.tech/stackable/zookeeper-operator:latest" .
	echo "${NEXUS_PASSWORD}" | docker login --username github --password-stdin docker.stackable.tech
	docker push --all-tags docker.stackable.tech/stackable/zookeeper-operator
