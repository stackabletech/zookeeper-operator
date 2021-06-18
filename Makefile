.PHONY: docker

docker:
	mkdir -p docker/build
	cp target/release/stackable-zookeeper-operator-server docker/build
	cd docker && docker build --rm -t "stackable/zookeeper-operator:${GITHUB_SHA}"
