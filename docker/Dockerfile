FROM docker.stackable.tech/stackable/ubi8-rust-builder AS builder

FROM registry.access.redhat.com/ubi8/ubi-minimal AS operator
LABEL maintainer="Stackable GmbH"

# Update image
RUN microdnf update --disablerepo=* --enablerepo=ubi-8-baseos --enablerepo=ubi-8-baseos -y \
  && rm -rf /var/cache/yum \
  && microdnf install --disablerepo=* --enablerepo=ubi-8-baseos shadow-utils -y \
  && rm -rf /var/cache/yum

COPY --from=builder /app/stackable-zookeeper-operator-server /

RUN groupadd -g 1000 stackable && adduser -u 1000 -g stackable -c 'Stackable Operator' stackable

USER 1000:1000

ENTRYPOINT ["/stackable-zookeeper-operator-server"]
