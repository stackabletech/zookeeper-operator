FROM registry.access.redhat.com/ubi8/ubi-minimal
USER root
LABEL maintainer="Stackable GmbH"

# Update image
RUN microdnf update --disablerepo=* --enablerepo=ubi-8-baseos --enablerepo=ubi-8-baseos -y \
  && rm -rf /var/cache/yum \
  && microdnf install --disablerepo=* --enablerepo=ubi-8-baseos shadow-utils -y \
  && rm -rf /var/cache/yum

COPY build/* /

RUN groupadd -g 1000 stackable && adduser -u 1000 -g stackable -c 'Stackable Operator' stackable

USER 1000:1000

ENTRYPOINT ["/stackable-zookeeper-operator-server"]
