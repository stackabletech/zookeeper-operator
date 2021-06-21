FROM registry.access.redhat.com/ubi8/ubi-minimal AS builder
LABEL maintainer="Stackable GmbH"

# Update image
RUN microdnf update --disablerepo=* --enablerepo=ubi-8-appstream --enablerepo=ubi-8-baseos --enablerepo=ubi-8-baseos -y \
  && rm -rf /var/cache/yum \
  && microdnf install --disablerepo=* --enablerepo=ubi-8-appstream --enablerepo=ubi-8-baseos curl gcc make openssl-devel pkg-config systemd-devel -y \
  && rm -rf /var/cache/yum

WORKDIR /app
COPY . /app

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
 && . $HOME/.cargo/env \
 && rustup toolchain install stable \
 && rustup default stable

RUN . $HOME/.cargo/env && cargo build --release


FROM registry.access.redhat.com/ubi8/ubi-minimal AS operator
LABEL maintainer="Stackable GmbH"

# Update image
RUN microdnf update --disablerepo=* --enablerepo=ubi-8-baseos --enablerepo=ubi-8-baseos -y \
  && rm -rf /var/cache/yum \
  && microdnf install --disablerepo=* --enablerepo=ubi-8-baseos shadow-utils -y \
  && rm -rf /var/cache/yum

COPY --from=builder /app/target/release/stackable-zookeeper-operator-server /

RUN groupadd -g 1000 stackable && adduser -u 1000 -g stackable -c 'Stackable Operator' stackable

USER 1000:1000

ENTRYPOINT ["/stackable-zookeeper-operator-server"]
