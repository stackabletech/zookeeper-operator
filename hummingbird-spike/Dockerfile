# syntax=docker/dockerfile:1.16.0@sha256:e2dd261f92e4b763d789984f6eab84be66ab4f5f08052316d8eb8f173593acf7
# NOTE: The syntax directive needs to be the first line in a Dockerfile

# SPIKE: Stackable Operator for Apache ZooKeeper on Red Hat Hummingbird, without any UBI stage.
#
#
# Differences to docker/Dockerfile:
# - The `ubi10-rust-builder` and `ubi10-minimal` stages are replaced by a Hummingbird builder and by
#   a Hummingbird runtime image (`RUNTIME_BASE`, default `hi/core-runtime`).
# - The final image is built FROM the Hummingbird runtime image and only adds files on top of it.
#   It is not `FROM scratch` and nothing is installed into it with dnf. Reason: the Red Hat certification check
#   `BasedOnUbi` asks Red Hat Pyxis whether a certified image contains the layers of the image under test. A
#   `FROM scratch` image with a single new layer is never found, even if its contents are the same RPMs.
#   Installing RPMs on top of the base could also overwrite files the base layers own (check `HasModifiedFiles`).
# - No user is created. The image runs as a numeric UID without a passwd entry (works in Kubernetes and OpenShift),
#   which avoids editing /etc/passwd and /etc/group, files that belong to RPMs of the base image.
# - Not part of the image (they were in the UBI image and in the previous `FROM scratch` version of this spike):
#   bash profile and aliases, `nano`, `iputils`, `libkadm5`, `krb5-libs` and the other tools from `ubi10-minimal`.
#   TODO: check that nothing needs them. The build prints the shared libraries the binary needs (see `additions`),
#   compare them with what the runtime image provides.
# - The builder is `hi/rust:latest-builder`, which ships the Rust toolchain and dnf. Nothing here installs or pins Rust, so the
#   image's Rust version must match rust-toolchain.toml (the build prints both, see the `builder` stage).
#
# Build from the repository root (the build context must be the repo root and contain `.git`,
# the `built` crate reads the commit hash from it):
#
#   docker build -f hummingbird-spike/Dockerfile --build-arg VERSION=0.0.0-dev -t zookeeper-operator-hum:spike .
#
# To try another runtime base (it must be a glibc based, certified Hummingbird image):
#
#   docker build -f hummingbird-spike/Dockerfile --build-arg VERSION=0.0.0-dev \
#     --build-arg RUNTIME_BASE=registry.access.redhat.com/hi/core-runtime:<tag> -t zookeeper-operator-hum:spike .
#
# Useful intermediate targets: builder, additions
#
#   docker build -f hummingbird-spike/Dockerfile --target additions -t zk-op-hum-additions .

# https://docs.docker.com/build/checks/#fail-build-on-check-violations
# check=error=true

# TODO: pin the bases by digest, like the other Dockerfiles in this repo do
# The `-builder` variant of hi/rust. The plain `hi/rust:latest` has no dnf. The builder variants add
# bash, dnf5 and shadow-utils (default_rpm_packages.builder in the Hummingbird containers repo,
# images/variables.yml), on top of cargo, rust, git-core, make, glibc-devel and openssl-devel (images/rust/properties.yml).
ARG BUILDER_BASE=registry.access.redhat.com/hi/rust:latest-builder
# Used for the `additions` stage only, it needs a shell, chown and ldd. The Rust toolchain in it is not used.
ARG ROOTFS_BASE=${BUILDER_BASE}
# The base of the final image. The final image must share its layers, so this has to be an image that
# Red Hat certified (see the BasedOnUbi check of the preflight tool). It must be glibc based, the operator is
# dynamically linked. `hi/static` has no glibc.
# The binary is built against the glibc of BUILDER_BASE. Keep both bases on the same Hummingbird release.
ARG RUNTIME_BASE=registry.access.redhat.com/hi/core-runtime:latest

# ==============================================================================
# builder: Hummingbird Rust image, builds the operator binary

# We want to automatically use the latest.
# hadolint ignore=DL3007
FROM ${BUILDER_BASE} AS builder

ARG CARGO_AUDITABLE_CRATE_VERSION=0.7.7

# Build stage only, nothing from it is shipped as is. The final image runs as an unprivileged user.
# Root is needed for dnf and to create /src and /app. hi/rust already
# runs as root (`user: root` in its properties.yml), this makes it explicit.
# hadolint ignore=DL3002
USER 0
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# What the build needs on top of the image's cargo, rust, git-core, make and glibc-devel:
# - a C compiler: build scripts of `ring` and the vendored libgit2 (via `git2` in the `built` crate).
#   hi/rust does not list gcc, so make sure it is there (a no-op if it already is).
# hadolint ignore=DL3041
RUN dnf install --assumeyes gcc && dnf clean all

WORKDIR /src
COPY . /src

# The image's Rust must match what the repo pins, nothing else enforces that here
RUN <<EOF
echo "rust-toolchain.toml: $(grep '^channel' rust-toolchain.toml)"
echo "image toolchain: $(rustc --version)"
EOF

# cargo-auditable embeds the dependency list into the binary so that scanners can read it
RUN <<EOF
cargo --locked --quiet install cargo-auditable@"$CARGO_AUDITABLE_CRATE_VERSION"
cargo auditable --quiet build --locked --release --package stackable-zookeeper-operator
mkdir /app
cp target/release/stackable-zookeeper-operator /app/
EOF

# ==============================================================================
# additions: collects the files that are added on top of the runtime base image into /additions

# hadolint ignore=DL3006,DL3007
FROM ${ROOTFS_BASE} AS additions

ARG VERSION

# This is chosen at random and is this high on purpose to have very little chance to clash with an existing user on the host system
# NOTE: Please also update default.nix accordingly!
# There is no passwd entry for it in the image, the UID only has to be numeric and non-zero.
ARG STACKABLE_USER_UID="782252253"

# hadolint ignore=DL3002
USER 0
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

RUN mkdir -p /additions/stackable /additions/usr/local/bin /additions/licenses

COPY <<EOF /additions/README.md
# Stackable Operator for Apache ZooKeeper

* This image contains version ${VERSION} of the operator.
* The operator binary can be found in `/usr/local/bin`
* It is licensed under the OSL-3.0 - the full text can be found in `/licenses/LICENSE`
EOF

COPY LICENSE /additions/licenses/LICENSE
COPY --from=builder /app/* /additions/usr/local/bin/

# All files and folders owned by the user and the root group to support running as arbitrary users
# This is best practice as all container users will belong to the root group (0)
RUN <<EOF
chown -R ${STACKABLE_USER_UID}:0 /additions/stackable /additions/usr/local/bin
chmod -R g=u /additions/stackable

# The runtime base has to provide these. Compare them with the output of `ldd` in the runtime image
# (the shell-less runtime image cannot run it), a missing library shows up as a start failure of the operator.
echo "Shared libraries needed by the operator binary:"
ldd /additions/usr/local/bin/stackable-zookeeper-operator || true
EOF

# ==============================================================================
# FINAL OPERATOR IMAGE

# hadolint ignore=DL3006,DL3007
FROM ${RUNTIME_BASE} AS operator

ARG VERSION
ARG RELEASE="1"
ARG TARGETARCH
ARG STACKABLE_USER_UID="782252253"

COPY --from=additions /additions /

# These labels have mostly been superseded by the OpenContainer spec annotations below but it doesn't hurt to include them
# http://label-schema.org/rc1/
LABEL name="Stackable Operator for Apache ZooKeeper"
LABEL maintainer="info@stackable.tech"
LABEL vendor="Stackable GmbH"
LABEL version="${VERSION}"
# NOTE (@Techassi): This is required for OpenShift/Red Hat certification
# Keeping this as "1" seems to be fine since a couple of years /shrug
LABEL release="${RELEASE}"
LABEL summary="Deploy and manage Apache ZooKeeper clusters."
LABEL description="Deploy and manage Apache ZooKeeper clusters."
LABEL distribution-scope="public"
LABEL url="https://stackable.tech"
LABEL architecture="${TARGETARCH}"

# https://github.com/opencontainers/image-spec/blob/64294bd7a2bf2537e1a6a34d687caae70300b0c4/annotations.md#annotations
LABEL org.opencontainers.image.authors="info@stackable.tech"
LABEL org.opencontainers.image.url="https://stackable.tech"
LABEL org.opencontainers.image.vendor="Stackable GmbH"
LABEL org.opencontainers.image.licenses="OSL-3.0"
LABEL org.opencontainers.image.documentation="https://docs.stackable.tech/home/stable/zookeeper/"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.revision="${RELEASE}"
LABEL org.opencontainers.image.title="Stackable Operator for Apache ZooKeeper"
LABEL org.opencontainers.image.description="Deploy and manage Apache ZooKeeper clusters."

# https://docs.openshift.com/container-platform/4.16/openshift_images/create-images.html#defining-image-metadata
LABEL io.openshift.tags="hummingbird,stackable,sdp,zookeeper"
LABEL io.k8s.description="Deploy and manage Apache ZooKeeper clusters."
LABEL io.k8s.display-name="Stackable Operator for Apache ZooKeeper"

# C.UTF-8 is built into glibc >= 2.35, so no langpack is needed (the Hummingbird images
# set LANG=en_US.UTF-8 but do not ship that locale)
ENV LANG=C.UTF-8
# PATH is left to the base image, the entrypoint below uses an absolute path.
ENV HOME=/stackable

USER ${STACKABLE_USER_UID}

ENTRYPOINT ["/usr/local/bin/stackable-zookeeper-operator"]
CMD ["run"]
