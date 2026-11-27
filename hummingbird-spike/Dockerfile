# syntax=docker/dockerfile:1.16.0@sha256:e2dd261f92e4b763d789984f6eab84be66ab4f5f08052316d8eb8f173593acf7
# NOTE: The syntax directive needs to be the first line in a Dockerfile

# SPIKE: Stackable Operator for Apache ZooKeeper on Red Hat Hummingbird, without any UBI stage.
#
#
# Differences to docker/Dockerfile:
# - The `ubi10-rust-builder` and `ubi10-minimal` stages are replaced by a Hummingbird builder and by
#   a Hummingbird runtime image (`RUNTIME_BASE`, default `hi/core-runtime`).
# - The final image is built FROM the Hummingbird runtime image and only adds files on top of it.
#   The runtime image has bash and coreutils but no package manager, so nothing is installed in it.
# - No user is created. The image runs as a numeric UID without a passwd entry (works in Kubernetes and OpenShift),
#   which avoids editing /etc/passwd and /etc/group, files that belong to RPMs of the base image.
# - Not part of the image (they were in the UBI image and in the previous `FROM scratch` version of this spike):
#   bash profile and aliases, `nano`, `iputils`, `libkadm5`, `krb5-libs` and the other tools from `ubi10-minimal`.
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
# Useful intermediate target: builder
#
#   docker build -f hummingbird-spike/Dockerfile --target builder -t zk-op-hum-builder .

# https://docs.docker.com/build/checks/#fail-build-on-check-violations
# check=error=true

# TODO: pin the bases by digest, like the other Dockerfiles in this repo do
# The `-builder` variant of hi/rust. The plain `hi/rust:latest` has no dnf. The builder variants add
# bash, dnf5 and shadow-utils (default_rpm_packages.builder in the Hummingbird containers repo,
# images/variables.yml), on top of cargo, rust, git-core, make, glibc-devel and openssl-devel (images/rust/properties.yml).
# The image also has gcc (gcc-16.2.1 when checked), which rustc needs as linker and the build scripts of
# `ring` and the vendored libgit2 (via `git2` in the `built` crate) need as C compiler.
ARG BUILDER_BASE=registry.access.redhat.com/hi/rust:latest-builder
# The base of the final image. The final image must share its layers, so this has to be an image that
# Red Hat certified. It must be glibc based, the operator is dynamically linked. `hi/static` has no glibc.
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
# FINAL OPERATOR IMAGE

# hadolint ignore=DL3006,DL3007
FROM ${RUNTIME_BASE} AS operator

ARG VERSION
ARG RELEASE="1"
ARG TARGETARCH
ARG STACKABLE_USER_UID="782252253"

# All files are owned by the user and the root group (g=u) to support running as arbitrary users.
# This is best practice as all container users will belong to the root group (0).
COPY --chown=${STACKABLE_USER_UID}:0 --chmod=0664 LICENSE /licenses/LICENSE
COPY --from=builder --chown=${STACKABLE_USER_UID}:0 --chmod=0775 /app/stackable-zookeeper-operator /usr/local/bin/stackable-zookeeper-operator

# The home directory and the README. The runtime image has bash and coreutils, so no extra stage is needed.
# Runs as root only for this step, the USER instruction below sets the unprivileged user.
# The check runs `ldd` in the runtime image: it fails the build if the base lacks a shared library of the binary.
USER 0
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
RUN <<EOF
mkdir /stackable
chown ${STACKABLE_USER_UID}:0 /stackable
chmod g=u /stackable

cat > /README.md <<README
# Stackable Operator for Apache ZooKeeper

* This image contains version ${VERSION} of the operator.
* The operator binary can be found in \`/usr/local/bin\`
* It is licensed under the OSL-3.0 - the full text can be found in \`/licenses/LICENSE\`
README
chown ${STACKABLE_USER_UID}:0 /README.md
chmod 0664 /README.md

libs="$(ldd /usr/local/bin/stackable-zookeeper-operator)"
echo "Shared libraries needed by the operator binary:"
echo "$libs"
if [[ "$libs" == *"not found"* ]]; then
  echo "The runtime base image does not provide all shared libraries of the operator."
  exit 1
fi
EOF

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
