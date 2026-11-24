# syntax=docker/dockerfile:1.16.0@sha256:e2dd261f92e4b763d789984f6eab84be66ab4f5f08052316d8eb8f173593acf7
# NOTE: The syntax directive needs to be the first line in a Dockerfile

# SPIKE: Stackable Operator for Apache ZooKeeper on Red Hat Hummingbird, without any UBI stage.
#
#
# Differences to docker/Dockerfile:
# - The `ubi10-rust-builder` and `ubi10-minimal` stages are replaced by a Hummingbird builder and by a
#   root filesystem that is assembled with `dnf --installroot` from the Hummingbird RPM repo.
# - The final image is `FROM scratch`, the distroless Hummingbird runtime has no shell to run these steps in.
# - `nano` and `iputils` are left out, they are not in the Hummingbird repo (same as in the ZooKeeper spike).
# - `libkadm5` is left out, the operator only needs the Kerberos client libraries (`krb5-libs`).
#   TODO: check that nothing needs it (the UBI image installs it).
# - The builder is `hi/rust:latest-builder`, which ships the Rust toolchain and dnf. Nothing here installs or pins Rust, so the
#   image's Rust version must match rust-toolchain.toml (the build prints both, see the `builder` stage).
#
# Build from the repository root (the build context must be the repo root and contain `.git`,
# the `built` crate reads the commit hash from it):
#
#   docker build -f hummingbird-spike/Dockerfile --build-arg VERSION=0.0.0-dev -t zookeeper-operator-hum:spike .
#
# Useful intermediate targets: builder, rootfs
#
#   docker build -f hummingbird-spike/Dockerfile --target rootfs -t zk-op-hum-rootfs .

# https://docs.docker.com/build/checks/#fail-build-on-check-violations
# check=error=true

# TODO: pin both bases by digest, like the other Dockerfiles in this repo do
# The `-builder` variant of hi/rust. The plain `hi/rust:latest` has no dnf. The builder variants add
# bash, dnf5 and shadow-utils (default_rpm_packages.builder in the Hummingbird containers repo,
# images/variables.yml), on top of cargo, rust, git-core, make, glibc-devel and openssl-devel (images/rust/properties.yml).
ARG BUILDER_BASE=registry.access.redhat.com/hi/rust:latest-builder
# Used for the runtime root filesystem only, it only needs dnf and shadow-utils. The Rust toolchain in it is not used.
ARG ROOTFS_BASE=${BUILDER_BASE}

# ==============================================================================
# builder: Hummingbird Rust image, builds the operator binary

# We want to automatically use the latest.
# hadolint ignore=DL3007
FROM ${BUILDER_BASE} AS builder

ARG CARGO_AUDITABLE_CRATE_VERSION=0.7.7

# Build stage only, nothing from it is shipped as is. The final image is FROM scratch and
# runs as an unprivileged user. Root is needed for dnf and to create /src and /app. hi/rust already
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
# rootfs: assembles the runtime filesystem from the Hummingbird RPM repo into /rootfs

# hadolint ignore=DL3006,DL3007
FROM ${ROOTFS_BASE} AS rootfs

ARG VERSION
ARG RELEASE="1"

# These are chosen at random and are this high on purpose to have very little chance to clash with an existing user or group on the host system
# NOTE: Please also update default.nix accordingly!
ARG STACKABLE_USER_GID="574654813"
ARG STACKABLE_USER_UID="782252253"
ARG STACKABLE_USER_NAME="stackable"

# hadolint ignore=DL3002
USER 0
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
ENV HOME=/root

COPY <<EOF /etc/dnf/dnf.conf
[main]
install_weak_deps=0
assumeyes=True
tsflags=nodocs
EOF

# bash, coreutils, findutils, less, tar: debugging aids and `kubectl cp`, as in docker/Dockerfile
# gzip: not pulled in by anything else, without it `tar -z` fails (found in the ZooKeeper spike)
# krb5-libs: the Kerberos client libraries (package name unverified, the ZooKeeper spike installed krb5-workstation)
# ca-certificates: trust store
# shadow-utils: only to create the user, removed again below
# hadolint ignore=DL3041
RUN <<EOF
mkdir /rootfs
dnf --installroot=/rootfs --use-host-config install \
  bash \
  ca-certificates \
  coreutils \
  findutils \
  gzip \
  krb5-libs \
  less \
  shadow-utils \
  tar

# The --no-log-init is required to work around a bug/problem in Go/Docker when very large UIDs are used
# See https://github.com/moby/moby/issues/5419#issuecomment-41478290 for more context
# We'll leave the home directory hardcoded to /stackable because I don't want to deal with which chars might be valid and which might not in user name vs. directory
chroot /rootfs groupadd --gid ${STACKABLE_USER_GID} --system ${STACKABLE_USER_NAME}
chroot /rootfs useradd \
  --no-log-init \
  --gid ${STACKABLE_USER_GID} \
  --uid ${STACKABLE_USER_UID} \
  --system \
  --create-home \
  --home-dir /stackable \
  ${STACKABLE_USER_NAME}
dnf --installroot=/rootfs --use-host-config remove shadow-utils

# dnf state and caches have no use in the final image (the rpmdb is kept)
dnf clean all
rm -rf /rootfs/var/cache /rootfs/var/lib/dnf /rootfs/var/log/*
EOF

# Make shell usage in our containers "nicer"
# SC2028: echo won't expand escape sequences. Consider printf.
# Reason: This is complaining about the complicated PS1 statement, it is the same as in docker/Dockerfile.
# hadolint ignore=SC2028
RUN <<EOF
{
  echo "alias ll='ls -alF --color=auto'"
  echo "alias ls='ls --color=auto'"
  echo "alias ..='cd ..'"
  echo "export PS1='\u@\[\e[36m\]\H\[\e[m\] \[\e[32m\]\$(pwd)\[\e[m\] \\$ '"
} >> /rootfs/stackable/.bashrc
printf 'if [ -f ~/.bashrc ]; then\n\tsource ~/.bashrc\nfi\n' >> /rootfs/stackable/.profile
EOF

COPY <<EOF /rootfs/README.md
# Stackable Operator for Apache ZooKeeper

* This image contains version ${VERSION} of the operator.
* The operator binary can be found in `/usr/local/bin`
* It is licensed under the OSL-3.0 - the full text can be found in `/licenses/LICENSE`
EOF

COPY LICENSE /rootfs/licenses/LICENSE
COPY --from=builder /app/* /rootfs/usr/local/bin/

# All files and folders owned by root to support running as arbitrary users
# This is best practice as all container users will belong to the root group (0)
RUN <<EOF
rpm --root /rootfs -qa --queryformat "%{NAME}-%{VERSION}-%{RELEASE}\n" | sort > /rootfs/stackable/package_manifest.txt
chown -R ${STACKABLE_USER_UID}:0 /rootfs/stackable /rootfs/usr/local/bin
chmod -R g=u /rootfs/stackable
EOF

# ==============================================================================
# FINAL OPERATOR IMAGE

FROM scratch AS operator

ARG VERSION
ARG RELEASE="1"
ARG TARGETARCH
ARG STACKABLE_USER_UID="782252253"

COPY --from=rootfs /rootfs /

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
ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ENV HOME=/stackable

USER ${STACKABLE_USER_UID}

ENTRYPOINT ["stackable-zookeeper-operator"]
CMD ["run"]
