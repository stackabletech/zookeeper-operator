FROM registry.access.redhat.com/ubi8/ubi-minimal
USER root
LABEL maintainer="Stackable GmbH"

# Update image
RUN microdnf update --disablerepo=* --enablerepo=ubi-8-appstream --enablerepo=ubi-8-baseos -y && rm -rf /var/cache/yum
RUN microdnf install --disablerepo=* --enablerepo=ubi-8-appstream --enablerepo=ubi-8-baseos httpd -y && rm -rf /var/cache/yum

