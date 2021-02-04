FROM openjdk:11-jre

MAINTAINER dCache "https://www.dcache.org"

# dCache version passed as an argument
ARG DCACHE_VERSION

ENV DCACHE_HOME=/opt/dcache

RUN groupadd -r dcache && useradd -r -g dcache dcache

ADD target/dcache-${DCACHE_VERSION}.tar.gz  /opt/

RUN mv /opt/dcache-${DCACHE_VERSION} /opt/dcache
