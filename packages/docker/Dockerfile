FROM openjdk:8-jdk

MAINTAINER dCache "https://www.dcache.org"

ENV DCACHE_HOME=/opt/dcache


RUN groupadd -r dcache && useradd -r -g dcache dcache

ADD target/dcache*.tar.gz  /opt/

RUN mv /opt/dcache* /opt/dcache
