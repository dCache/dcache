FROM dcache/deps:master
MAINTAINER dCache "https://www.dcache.org"

RUN groupadd -r dcache && useradd -r -g dcache dcache

RUN git clone https://github.com/dCache/dcache.git

WORKDIR /dcache

RUN git checkout master

RUN mvn -am -pl packages/fhs package -P deb -DskipTests 

RUN dpkg -i packages/fhs/target/dcache_*all.deb

WORKDIR /

RUN rm -rf /dcache
RUN rm -rf /root/.m2
