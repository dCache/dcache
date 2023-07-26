
# Minimalistic Java image
FROM alpine:3.15

ENV DCACHE_INSTALL_DIR=/opt/dcache

# Run dCache as user 'dcache'
RUN addgroup dcache && adduser -S -G dcache dcache

# Add JRE
RUN apk --no-cache add openjdk11 --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community

# Add dCache
COPY maven /opt/dcache

# generate ssh keys
RUN apk --update add openssh
RUN ssh-keygen -t rsa -b 2048 -N '' -f ${DCACHE_INSTALL_DIR}/etc/admin/ssh_host_rsa_key
RUN chown dcache:dcache ${DCACHE_INSTALL_DIR}/etc/admin/ssh_host_rsa_key

# fix liquibase
#RUN rm ${DCACHE_INSTALL_DIR}/share/classes/liquibase-core-*.jar
#COPY liquibase-core-3.5.3.jar ${DCACHE_INSTALL_DIR}/share/classes/liquibase-core-3.5.3.jar


# add external files into container at the build time
COPY je.properties ${DCACHE_INSTALL_DIR}/var/nfs/je.properties
COPY dcache.conf ${DCACHE_INSTALL_DIR}/etc/dcache.conf
COPY docker-layout.conf ${DCACHE_INSTALL_DIR}/etc/layouts/docker-layout.conf
COPY exports ${DCACHE_INSTALL_DIR}/etc/exports
COPY run.sh /run.sh

# where we store the data
RUN mkdir /pool

# location of init scripts
RUN mkdir /dcache.init.d

# Stupid grid tools....
RUN mkdir -p /etc/grid-security/certificates

# adjust permissions
RUN chown -R dcache:dcache ${DCACHE_INSTALL_DIR}/var
RUN chown -R dcache:dcache /pool

# the data log files must survive container restarts
VOLUME ${DCACHE_INSTALL_DIR}/var
VOLUME /pool
VOLUME /dcache.init.d

# expose TCP ports for network services
EXPOSE 2288 22125 2049 32049 22224

ENTRYPOINT ["/run.sh"]

# run as user dcache
USER dcache
