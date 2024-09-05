
# Minimalistic Java image
FROM almalinux:9-minimal

ENV DCACHE_INSTALL_DIR=/opt/dcache

# Neede by groupadd and useradd
# REVISIT: in some environments base OS kernel doesnt allow use of xattrs and the installation of
#          shadow-utils fails.
# RUN microdnf -y install shadow-utils

# Run dCache as user 'dcache'
#
# REVISIT: as we cant use groupadd and useradd add user/group manually
#
# RUN groupadd dcache && useradd -r -g dcache dcache
RUN echo "dcache:x:1000:" >> /etc/group
RUN echo "dcache:x:994:1000::/home/dcache:/bin/bash" >> /etc/passwd
RUN echo "dcache:!!:18438::::::" >> /etc/shadow

# Add JRE
RUN microdnf -y install java-17-openjdk-headless hostname which procps-ng tar gzip

# Add dCache
COPY maven /opt/dcache

# RUN microdnf -y install  openssh-clients
# generate ssh keys
# RUN ssh-keygen -t rsa -b 2048 -N '' -f ${DCACHE_INSTALL_DIR}/etc/admin/ssh_host_rsa_key
# RUN chown dcache:dcache ${DCACHE_INSTALL_DIR}/etc/admin/ssh_host_rsa_key

# fix liquibase
#RUN rm ${DCACHE_INSTALL_DIR}/share/classes/liquibase-core-*.jar
#COPY liquibase-core-3.5.3.jar ${DCACHE_INSTALL_DIR}/share/classes/liquibase-core-3.5.3.jar

# Post-install brutal cleanup
RUN microdnf clean all && rm -rf /var/cache/yum /var/lib/dnf /var/lib/rpm

# add external files into container at the build time
COPY je.properties ${DCACHE_INSTALL_DIR}/var/nfs/je.properties
COPY dcache.conf ${DCACHE_INSTALL_DIR}/etc/dcache.conf
COPY docker-layout.conf ${DCACHE_INSTALL_DIR}/etc/layouts/docker-layout.conf
COPY exports ${DCACHE_INSTALL_DIR}/etc/exports
COPY run.sh /run.sh
COPY s3-tape.yml /${DCACHE_INSTALL_DIR}/etc/s3-tape.yml

# where we store the data
# location of init scripts
# Stupid grid tools....
RUN mkdir -p /pool /dcache.init.d /etc/grid-security/certificates

# adjust permissions
RUN chown -R dcache:dcache ${DCACHE_INSTALL_DIR}/var /pool

# add s3hsm for fake tape interraction
RUN curl -s -L -o /tmp/s3hsm.tar.gz https://github.com/kofemann/s3hsm/releases/download/v0.1.0/s3hsm_0.1.0_linux_amd64.tar.gz
RUN cd /usr/bin && tar xzf /tmp/s3hsm.tar.gz s3hsm

# the data log files must survive container restarts
VOLUME ${DCACHE_INSTALL_DIR}/var
VOLUME /pool
VOLUME /dcache.init.d

# expose TCP ports for network services
EXPOSE 2288 22125 2049 32049 22224

ENTRYPOINT ["/run.sh"]

# run as user dcache
USER dcache
