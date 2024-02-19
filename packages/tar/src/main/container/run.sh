#!/bin/sh

DOMAIN=$1

if [ -z $DOMAIN ]
then
  DOMAIN=`hostname -s`
fi

echo $DOMAIN

DCACHE_HOME=${DCACHE_INSTALL_DIR}
export CLASSPATH=${DCACHE_HOME}/share/classes/*

# we hope that there is only one agent file and it the right one
ASPECT_AGENT=`ls ${DCACHE_HOME}/share/classes/aspectjweaver-*.jar`


if [ ! -f /.init_complete ]
then
  for f in `ls /dcache.init.d`
  do
    . /dcache.init.d/$f
  done
fi


exec /usr/bin/java \
	-Dsun.net.inetaddr.ttl=1800 \
	-Dorg.globus.tcp.port.range=20000,25000 \
	-Dorg.dcache.dcap.port=0 \
	-Dorg.dcache.net.tcp.portrange=33115:33145 \
	-Dorg.globus.jglobus.delegation.cache.lifetime=30000 \
	-Dorg.globus.jglobus.crl.cache.lifetime=60000 \
	-Djava.security.krb5.realm= \
	-Djava.security.krb5.kdc= \
	-Djavax.security.auth.useSubjectCredsOnly=false \
	-XX:+HeapDumpOnOutOfMemoryError \
	-XX:HeapDumpPath=${DCACHE_INSTALL_DIR}/var/log/${DOMAIN}-oom.hprof \
	-javaagent:${ASPECT_AGENT} \
	-Djava.awt.headless=true -DwantLog4jSetup=n \
	-Ddcache.home=${DCACHE_HOME} \
	-Ddcache.paths.defaults=${DCACHE_HOME}/share/defaults \
	-Dzookeeper.sasl.client=false \
	--add-opens=java.base/java.lang=ALL-UNNAMED \
	--add-opens=java.base/java.util=ALL-UNNAMED \
	--add-opens=java.base/java.net=ALL-UNNAMED \
	--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
	--add-opens=java.base/java.text=ALL-UNNAMED \
	--add-opens=java.sql/java.sql=ALL-UNNAMED  \
	--add-opens=java.base/java.math=ALL-UNNAMED \
	--add-opens=java.base/sun.nio.fs=ALL-UNNAMED \
	${JAVA_ARGS} \
	org.dcache.boot.BootLoader start ${DOMAIN}
