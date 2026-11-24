#!/bin/sh

DOMAIN=$1

if [ -z $DOMAIN ]
then
  DOMAIN=`hostname -s`
fi

echo $DOMAIN

DCACHE_HOME=${DCACHE_INSTALL_DIR}

PLUGINS_CLASSPATH=""
if [ -d ${DCACHE_HOME}/share/plugins ]
then
  PLUGINS_CLASSPATH=`find ${DCACHE_HOME}/share/plugins -type f -name \*.jar -printf "%p:"`
fi

CLASSPATH=${DCACHE_HOME}/share/classes/*
if [ ! -z "${PLUGINS_CLASSPATH}" ]
then
  CLASSPATH=${CLASSPATH}:${PLUGINS_CLASSPATH}
fi

export CLASSPATH

# we hope that there is only one agent file and it the right one
ASPECT_AGENT=`ls ${DCACHE_HOME}/share/classes/aspectjweaver-*.jar`

# The JaCoCo agent must come before the AspectJ agent, so that JaCoCo instruments
# the compiled classes rather than the woven ones. Otherwise the execution data of
# woven classes does not match the compiled classes used for the coverage report.
JACOCO_AGENT_JAR="${DCACHE_HOME}/share/classes/jacocoagent.jar"
JACOCO_AGENT=""

# Only classes built from dCache's own modules are instrumented. This also
# matches the dCache projects nfs4j, oncrpc4j and xrootd4j (org.dcache.*). The
# org.apache and org.globus entries are the few dCache classes that live in
# library packages.
JACOCO_INCLUDES="org.dcache.*:diskCacheV111.*:dmg.*:javatunnel.*:org.gaul.s3proxy.*"
JACOCO_INCLUDES="${JACOCO_INCLUDES}:org.apache.zookeeper.server.PatchedZooKeeperServer*"
JACOCO_INCLUDES="${JACOCO_INCLUDES}:org.apache.curator.framework.imps.PublicDelegatingCuratorFramework"
JACOCO_INCLUDES="${JACOCO_INCLUDES}:org.globus.gsi.gssapi.jaas.GlobusPrincipal"
JACOCO_INCLUDES="${JACOCO_INCLUDES}:org.globus.gsi.gssapi.jaas.SimplePrincipal"

if [ "${JACOCO_ENABLED}" = "true" ] && [ -s "$JACOCO_AGENT_JAR" ]; then
    _opts="${JACOCO_OPTS:-output=tcpserver,address=*,port=6300,append=false}"
    JACOCO_AGENT="-javaagent:${JACOCO_AGENT_JAR}=${_opts},includes=${JACOCO_INCLUDES}"
    echo "JaCoCo enabled: ${_opts}"
else
    echo "JaCoCo disabled."
fi

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
	${JACOCO_AGENT:+"${JACOCO_AGENT}"} \
	-javaagent:${ASPECT_AGENT} \
	-Djava.awt.headless=true -DwantLog4jSetup=n \
	-Ddcache.home=${DCACHE_HOME} \
	-Ddcache.paths.defaults=${DCACHE_HOME}/share/defaults \
	-XX:MaxRAMPercentage=75 \
	-XX:+UseZGC \
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