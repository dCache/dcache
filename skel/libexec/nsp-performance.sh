#!/bin/sh
#
#  A simple shell wrapper around the NameSpaceProvider performance
#  testing tool.

#  Default location for dCache.
DCACHE_HOME=${DCACHE_HOME:-/opt/d-cache}

#  Ask dCache for the externalLibsClassPath.
ourHomeDir=${DCACHE_HOME} . $DCACHE_HOME/classes/extern.classpath

LOG4J_FILE=${DCACHE_HOME}/config/log4j.properties

DCACHE_JAR=$DCACHE_HOME/classes/dcache.jar

CLASS=diskCacheV111.namespace.PerformanceTest
OPTIONS=-Dlog4j.configuration=file:$LOG4J_FILE

java -cp $DCACHE_JAR:$externalLibsClassPath $OPTIONS $CLASS "$@"
