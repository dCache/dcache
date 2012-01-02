#!/bin/sh
#
#  A simple shell wrapper around the NameSpaceProvider performance
#  testing tool.

DCACHE_HOME=${DCACHE_HOME:-@dcache.home@}
. @dcache.paths.bootloader@/loadConfig.sh

CLASSPATH="$(getProperty dcache.paths.classpath)" java \
   -Dlog=${DCACHE_LOG:-warn} \
   diskCacheV111.namespace.PerformanceTest "$@"
