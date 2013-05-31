#!/bin/sh
#
#  A simple shell wrapper around the NameSpaceProvider performance
#  testing tool.

@DCACHE_LOAD_CONFIG@

CLASSPATH="$(getProperty dcache.paths.classpath)" java \
   -Dlog=${DCACHE_LOG:-warn} \
   diskCacheV111.namespace.PerformanceTest "$@"
