#!/bin/sh
#
# script that updates SRM space manager schema 
#

@DCACHE_LOAD_CONFIG@

CLASSPATH="$(getProperty dcache.paths.classpath)" \
    ${JAVA} $(getProperty dcache.java.options) \
    diskCacheV111.services.space.Manager $*