#!/bin/sh
#
#  A simple shell wrapper around the NameSpaceProvider performance
#  testing tool.

@DCACHE_LOAD_CONFIG@

lib="$(getProperty dcache.paths.share.lib)"
. ${lib}/utils.sh

classpath=$(printLimitedClassPath chimera HikariCP javassist \
    guava jline common-cli dcache-common acl-vehicles acl \
    slf4j-api logback-classic logback-core logback-console-config \
    $(getProperty chimera.db.jar))

CLASSPATH="$(getProperty dcache.paths.classpath)" java \
   -Dlog=${DCACHE_LOG:-warn} \
   org.dcache.chimera.PerformanceTest \
    "$(getProperty chimera.db.url)" \
    "$(getProperty chimera.db.dialect)" \
    "$(getProperty chimera.db.user)" \
    "$(getProperty chimera.db.password)" \
    "$@"
