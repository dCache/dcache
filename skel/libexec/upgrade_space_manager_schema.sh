#!/bin/sh
#
# script that updates SRM space manager schema 
#

# Initialize environment. /etc/default/ is the normal place for this
# on several Linux variants. For other systems we provide
# /etc/dcache.env. Those files will typically declare JAVA_HOME and
# DCACHE_HOME and nothing else.
[ -f /etc/default/dcache ] && . /etc/default/dcache
[ -f /etc/dcache.env ] && . /etc/dcache.env

# Set home path
if [ -z "$DCACHE_HOME" ]; then
    DCACHE_HOME="@dcache.home@"
fi
if [ ! -d "$DCACHE_HOME" ]; then
    echo "$DCACHE_HOME is not a directory"
    exit 2
fi

. @dcache.paths.bootloader@/loadConfig.sh

CLASSPATH="$(getProperty dcache.paths.classpath)" \
    ${JAVA} $(getProperty dcache.java.options) \
    diskCacheV111.services.space.Manager $*