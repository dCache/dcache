#!/bin/sh


if [ $# -lt 2 ]
then
    echo "Usage <command> <path> [options]"
    exit 1
fi

command=$1
shift

# Initialize environment. /etc/default/ is the normal place for this
# on several Linux variants. For other systems we provide
# /etc/dcache.env. Those files will typically declare JAVA_HOME and
# DCACHE_HOME and nothing else.
[ -f /etc/default/dcache ] && . /etc/default/dcache
[ -f /etc/dcache.env ] && . /etc/dcache.env

# Set home path
if [ -z "$DCACHE_HOME" ]; then
    DCACHE_HOME="/opt/d-cache"
fi
if [ ! -d "$DCACHE_HOME" ]; then
    echo "$DCACHE_HOME is not a directory"
    exit 2
fi
ourHomeDir="${DCACHE_HOME}"   # We still use ourHomeDir in some places

# Load libraries
. "${DCACHE_HOME}/share/lib/paths.sh"
. "${DCACHE_LIB}/utils.sh"
. "${DCACHE_LIB}/services.sh"

# Check for java
if ! findJavaTool java; then
    fail 1 "Could not find usable Java VM. Please set JAVA_HOME."
fi
JAVA="$java"

loadConfig "dcache.*"

# Build classpath
classpath="${DCACHE_HOME}/classes/cells.jar"
if [ "$DCACHE_JAVA_CLASSPATH" ]; then
    classpath="${classpath}:${DCACHE_JAVA_CLASSPATH}"
fi
if [ -r "${DCACHE_HOME}/classes/extern.classpath" ]; then
    . "${DCACHE_HOME}/classes/extern.classpath"
    classpath="${classpath}:${externalLibsClassPath}"
fi

CLASSPATH="$classpath" ${JAVA} ${DCACHE_JAVA_OPTIONS}  \
     org.dcache.chimera.examples.cli.${command} ${DCACHE_HOME}/config/chimera-config.xml $*
