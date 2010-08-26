#!/bin/sh


if [ $# -lt 2 ]
then
    echo "Usage <command> <path> [options]"
    exit 1
fi

command=$1
shift



if [  -z ${ourHomeDir} ]
then
    ourHomeDir=/opt/d-cache
fi

. ${ourHomeDir}/classes/extern.classpath
. ${ourHomeDir}/config/dCacheSetup

 ${java} ${java_options} -classpath ${ourHomeDir}/classes/cells.jar:${externalLibsClassPath} \
     org.dcache.chimera.examples.cli.${command} ${ourHomeDir}/config/chimera-config.xml $*
