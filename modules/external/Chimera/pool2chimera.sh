#!/bin/sh

if [ $# -ne 1 ]
then
    echo "Usage: `basename $0` <pool base dir>"
    echo "     convers id in pool to corresponding chimera id's."
    exit 1
fi

if [  -z ${ourHomeDir} ]
then
    ourHomeDir=/opt/d-cache
fi


. ${ourHomeDir}/classes/extern.classpath
. ${ourHomeDir}/config/dCacheSetup

${java} ${java_options} -classpath ${externalLibsClassPath} \
              -Xmx512M org.dcache.chimera.migration.PoolRepository2Chimera ${ourHomeDir}/config/chimera-config.xml $1

