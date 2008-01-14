#!/bin/sh

if [ $# -ne 2 ]
then
    echo "Usage: `basename $0` <pnfs path> <chimera path>"
    echo "     'copy' content of pnfs path into chimera path."
    echo "     Only name space is copied. No data are moved."
    exit 1
fi

if [  -z ${ourHomeDir} ]
then
    ourHomeDir=/opt/d-cache
fi


. ${ourHomeDir}/classes/extern.classpath
. ${ourHomeDir}/config/dCacheSetup

${java} ${java_options} -classpath ${externalLibsClassPath} \
              -Xmx512M org.dcache.chimera.migration.Pnfs2Chimera ${ourHomeDir}/config/chimera-config.xml $1 $2

