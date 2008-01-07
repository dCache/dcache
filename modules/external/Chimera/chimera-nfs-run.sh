#!/bin/sh


if [  -z ${ourHomeDir} ]
then
    ourHomeDir=/opt/d-cache
fi

. ${ourHomeDir}/classes/extern.classpath
. ${ourHomeDir}/config/dCacheSetup

case $1 in

    start)
        echo "Starting Chimera-NFSv3 interface"
        ${java} ${java_options} -classpath ${externalLibsClassPath} \
              -Xmx512M org.dcache.chimera.nfsv3.Main2 \
              ${ourHomeDir}/config/chimera-config.xml > /tmp/chimera-nfsv3.log 2>&1 &
        echo $! > /var/run/chimera-nfsv3.pid        
        ;;
    stop)
        echo "Shuting down Chimera-NFSv3 interface"
        kill `cat /var/run/chimera-nfsv3.pid`
        rm -f /var/run/chimera-nfsv3.pid
        ;;
    restart)
        stop
        start
        ;;
    *)
        echo "Usage: `basename $0` [start | stop | restart]"
        exit 1;

esac
