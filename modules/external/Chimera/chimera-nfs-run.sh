#!/bin/sh

# chkconfig: - 92 8
# description: chimera nfs3 startup script


if [  -z ${ourHomeDir} ]
then
    ourHomeDir=/opt/d-cache
fi

log=/var/log/chimera-nfsv3.log
pfile=/var/run/chimera-nfsv3.pid

. ${ourHomeDir}/classes/extern.classpath
. ${ourHomeDir}/config/dCacheSetup

case $1 in

    start)
        if [ -f ${pfile} ]
        then
            pid=`cat ${pfile}`
            kill -0 ${pid} > /dev/null 2>&1
            if [ $? -eq 0 ]
            then
                echo "Old NFS process still running"
                exit 1
            fi
        fi

        echo "Starting Chimera-NFSv3 interface"
        ${java} ${java_options} -classpath ${externalLibsClassPath} \
              -Xmx512M org.dcache.chimera.nfs.v3.Main2 \
              ${ourHomeDir}/config/chimera-config.xml > ${log} 2>&1 &
        echo $! > ${pfile}
        ;;

    stop)
        if [ -f ${pfile} ]
        then
            pid=`cat ${pfile}`
            kill -0 ${pid} > /dev/null 2>&1
            if [ $? -eq 0 ]
            then
                echo "Shuting down Chimera-NFSv3 interface"
                kill `cat ${pfile}`
                rm -f ${pfile}
            else
                echo "NFS process not running"
                exit 1
            fi
        else
            echo "Pid file missing. NFS process not running?"
            exit 1;
        fi
        ;;

    restart)
        stop
        start
        ;;
    *)
        echo "Usage: `basename $0` [start | stop | restart]"
        exit 1;

esac
