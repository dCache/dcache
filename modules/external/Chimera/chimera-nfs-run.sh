#!/bin/sh

# chkconfig: 345 25 75
# description: chimera nfs3 startup script


if [  -z ${ourHomeDir} ]
then
    ourHomeDir=/opt/d-cache
fi

log=/var/log/chimera-nfsv3.log
pfile=/var/run/chimera-nfsv3.pid

. ${ourHomeDir}/classes/extern.classpath
. ${ourHomeDir}/config/dCacheSetup


waitForChimera()
{
    echo -n "Waiting for NFS server to register itself: "
    for try in 1 2 3 4 5 6 7 too-much; do 
        rpcinfo -p localhost|grep 100003 >/dev/null
        if [ $? == 0 ]; then
            echo
            break
        else
            echo -n "."
        fi
	
        if [ "x$try"  = "xtoo-much" ]; then
            echo
            echo "Chimera is taking too long; giving up."
            break
        else
            sleep 1
        fi
    done
}


dCacheChimeraStart()
{
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

  waitForChimera
}


dCacheChimeraStop()
{
  if [ -f ${pfile} ]
  then
    pid=`cat ${pfile}`
    kill -0 ${pid} > /dev/null 2>&1
    if [ $? -eq 0 ]
    then
      echo "Shutting down Chimera-NFSv3 interface"
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
}
case $1 in

    start)
        dCacheChimeraStart
        ;;

    stop)
        dCacheChimeraStop
        ;;

    restart)
        dCacheChimeraStop
        dCacheChimeraStart
        ;;
    *)
        echo "Usage: `basename $0` [start | stop | restart]"
        exit 1;

esac
