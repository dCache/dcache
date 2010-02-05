#
#  defined :
#     ourBaseName
#     setupFilePath
#     thisDir
#     ourHomeDir
#     config
#     pidDir
#
if [ ! -z "${pool}" ] ; then
     poolFile=${config}/${pool}
     if [ ! -f ${poolFile} ] ; then
        if [ -f ${poolFile}.poollist ] ; then
           poolFile=${poolFile}.poollist
        else
           echo "Pools file not found : ${poolFile}" 1>&2
           exit 4
        fi
     fi
     POOL="pool=${poolFile}"
     x=`echo ${pool} | awk -F. '{print $1}'`
     domainName=${x}Domain
elif [ -f ${config}/${ourBaseName}.poollist ] ; then
     POOL="pool=${poolFile}"
     domainName=${ourBaseName}Domain
else
     domainName=${ourBaseName}Domain
fi
if [ ! -z "${domain}" ] ; then
    domainName=${domain}
    base=`expr "${domainName}" : "\(.*\)Domain"`
    if [ -z "$base" ] ; then base=${domainName} ; fi
fi
if [ -z  "${logfile}" ] ; then
   if [ -z "${logArea}" ] ; then
     logfile=/tmp/${domainName}.log
   else
     logfile=${logArea}/${domainName}.log
   fi
fi

stopFile="/tmp/.dcache-stop.${domainName}"
javaPidFile="${pidDir}/dcache.${domainName}-java.pid"
daemonPidFile="${pidDir}/dcache.${domainName}-daemon.pid"

getPid() # in $1=pidfile, out $2=pid
{
    if [ ! -f "$1" ] ; then
        echo "Cannot find appropriate PID file ($1)" 1>&2
        exit 1
    fi
    pid=`cat "$1"`
    if [ -z "$pid" ] ; then
        echo "$1 does not contain valid PID" 1>&2
        exit 1
    fi
    eval $2=$pid
}


#
#
procStop()
{
    getPid "$daemonPidFile" daemonPid
    getPid "$javaPidFile" javaPid

    # Don't do anything if not running
    ps -p $daemonPid  1>/dev/null 2>/dev/null || exit 0

    # Fail if we don't have permission to signal the daemon
    kill -0 $daemonPid || { echo "Failed to kill ${domainName}"; exit 1; }

    # Stopping a dCache domain for good requires that we supress the
    # automatic restart by creating a stop file.
    touch "$stopFile" 2>/dev/null
    kill -TERM $javaPid 1>/dev/null 2>/dev/null || true

    printf "Stopping ${domainName} (pid=$javaPid) "
    for c in  0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
        ps -p $daemonPid 1>/dev/null 2>/dev/null || {
            rm -f "$daemonPidFile" "$javaPidFile"
            echo "Done"
            return
        }
        printf "$c "
        sleep 1
        if [ $c -eq 9 ] ; then
            kill -9 $javaPid 1>/dev/null 2>/dev/null || true
        fi
    done
    echo
    echo "Giving up : ${domainName} might still be running" 1>&2
    exit 4
}

isRunning() # in $1=pidFile
{
    [ -f "$1" ] && ps -p `cat "$1"` 1>/dev/null 2>/dev/null
}

procStart()
{
    if isRunning "${daemonPidFile}"; then
        echo "${domainName} might still be running" 1>&2
        exit 4
    fi

    touch "${logfile}" || {
        echo "Could not write to logfile ${logfile}" 1>&2
        exit 1
    }

    if [ ! -z "${telnetPort}" ]; then
        TELNET_PORT="-telnet ${telnetPort}"
    elif [ ! -z "${telnet}" ]; then
        TELNET_PORT="-telnet ${telnet}"
    fi

    if [ ! -z "${batch}" ]; then
        batchFile=${batch}
    else
        batchFile=${config}/${ourBaseName}.batch
    fi

    if [ -f "$batchFile" ]; then
        BATCH_FILE="-batch ${batchFile}"
    else
        echo "Batch file does not exist: ${batchFile}, cannot continue ..." 1>&2
        exit 5
    fi

    if [ ! -z "${debug}" ]; then
        DEBUG="-debug"
    fi

    CLASSPATH="${classpath}:${thisDir}/../..:${thisDir}/../classes/cells.jar:${thisDir}/../classes/dcache.jar:${externalLibsClassPath}"
    LD_LIBRARY_PATH="${librarypath}"
    export LD_LIBRARY_PATH

# use local jre ( ${ourHomeDir}/jre if it exist. This will allows to
# us to package required jre with dCache.

    if [ -x "${ourHomeDir}/jre/bin/java" ]; then
        java="${ourHomeDir}/jre/bin/java"
    fi

# we need to activate the globus gsi key pair caching for those installations that
# continue to use  dCacheSetup where -Dorg.globus.jglobus.delegation.cache.lifetime is not
# specified
    echo "${java_options}" | grep "\-Dorg\.globus\.jglobus\.delegation\.cache\.lifetime=" >/dev/null || {
        java_options="${java_options} \
                 -Dorg.globus.jglobus.delegation.cache.lifetime=30000"
    }

# Add a java option for an endorsed directory
    java_options="${java_options} \
                -Djava.endorsed.dirs=${ourHomeDir}/classes/endorsed \
                dmg.cells.services.Domain ${domainName} $TELNET_PORT \
                -param setupFile=${setupFilePath} \
                       ourHomeDir=${ourHomeDir} \
                       ourName=${ourBaseName} \
                       ${POOL} \
                $BATCH_FILE $DEBUG"

#
#  echo "(Using classpath : $CLASSPATH )"
#  echo "(Using librarypath : $LD_LIBRARY_PATH )"
#  echo "${java} ${java_options}"
    [ "${logMode}" = "new" ] && mv -f "${logfile}" "${logfile}.old"

    if [ -f "${config}/delay" ] ; then
        delay=`cat "${config}/delay"`
    else
        delay=10
    fi

    rm -f "$stopFile"
    CLASSPATH=${CLASSPATH} /bin/sh ${ourHomeDir}/share/lib/daemon ${user:+-u} ${user:+"$user"} -r "${stopFile}" -d "${delay}" -f -c "${javaPidFile}" -p "${daemonPidFile}" -o "${logfile}" "${java}" ${java_options}

    printf "Starting ${domainName} "
    for c in 6 5 4 3 2 1 0; do
        if isRunning "${javaPidFile}"; then
            break
        fi

        sleep 1
        printf "$c "
    done

    if isRunning "${javaPidFile}"; then
        echo "Done"
    else
        echo "failed"
        grep PANIC "${logfile}"
        exit 4
    fi
}

procHelp() {
   echo "Usage : ${ourBaseName} start|stop"
   exit 4
}

procSwitch() {
   case "$1" in
      *start)       shift 1 ; procStart $* ;;
      *stop)        procStop  ;;
      *) procHelp $*
      ;;
   esac
}
