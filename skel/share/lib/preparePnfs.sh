#!/bin/bash

# Default Log level
# Standard logging levels are
# ABORT ERROR WARNING INFO DEBUG
loglevel=14


# Default Log File
logfile=""


usage()
{
  echo "Prepares PNFS for use with dCache. preparePnfs.sh only needs to"
  echo "be run once on the node hosting PnfsManager."
  echo ""
  echo "-h --help             Show this help"
  echo "-l LEVEL --loglevel LEVEL   Set loglevel (by default '15')"
  echo "                        debug level ABORT=45"
  echo "                        debug level ERROR=35"
  echo "                        debug level WARNING=25"
  echo "                        debug level INFO=15"
  echo "                        debug level DEBUG=5"
  echo
  echo "--serverid SERVERID   defaults to domain name"
  echo "--root PNFSROOT       defaults to /pnfs"
  echo "--server PNFSSERVER   defaults to localhost"
  echo "--dcap PORT           defaults to 22125"
}

logmessage()
{
  local ThisLogLevel
  local ThisPrefix
  let ThisLogLevel=$1
  if [ "$1" == "NONE" ] ; then
    ThisLogLevel=55
  fi
  if [ "$1" == "ABORT" ] ; then
    ThisLogLevel=45
  fi
  if [ "$1" == "ERROR" ] ; then
    ThisLogLevel=35
  fi
  if [ "$1" == "WARNING" ] ; then
    ThisLogLevel=25
  fi
  if [ "$1" == "INFO" ] ; then
    ThisLogLevel=15
  fi
  if [ "$1" == "DEBUG" ] ; then
    ThisLogLevel=5
  fi
  if [ $ThisLogLevel -lt 60 ] ; then
    ThisPrefix="NONE:"
  fi
  if [ $ThisLogLevel -lt 50 ] ; then
    ThisPrefix="ABORT:"
  fi
  if [ $ThisLogLevel -lt 40 ] ; then
    ThisPrefix="ERROR:"
  fi
  if [ $ThisLogLevel -lt 30 ] ; then
    ThisPrefix="WARNING:"
  fi
  if [ $ThisLogLevel -lt 20 ] ; then
    ThisPrefix="INFO:"
  fi
  if [ $ThisLogLevel -lt 10 ] ; then
    ThisPrefix="DEBUG:"
  fi
  if [ $# -ge 1 ]
  then
    shift 1
    if [ ${ThisLogLevel} -gt ${loglevel} ]
    then
      if [ $# -eq 1 ]; then
        echo ${ThisPrefix}$*
        if [ "${logfile}" != "" ]
        then
          echo ${ThisPrefix}$* >> ${logfile}
        fi
      fi
      if [ $# -eq 0 ]; then
        while read logline
	do
	  echo ${ThisPrefix}${logline}
	  if [ "${logfile}" != "" ]
          then
            echo ${ThisPrefix}${logline} >> ${logfile}
          fi
	done
      fi
    fi
  fi
}

getServerId()
{
    RET=$(domainname_os)
    if [ -z "${RET}" ]; then
        RET=$(sed -e 's/#.*$//' /etc/resolv.conf | awk '/^[ \t]*search/ { print $2 }')
        if [ -z "${RET}" ]; then
            RET=$(sed -e 's/#.*$//' /etc/resolv.conf | awk '/^[ \t]*domain/ { print $2 }')
        fi
    fi
}

domainname_os() {
    case $(uname) in
        Linux)
            echo $(hostname -d)
            ;;
        SunOS)
            echo $(/usr/lib/mail//sh/check-hostname |cut -d" " -f7 | awk -F. '{ for( i=2; i <= NF; i++){ printf("%s",$i); if( i  <NF) printf("."); } } ')
            ;;
        Darwin)
            echo $(hostname | sed -e 's/[^\.]*//' | sed -e 's/^\.//')
            ;;
    esac
}


fqdn_os() {
    case $(uname) in
        Linux)
            echo $(hostname --fqdn)
            ;;
        SunOS)
            echo $(/usr/lib/mail/sh/check-hostname |cut -d" " -f7)
            ;;
        Darwin)
            echo $(hostname)
            ;;
    esac
}

dcacheInstallGetPnfsMountPoint()
{
  RET=${PNFS_ROOT}/${SERVER_ID}
}

dcacheInstallGetExportPoint()
{
  local exportPoint
  local pnfsMountPoint

  dcacheInstallGetPnfsMountPoint
  pnfsMountPoint=$RET

  exportPoint=$(mount | grep ${pnfsMountPoint} | awk '{print $1}' | awk -F':' '{print $2}')
  RET=${exportPoint}
}


absfpath () {
  CURDIR="$(pwd)"
  cd $(dirname $1)
  ABSPATH="$(pwd)/$(basename $1)"
  cd "$CURDIR"
  echo ${ABSPATH}
}

dcacheNameServerIs()
{
  if [ "${PNFS_SERVER}" == "localhost" ]
  then
    return 0
  fi
  if [ "${PNFS_SERVER}" == $(fqdn_os) ]
  then
    return 0
  fi
  return 1
}

dcacheInstallPnfsMountPointServer()
{
  logmessage DEBUG "dcacheInstallPnfsMountPointServer.start"
  local pnfsMountPoint
  local serverIdLinkedTo
  local ftpBaseLinkedTo
  #    Checking and creating mountpoint and link
  #
  if [ -z "${PNFS_SERVER}" ] ; then
    logmessage ERROR "Unable to determine name space server. Install has failed."
    exit 1
  fi
  pnfsMountPoint=${PNFS_ROOT}/fs
  # if not a directory
  if [ ! -d "${pnfsMountPoint}" ]; then
    # if exists directory
    if [ -e "${pnfsMountPoint}" ] ; then
      logmessage ERROR "The file ${pnfsMountPoint} is in the way. Please move it out of the way"
      logmessage ERROR "and call me again. Exiting."
      exit 1
    else
      logmessage INFO "Creating pnfs mount point (${pnfsMountPoint})"
	mkdir -p ${pnfsMountPoint}
    fi
  fi

  cd ${PNFS_ROOT}
  # if file is not a symbolic link
  if [ ! -L "${SERVER_ID}" ]; then
    # if file exists
    if [ -e "${SERVER_ID}" ] ; then
      logmessage ERROR "The file/directory ${PNFS_ROOT}/${SERVER_ID} is in the way. Please move it out"
      logmessage ERROR "of the way and call me again. Exiting."
    else
      logmessage INFO "Creating link ${PNFS_ROOT}/${SERVER_ID} --> ${pnfsMountPoint}/usr/"
      ln -s fs/usr ${SERVER_ID}
      echo MADE THE SYMBOLIC LINK
    fi
  fi

  cd ${PNFS_ROOT}
  if [ ! -L "${SERVER_ID}" -a ! -e "${SERVER_ID}" ] ; then
    logmessage INFO "Creating link ${PNFS_ROOT}/${SERVER_ID} --> ${PNFS_ROOT}/fs/usr."
    ln -s fs/usr ${SERVER_ID}
  else
    serverIdLinkedTo=$(find ${PNFS_ROOT}/${SERVER_ID} -type l | xargs readlink)
    if [ "${serverIdLinkedTo}" = "fs/usr" -o "${serverIdLinkedTo}" = "${PNFS_ROOT}/fs/usr" ] ; then
      logmessage INFO "Link ${PNFS_ROOT}/${SERVER_ID} --> ${PNFS_ROOT}/fs/usr already there."
    else
      logmessage ERROR "[ERROR] Link ${PNFS_ROOT}/${SERVER_ID} --> ${PNFS_ROOT}/fs/usr cannot be created."
      logmessage ERROR "Please move ${PNFS_ROOT}/${SERVER_ID} and run me again. Exiting."
      exit 1
    fi
  fi

  cd ${PNFS_ROOT}
  if [ ! -L ${PNFS_ROOT}/ftpBase -a ! -e ${PNFS_ROOT}/ftpBase ] ; then
    logmessage INFO "[INFO]  Creating link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} which is used by the GridFTP door."
    ln -s ${pnfsMountPoint} ${PNFS_ROOT}/ftpBase
  else
    ftpBaseLinkedTo=$(ls -l ${PNFS_ROOT}/ftpBase | awk '{print $NF}')
    if [ "${ftpBaseLinkedTo}" = "${pnfsMountPoint}" ] ; then
      logmessage INFO "Link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} already there."
    else
      logmessage ERROR "Link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} cannot be created. and is needed by the GridFTP door."
      logmessage ERROR "Please move ${PNFS_ROOT}/ftpBase and run me again. Exiting."
      exit 1
    fi
  fi

  # Checking for existance of rpcinfo
  #

  which rpcinfo 2>&1 > /dev/null
  RC=$?
  if [ ${RC} -ne 0 ] ; then
    logmessage ERROR "rpcinfo is not on the path"
    exit 1
  fi

  #    Checking and maybe starting pnfs server
  #

  RETVAL=0
  rpcinfo -u localhost 100003 >/dev/null 2>&1
  RETVAL=$?
  if [ ${RETVAL} -eq 1 ]; then
    logmessage INFO "PNFS is not running. It is needed to prepare dCache. ... "
    exit 1
  fi

  #    Checking pnfs mount and possibly mounting
  #
  cp=$(df "${pnfsMountPoint}" 2>/dev/null | grep "${pnfsMountPoint}" | awk '{print $2}')
  if [ -z ${cp} ]; then
    logmessage INFO "${pnfsMountPoint} mount point exists, but is not mounted - going to mount it now ..."
    mount -o intr,rw,noac,hard,nfsvers=2 ${PNFS_SERVER}:/fs ${pnfsMountPoint}
  fi
  cp=$(df "${pnfsMountPoint}" 2>/dev/null | grep "${pnfsMountPoint}" | awk '{print $2}')
  if [ -z $cp ]; then
    logmessage ERROR "Was not able to mount ${PNFS_SERVER}:/fs to ${pnfsMountPoint}. Exiting."
    exit 1
  fi

  dcacheInstallPnfsConfigCheck

  logmessage DEBUG "dcacheInstallPnfsMountPointServer.stop"
}

dcacheInstallPnfsConfigCheck()
{
  logmessage DEBUG "dcacheInstallPnfsConfigCheck.start"
  # Checking if pnfs config exists
  #
  local fqHostname
  local WRITING_PNFS
  local serverRoot
  fqHostname=$(fqdn_os)
  logmessage INFO "Checking on a possibly existing dCache/PNFS configuration ..."
  if [ -f "${PNFS_ROOT}/fs/admin/etc/config/serverRoot" ]; then
    WRITING_PNFS=no
  else
    WRITING_PNFS=yes
  fi
  logmessage DEBUG "WRITING_PNFS=$WRITING_PNFS"

  #     Writing new pnfs configuration
  #
  if [ "${WRITING_PNFS}" = "yes" ] ; then

    cd ${PNFS_ROOT}/fs
    serverRoot=$(cat ".(id)(usr)")
    logmessage DEBUG serverRoot=$serverRoot
    #     Writing Wormhole information
    #
    logmessage DEBUG "Changing directory to ${PNFS_ROOT}/fs/admin/etc/config"
    cd ${PNFS_ROOT}/fs/admin/etc/config

    echo "${fqHostname}" > ./serverName
    echo "${SERVER_ID}" >./serverId
    echo "$serverRoot ." > ./serverRoot

    touch ".(fset)(serverName)(io)(on)"
    touch ".(fset)(serverId)(io)(on)"
    touch ".(fset)(serverRoot)(io)(on)"

    echo "${fqHostname}" > ./serverName
    echo "${SERVER_ID}" >./serverId
    echo "$serverRoot ." > ./serverRoot

    mkdir -p dCache
    cd dCache
    echo "${fqHostname}:${DCAP_PORT}" > ./dcache.conf
    touch ".(fset)(dcache.conf)(io)(on)"
    echo "${fqHostname}:${DCAP_PORT}" > ./dcache.conf

    #     Configure directory tags
    #
    cd ${PNFS_ROOT}/fs/usr/data
    echo "StoreName myStore" > ".(tag)(OSMTemplate)"
    echo "STRING" > ".(tag)(sGroup)"
  fi
  logmessage DEBUG "dcacheInstallPnfsConfigCheck.stop"
}

# Defaults
getServerId; SERVER_ID="$RET"
PNFS_ROOT="/pnfs"
PNFS_SERVER="localhost"
DCAP_PORT=22125

# now process the command line
while [ $# -ne 0 ]
do
    case "$1" in
        --help|-h)
            # set dCache install location
            usage
            exit 0
            ;;
        --loglevel|-l)
            # set dCache install location
            loglevel=$2
            shift 2
            ;;
        --logfile)
            logfile=$2
            shift 2
            ;;
        --serverid)
            SERVER_ID=$2
            shift 2
            ;;
        --root)
            PNFS_ROOT=$2
            shift 2
            ;;
        --server)
            PNFS_SERVER=$2
            shift 2
            ;;
        --dcap)
            DCAP_PORT=$2
            shift 2
            ;;
        *)
            shift 1
            ;;
    esac
done

if [ -z "$SERVER_ID" ]; then
    echo "Server ID is not defined; please use --serverid"
    exit 2
fi

dcacheInstallPnfsMountPointServer
