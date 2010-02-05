#!/bin/bash

# Set up the preset values before we get to the command line
# Note these values can be changed with the command line

# Default Log level
# Standard logging levels are
# ABORT ERROR WARNING INFO DEBUG
loglevel=14


# Default Log File

logfile=""



usage()
{
  echo "dCache install script"
  echo ""
  echo "-h --help             Show this help"
  echo "-p PATH  --prefix   PATH    Set install prefix (by default '/opt/dcache')"
  echo "-l LEVEL --loglevel LEVEL   Set loglevel (by default '15')"
  echo "                        debug level ABORT=45"
  echo "                        debug level ERROR=35"
  echo "                        debug level WARNING=25"
  echo "                        debug level INFO=15"
  echo "                        debug level DEBUG=5"

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


check_shell()
{
  if [ "$BASH_VERSION" = "" ] ; then
    logmessage ERROR "bash was not detected script exiting."
    exit 1
  fi
}

check_os()
{
  local osname
  RET="Unknown"
  osname=`uname`
  if [ "$osname" = "Linux" ] ; then
    RET=Linux
  fi
  if [ "$osname" = "Darwin" ] ; then
    RET=Darwin
  fi
  if [ "$osname" = "SunOS" ] ; then
    RET=SunOS
  fi
}

checkReadlinkf()
{
  if [ ! "$hasReadlinkf" ] ; then
    readlink -f . > /dev/null 2>&1
    hasReadlinkf=$?
  fi
  return $hasReadlinkf
}

# Get the canonical path of $1. Only returns a truely canonical path
# if readlink is available. Otherwise an absolute path which does not
# end in a symlink is returned.
getCanonicalPath() # $1 = path
{
    local link
    link="$1"
    if checkReadlinkf ; then
        RET="$(readlink -f $link)"
    else
        RET="$(cd $(dirname $link); pwd)/$(basename $link)"
        while [ -h "$RET" ]; do
            link="$(ls -ld $RET | sed 's/.*-> //')"
            if [ -z "${link##/*}" ]; then
                RET="${link}"
            else
                link="$(dirname $RET)/${link}"
                RET="$(cd $(dirname $link); pwd)/$(basename $link)"
            fi
        done
    fi
}

check_install()
{
  if [ ! -r ${DCACHE_HOME}/etc/node_config ]; then
    logmessage ABORT "${DCACHE_HOME}/etc/node_config missing."
    logmessage INFO "Copy ${DCACHE_HOME}/etc/node_config.template to ${DCACHE_HOME}/etc/node_config and customize it before running the install script. Exiting."
    exit 4
  fi
}





shortname_os() {
    case `uname` in
        Linux)
            echo `hostname -s`
            ;;
        SunOS)
            echo `uname -n`
            ;;
        Darwin)
            echo `hostname -s`
            ;;
    esac
}

domainname_os() {
    case `uname` in
        Linux)
            echo `hostname -d`
            ;;
        SunOS)
            echo `/usr/lib/mail//sh/check-hostname |cut -d" " -f7 | awk -F. '{ for( i=2; i <= NF; i++){ printf("%s",$i); if( i  <NF) printf("."); } } '`
            ;;
        Darwin)
            echo `hostname | sed -e 's/[^\.]*//' | sed -e 's/^\.//'`
            ;;
    esac
}


fqdn_os() {
    case `uname` in
        Linux)
            echo `hostname --fqdn`
            ;;
        SunOS)
            echo `/usr/lib/mail/sh/check-hostname |cut -d" " -f7`
            ;;
        Darwin)
            echo `hostname`
            ;;
    esac
}




dcacheInstallGetPnfsMountPoint()
{
  RET=${NODE_CONFIG_PNFS_ROOT}/${SERVER_ID}
}

dcacheInstallGetExportPoint()
{
  local exportPoint
  local pnfsMountPoint

  dcacheInstallGetPnfsMountPoint
  pnfsMountPoint=$RET

  exportPoint=`mount | grep ${pnfsMountPoint} | awk '{print $1}' | awk -F':' '{print $2}'`
  RET=${exportPoint}
}


absfpath () {
  CURDIR=`pwd`
  cd `dirname $1`
  ABSPATH=`pwd`/`basename $1`
  cd "$CURDIR"
  echo ${ABSPATH}
}

dcacheInstallGetNameSpaceType()
{
  if [ "${NODE_CONFIG_NAMESPACE}" != "pnfs" -a "${NODE_CONFIG_NAMESPACE}" != "chimera" ]
  then
    logmessage WARNING "node_config does not have NAMESPACE set to chimera or pnfs."
    logmessage INFO "NAMESPACE=${NODE_CONFIG_NAMESPACE}"
    nameServerFormat="pnfs"
    logmessage WARNING "Defaulting node_config to pnfs. This behaviour will change in future dCache releases."
  else
    nameServerFormat=$NODE_CONFIG_NAMESPACE
  fi
  RET=$nameServerFormat
}


dcacheInstallGetNameSpaceServer()
{
  getNameSpaceServer RET
}

dcacheNameServerIs()
{
  dcacheInstallGetNameSpaceServer
  pnfsHost="${RET}"
  if [ "${pnfsHost}" == "localhost" ]
  then
    return 1
  fi
  if [ "${pnfsHost}" == `fqdn_os` ]
  then
    return 1
  fi
  return 0
}




dcacheInstallPnfsMountPointClient()
{
  logmessage DEBUG "dcacheInstallPnfsMountPointClient.start"
  local pnfsMountPoint
  local NAMESPACE_NODE
  local exportPoint
  dcacheInstallGetPnfsMountPoint
  pnfsMountPoint=${RET}
  dcacheInstallGetNameSpaceServer
  NAMESPACE_NODE=$RET
  if [ -z "${NAMESPACE_NODE}" ] ; then
    logmessage ERROR "Unable to determine name space server. Install failed."
    exit 1
  fi
  logmessage INFO "Checking if ${pnfsMountPoint} mounted to the right export. ..."
  dcacheInstallGetExportPoint
  exportPoint=$RET
  if [ "${exportPoint}" = '/pnfsdoors' ] ; then
    logmessage INFO "OK"
  else
    if [ "${exportPoint}" != "" ] ; then
      logmessage WARNING "${pnfsMountPoint} mounted, however not to ${NAMESPACE_NODE}:/pnfsdoors."
      logmessage INFO "Unmounting it now:"
      umount ${pnfsMountPoint}

    fi
    if [ -L "${pnfsMountPoint}" ] ; then
      logmessage INFO "Trying to remove symbolic link ${pnfsMountPoint} :"
      rm -f ${pnfsMountPoint}
      if [ $? -eq 0 ] ; then
        logmessage INFO "'rm -f ${pnfsMountPoint}' went fine."
      else
        logmessage ABORT "'rm -f ${pnfsMountPoint}' failed. Please move it out of the way manually"
        logmessage ERROR "and run me again. Exiting."
        exit 1
      fi
    fi
    if [ ! -d "${pnfsMountPoint}" ]; then
      if [ -e "${pnfsMountPoint}" ] ; then
        logmessage ABORT "The file ${pnfsMountPoint} is in the way. Please move it out of the way"
        logmessage ERROR "and call me again. Exiting."
        exit 1
      else
        logmessage INFO "Creating pnfs mount point (${pnfsMountPoint})"
        mkdir -p ${pnfsMountPoint}
      fi
    fi
    logmessage INFO "Will be mounted to ${NAMESPACE_NODE}:/pnfsdoors by dcache start-up script."
  fi
  if [ ! -L "${NODE_CONFIG_PNFS_ROOT}/ftpBase" -a ! -e "${NODE_CONFIG_PNFS_ROOT}/ftpBase" ] ; then
    logmessage INFO "Creating link ${NODE_CONFIG_PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} which is used by the GridFTP door."
    ln -s ${pnfsMountPoint} ${NODE_CONFIG_PNFS_ROOT}/ftpBase
  else
    local ftpBaseLinkedTo
    ftpBaseLinkedTo=`find ${NODE_CONFIG_PNFS_ROOT}/ftpBase -type l | xargs readlink`
    logmessage INFO "ftpBaseLinkedTo=$ftpBaseLinkedTo"
    logmessage INFO "pnfsMountPoint=${pnfsMountPoint}"
    if [ "${ftpBaseLinkedTo}" == "${pnfsMountPoint}" ] ; then
      logmessage INFO "Link ${NODE_CONFIG_PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} already there."
    else
      logmessage ABORT "Link ${NODE_CONFIG_PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} cannot be created. Needed by the GridFTP door."
      logmessage ERROR "Please move ${NODE_CONFIG_PNFS_ROOT}/ftpBase and run this script again. Exiting."
      exit 1
    fi
  fi
  if ! grep "^ftpBase=${NODE_CONFIG_PNFS_ROOT}/ftpBase" ${DCACHE_HOME}/config/dCacheSetup 2>/dev/null >/dev/null ; then
    logmessage WARNING "The file ${DCACHE_HOME}/config/dCacheSetup does not contain:"
    logmessage WARNING "   ftpBase=${NODE_CONFIG_PNFS_ROOT}/ftpBase"
    logmessage WARNING "Make shure it is set correctly before you start dCache."
  fi

  if mount | grep pnfs 2>/dev/null >/dev/null ; then
    logmessage WARNING "A pnfs export is already mounted. The GridFTP door will only use the"
    logmessage WARNING "mount at ${pnfsMountPoint} which will be mounted by the start-up script."
    logmessage WARNING "You might want to remove any mounts not needed anymore."
  fi
  logmessage DEBUG "dcacheInstallPnfsMountPointClient.stop"

}


dcacheInstallPnfsMountPointServer()
{
  logmessage DEBUG "dcacheInstallPnfsMountPointServer.start"
  local pnfsMountPoint
  local serverIdLinkedTo
  local ftpBaseLinkedTo
  dcacheInstallGetNameSpaceServer
  pnfsServer=$RET
  #    Checking and creating mountpoint and link
  #
  if [ -z "${pnfsServer}" ] ; then
    logmessage ERROR "Unable to determine name space server. Install has failed."
    exit 1
  fi
  pnfsMountPoint=${NODE_CONFIG_PNFS_ROOT}/fs
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

  logmessage INFO "Will be mounted to ${pnfsServer}:/fs by dcache start-up script."

  cd ${NODE_CONFIG_PNFS_ROOT}
  # if file is not a symbolic link
  if [ ! -L "${SERVER_ID}" ]; then
    # if file exists
    if [ -e "${SERVER_ID}" ] ; then
      logmessage ERROR "The file/directory ${NODE_CONFIG_PNFS_ROOT}/${SERVER_ID} is in the way. Please move it out"
      logmessage ERROR "of the way and call me again. Exiting."
    else
      logmessage INFO "Creating link ${NODE_CONFIG_PNFS_ROOT}/${SERVER_ID} --> ${pnfsMountPoint}/usr/"
      ln -s fs/usr ${SERVER_ID}
      echo MADE THE SYMBOLIC LINK
    fi
  fi

  cd ${NODE_CONFIG_PNFS_ROOT}
  if [ ! -L "${SERVER_ID}" -a ! -e "${SERVER_ID}" ] ; then
    logmessage INFO "Creating link ${NODE_CONFIG_PNFS_ROOT}/${SERVER_ID} --> ${NODE_CONFIG_PNFS_ROOT}/fs/usr."
    ln -s fs/usr ${SERVER_ID}
  else
    serverIdLinkedTo=`find ${NODE_CONFIG_PNFS_ROOT}/${SERVER_ID} -type l | xargs readlink `
    if [ "${serverIdLinkedTo}" = "fs/usr" -o "${serverIdLinkedTo}" = "${NODE_CONFIG_PNFS_ROOT}/fs/usr" ] ; then
      logmessage INFO "Link ${NODE_CONFIG_PNFS_ROOT}/${SERVER_ID} --> ${NODE_CONFIG_PNFS_ROOT}/fs/usr already there."
    else
      logmessage ERROR "[ERROR] Link ${NODE_CONFIG_PNFS_ROOT}/${SERVER_ID} --> ${NODE_CONFIG_PNFS_ROOT}/fs/usr cannot be created."
      logmessage ERROR "Please move ${NODE_CONFIG_PNFS_ROOT}/${SERVER_ID} and run me again. Exiting."
      exit 1
    fi
  fi

  cd ${NODE_CONFIG_PNFS_ROOT}
  if [ ! -L ${NODE_CONFIG_PNFS_ROOT}/ftpBase -a ! -e ${NODE_CONFIG_PNFS_ROOT}/ftpBase ] ; then
    logmessage INFO "[INFO]  Creating link ${NODE_CONFIG_PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} which is used by the GridFTP door."
    ln -s ${pnfsMountPoint} ${NODE_CONFIG_PNFS_ROOT}/ftpBase
  else
    ftpBaseLinkedTo=`ls -l ${NODE_CONFIG_PNFS_ROOT}/ftpBase | awk '{print $NF}'`
    if [ "${ftpBaseLinkedTo}" = "${pnfsMountPoint}" ] ; then
      logmessage INFO "Link ${NODE_CONFIG_PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} already there."
    else
      logmessage ERROR "Link ${NODE_CONFIG_PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} cannot be created. and is needed by the GridFTP door."
      logmessage ERROR "Please move ${NODE_CONFIG_PNFS_ROOT}/ftpBase and run me again. Exiting."
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
    if [ ! -x ${NODE_CONFIG_PNFS_INSTALL_DIR}/tools/pnfs.server ]; then
      logmessage ERROR "PNFS not installed but needed for dCache admin node installation. Exiting."
      exit 1
    fi
    logmessage INFO "PNFS is not running. It is needed to prepare dCache. ... "
    yesno="${NODE_CONFIG_PNFS_START}"
    if [ \( "${yesno}" = "n" \) -o \( "${yesno}" = "no" \) ] ; then
      logmessage ERROR "Not allowed to start it. Set PNFS_START in etc/node_config to 'yes' or start by hand. Exiting."
      exit 1
    elif [ \( "${yesno}" = "y" \) -o \( "${yesno}" = "yes" \) ] ; then
      logmessage INFO "Trying to start it now:"
      ${NODE_CONFIG_PNFS_INSTALL_DIR}/tools/pnfs.server start
    fi
  fi

  #    Checking pnfs mount and possibly mounting
  #
  cp=`df ${pnfsMountPoint} 2>/dev/null | grep "${pnfsMountPoint}" | awk '{print $2}'`
  if [ -z ${cp} ]; then
    logmessage INFO "${pnfsMountPoint} mount point exists, but is not mounted - going to mount it now ..."
    mount -o intr,rw,noac,hard,nfsvers=2 ${pnfsServer}:/fs ${pnfsMountPoint}
  fi
  cp=`df ${pnfsMountPoint} 2>/dev/null | grep "${pnfsMountPoint}" | awk '{print $2}'`
  if [ -z $cp ]; then
    logmessage ERROR "Was not able to mount ${pnfsServer}:/fs to ${pnfsMountPoint}. Exiting."
    exit 1
  fi
  dcacheNameServerIs
  dcacheNameServerIsRc=$?
  if [ "${dcacheNameServerIsRc}" == "1" ]
  then
    dcacheInstallPnfsConfigCheck
  else
    logmessage DEBUG "This node is not a name space server."
  fi
  logmessage DEBUG "dcacheInstallPnfsMountPointServer.stop"
}

dcacheInstallPnfsMountPoints()
{
  logmessage DEBUG "dcacheInstallPnfsMountPoints.start"
  # Creating /pnfs/fs and Symbolic Link /pnfs/fs/usr to /pnfs/<domain>
  # (e.g. /pnfs/fnal.gov) for GridFTP

  if contains pnfsDomain $DOMAINS; then
      dcacheInstallPnfsMountPointServer
  elif isNameSpaceMountNeeded $DOMAINS; then
      dcacheInstallPnfsMountPointClient
  fi

  logmessage DEBUG "dcacheInstallPnfsMountPoints.stop"
}



dcacheInstallChimeraMountPointServer()
{
  local pnfsServer
  local localhostName
  local tryToMount
  local cmdline
  local counter
  local mountrc
  if [ ! -d "${NODE_CONFIG_PNFS_ROOT}" ] ; then
    mkdir "${NODE_CONFIG_PNFS_ROOT}"
  fi
  dcacheInstallGetNameSpaceServer
  pnfsServer=$RET
  localhostName=`fqdn_os`
  tryToMount=1
  if [ -z "${pnfsServer}" ] ; then
    logmessage ERROR "Unable to determine Name Server node exiting as an error."
    exit 1
  fi
  if [ "$pnfsServer" == "$localhostName" ] ; then
    pnfsServer="localhost"
  fi
  if [ -n "$(mount | grep "${pnfsServer}" )" ] ; then
    tryToMount=0
  fi
  if [ -n "$(mount | grep "/pnfs" )" ] ; then
    tryToMount=0
  fi
  if [ "${tryToMount}" == "0" ] ; then
    logmessage INFO "Already Mounted ${pnfsServer}"
  else
    cmdline="mount -o intr,rw,hard ${pnfsServer}:/pnfs /pnfs"
    counter=1
    logmessage INFO "Need to mount ${pnfsServer}"
    while [ "${tryToMount}" == "1" ] ; do
      $cmdline
      mountrc=$?
      if [ "${mountrc}" == "0" ] ; then
        logmessage INFO "Successflly mounted Chimera running $cmdline"
        tryToMount=0
      else
        let counter="$counter + 1"
        if [ "$counter" == "12" ] ; then
          logmessage ERROR "Failed running $cmdline and giving up"
          tryToMount=0
        else
          logmessage INFO "Trying to mount Chimera failed, will retry."
          logmessage DEBUG "Using command: $cmdline"
        fi
      fi
      sleep 1
    done
  fi
}



dcacheInstallChimeraMountPoints()
{
  logmessage DEBUG "dcacheInstallChimeraMountPoints.start"
  # Creating /pnfs/fs and Symbolic Link /pnfs/fs/usr to /pnfs/<domain>
  # (e.g. /pnfs/fnal.gov) for GridFTP

  if contains chimeraDomain $DOMAINS; then
      /etc/init.d/portmap restart
      dcacheInstallChimeraMountPointServer
  elif isNameSpaceMountNeeded $DOMAINS; then
      dcacheInstallChimeraMountPointServer
  fi

  logmessage DEBUG "dcacheInstallChimeraMountPoints.stop"
}

dcacheInstallMountPoints()
{
  logmessage DEBUG "dcacheInstallMountPoints.start"
  dcacheInstallGetNameSpaceType
  nameServerFormat=${RET}
  if [ "${nameServerFormat}" == "pnfs" ]
  then
    dcacheInstallPnfsMountPoints
  fi
  if [ "${nameServerFormat}" == "chimera" ]
  then
    dcacheInstallChimeraMountPoints
  fi
  logmessage DEBUG "dcacheInstallMountPoints.stop"

}


dcacheInstallPnfsConfigCheck()
{
  logmessage DEBUG "dcacheInstallPnfsConfigCheck.start"
  # Checking if pnfs config exists
  #
  local fqHostname
  local WRITING_PNFS
  local dCapPort
  local yesno
  local serverRoot
  fqHostname=`fqdn_os`
  getConfigurationValue dcap dCapPort dCapPort
  logmessage INFO "Checking on a possibly existing dCache/PNFS configuration ..."
  if [ -f ${NODE_CONFIG_PNFS_ROOT}/fs/admin/etc/config/serverRoot ]; then
    WRITING_PNFS=no
  else
    WRITING_PNFS=yes
  fi
  logmessage DEBUG "WRITING_PNFS=$WRITING_PNFS"
  yesno="${NODE_CONFIG_PNFS_OVERWRITE}"
  if [ \( "${yesno}" = "n" -o "${yesno}" = "no" \) -a "${WRITING_PNFS}" = "no" ] ; then
    logmessage INFO "Found an existing dCache/PNFS configuration!"
    logmessage INFO "Not allowed to overwrite existing PNFS configuration."
  elif [ \( "${yesno}" = "y" -o "${yesno}" = "yes" \) -a "${WRITING_PNFS}" = "no" ] ; then
    logmessage INFO "Found an existing dCache/PNFS configuration!"

    logmessage WARNING "Overwriting existing dCache/PNFS configuration..."

    WRITING_PNFS=yes

    sleep 5
  fi

  #     Writing new pnfs configuration
  #
  if [ "${WRITING_PNFS}" = "yes" ] ; then

    cd ${NODE_CONFIG_PNFS_ROOT}/fs
    serverRoot=`cat ".(id)(usr)"`
    logmessage DEBUG serverRoot=$serverRoot
    #     Writing Wormhole information
    #
    logmessage DEBUG "Changing directory to ${NODE_CONFIG_PNFS_ROOT}/fs/admin/etc/config"
    cd ${NODE_CONFIG_PNFS_ROOT}/fs/admin/etc/config

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
    echo "${fqHostname}:${dCapPort}" > ./dcache.conf
    touch ".(fset)(dcache.conf)(io)(on)"
    echo "${fqHostname}:${dCapPort}" > ./dcache.conf

    #     Configure directory tags
    #
    cd ${NODE_CONFIG_PNFS_ROOT}/fs/usr/data
    echo "StoreName myStore" > ".(tag)(OSMTemplate)"
    echo "STRING" > ".(tag)(sGroup)"
  fi
  dcacheInstallPnfsMount
  logmessage DEBUG "dcacheInstallPnfsConfigCheck.stop"
}


dcacheInstallPnfsMount()
{
  logmessage DEBUG "dcacheInstallPnfsMount.start"
  #  ----  Mount point for doors
  #    This is done, even if PNFS_OVERWRITE=no in order to
  #    cleanly upgrade
  #
  cd ${NODE_CONFIG_PNFS_ROOT}/fs/admin/etc/exports
  if ! grep '^/pnfsdoors' * >/dev/null 2>/dev/null ; then
    logmessage INFO "Configuring pnfs export '/pnfsdoors' (needed from version 1.6.6 on)"
    logmessage INFO "mountable by world."
    echo '/pnfsdoors /0/root/fs/usr/ 30 nooptions' >> '0.0.0.0..0.0.0.0'
  else
    logmessage INFO "There already are pnfs exports '/pnfsdoors' in"
    logmessage INFO "        /pnfs/fs/admin/etc/exports. The GridFTP doors need access to it."
    if grep '^/pnfsdoors' * | grep -v ':/pnfsdoors[^[:graph:]]\+/0/root/fs/usr/[^[:graph:]]\+' >/dev/null 2>/dev/null ; then
      logmessage WARNING "  Make shure they all point to '/0/root/fs/usr/'! The GridFTP doors which"
      logmessage WARNING "        are not on the admin node will mount these from version 1.6.6 on."
    fi
  fi
  logmessage INFO "You may restrict access to this export to the GridFTP doors which"
  logmessage INFO "are not on the admin node. See the documentation."
  logmessage DEBUG "dcacheInstallPnfsMount.stop"
}


dcacheInstallPnfs()
{
  dcacheInstallPnfsMountPoints
}

dcacheGetSshHostKey(){
  if [ $(uname) = "Darwin" ] ; then
    SSH_HOST_KEY="/etc/ssh_host_key"
  else
    SSH_HOST_KEY="/etc/ssh/ssh_host_key"
  fi
}

dcacheInstallSshKeys()
{
  # Install ssh keys for secure communication
  #
  logmessage DEBUG "dcacheInstallSshKeys.start"
  if contains admin $SERVICES ; then
    cd ${DCACHE_HOME}/config
    if [ -f ./server_key ]; then
      logmessage INFO "Skipping ssh key generation"
    else
      logmessage INFO "Generating ssh keys:"
      ssh-keygen -b 768 -t rsa1 -f ./server_key -N "" 2>&1 | logmessage INFO
      dcacheGetSshHostKey
      ln -s $SSH_HOST_KEY ./host_key
    fi
  else
    logmessage INFO "Not an admin door inteface node"
  fi
  logmessage DEBUG "dcacheInstallSshKeys.stop"
}



#    Pool configuration
#
dcacheInstallCheckPool()
{
  logmessage DEBUG "dcacheInstallCheckPool.start"
  local rt
  local size
  local shortHostname
  rt=`echo ${rrt} | awk '{print $1}'`
  size=`echo ${rrt} | awk '{print $2}'`

  if [ ! -e "${rt}/pool" ]; then
    logmessage INFO "Creating Pool" ${pn}
    shortHostname=`shortname_os`
    createPool "${size}g" "${rt}/pool"
    addPool "${pn}" "${rt}/pool" "${shortHostName}Domain" 0 "precious"
  fi

  logmessage DEBUG "dcacheInstallCheckPool.stop"
}

dcacheInstallPool()
{
  logmessage DEBUG "dcacheInstallPool.start"
  local shortHostname
  local x
  local fileToProcess
  local linecount
  shortHostname=`shortname_os`
  if [ ! -r "${DCACHE_HOME}/config/${shortHostname}.poollist" ]; then
    rm -f "${DCACHE_HOME}/config/${shortHostname}.poollist"
    touch "${DCACHE_HOME}/config/${shortHostname}.poollist"
  fi
  if [ -r ${DCACHE_HOME}/etc/pool_path ]; then
    logmessage WARNING "Defining pools in ${DCACHE_HOME}/etc/pool_path is deprecated."
    logmessage WARNING "Please use ${DCACHE_HOME}/bin/dcache pool create and"
    logmessage WARNING "${DCACHE_HOME}/bin/dcache pool add instead."

    # For all the static areas under rt make a pool
    fileToProcess=${DCACHE_HOME}/etc/pool_path
    let x=1
    linecount=`wc -l ${fileToProcess} | cut -d" " -f1`
    let linecount=${linecount}+1
    while [ $x -lt $linecount ]
    do
      rrt=`sed "${x}q;d" ${fileToProcess}`
      pn=${shortHostname}"_"${x}
      dcacheInstallCheckPool
      let x=${x}+1
    done

  fi

  logmessage DEBUG "dcacheInstallPool.stop"
}


dcacheInstallSrm()
{
  local java

  logmessage DEBUG "dcacheInstallSrm.start"
  getConfigurationValue srm java java

  #
  # check java:
  #     jdk >= 1.6 , ( javac needed by tomcat/SRM )
  #

  if [ -z "${java}" ]; then
    logmessage ABORT "java variable in ${DCACHE_HOME}/config/srmSetup not defined"
    exit 6
  fi

  #
  # resove java path eg. /usr/bin/java = /usr/j2se_1.4.2/bin/java
  #
  getCanonicalPath "${java}"; java="${RET}"
  if [ -z "${java}" ]; then
    logmessage ABORT "java variable in ${DCACHE_HOME}/config/srmSetup does not point to existing binary"
    exit 7
  fi

  ${java} -version 2>&1 | grep version | egrep "1\.[6]\." >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    logmessage ABORT "java variable in ${DCACHE_HOME}/config/srmSetup do not point to java version 1.6.x"
    exit 6
  fi
  # standard javac location $JAVA_HOME/bin/java
  # check for javac
  JAVA_HOME=${java%/bin/*}
  if [ ! -x ${JAVA_HOME}/bin/javac ]; then
    # on some system (e.g. Debian), $JAVA_HOME/bin/java points
    # to $JAVA_HOME/jre/bin/java. Try to go up another level.
    JAVA_HOME=${java%/jre/bin/*}
  fi
  # install SRM
  #

  # put correct JAVA_HOME into srm_setup.env
  (
    grep -v JAVA_HOME ${DCACHE_HOME}/etc/srm_setup.env
    echo "JAVA_HOME=${JAVA_HOME}"
  ) > ${DCACHE_HOME}/etc/srm_setup.env.$$
  mv ${DCACHE_HOME}/etc/srm_setup.env.$$ ${DCACHE_HOME}/etc/srm_setup.env
  if contains srm $SERVICES; then
    export loglevel logfile
    logmessage DEBUG "Running ${DCACHE_HOME}/install/deploy_srmv2.sh ${DCACHE_HOME}/etc/srm_setup.env"
    ${DCACHE_HOME}/install/deploy_srmv2.sh ${DCACHE_HOME}/etc/srm_setup.env
  fi
  logmessage DEBUG "dcacheInstallSrm.stop"
}

dcacheInstallCreateWrappers()
{
  logmessage DEBUG "dcacheInstallCreateWrappers.start"
  #
  # init package ( create wrappers in jobs directory )
  #
  logmessage DEBUG "Running ${DCACHE_HOME}/jobs/initPackage.sh ${DCACHE_HOME} "
  ${DCACHE_HOME}/jobs/initPackage.sh ${DCACHE_HOME} 2>&1
  if [ $? != 0 ]; then
    logmessage ABORT "Failed to initalize dCache installation, exiting."
    exit 2
  fi
  logmessage DEBUG "dcacheInstallCreateWrappers.stop"
}

# Do the inistialtion checks


# Now start processing
check_shell
# Now check OS
check_os
if [ "$RET" == "Unknown" ] ; then
  echo "ERROR: OS is not found."
  exit 1
fi

# now process the command line

while [ $# -ne 0 ]
do
  # Default to shifting to next parameter
  shift_size=1
  if [ $1 == "--prefix" -o $1 == "-p" ]
  then
    # set dCache install location
    DCACHE_HOME="$2"
    shift_size=2
  fi
  if [ $1 == "--help" -o $1 == "-h" ]
  then
    # set dCache install location
    usage
    exit 0
  fi
  if [ $1 == "--loglevel" -o $1 == "-l"  ]
  then
    # set dCache install location
    let loglevel=$2
    shift_size=2
  fi
  if [ $1 == "--logfile" ]
  then
    # set dCache install location
    logfile=$2
    shift_size=2
  fi
  shift $shift_size
done

# Set home path
if [ -z "$DCACHE_HOME" ]; then
    getCanonicalPath "$0"
    DCACHE_HOME=${RET%/install/install.sh}
fi

if [ ! -d "$DCACHE_HOME" ]; then
    echo "$DCACHE_HOME is not a directory"
    exit 2
fi

ourHomeDir="$DCACHE_HOME"

# Load libraries
. ${DCACHE_HOME}/share/lib/paths.sh
. ${DCACHE_LIB}/utils.sh
. ${DCACHE_LIB}/config.sh
. ${DCACHE_LIB}/services.sh
. ${DCACHE_LIB}/namespace.sh
. ${DCACHE_LIB}/pool.sh

# Now check for missing files
check_install

# Read node config into variables prefixed by NODE_CONFIG_
readconf ${DCACHE_HOME}/etc/node_config NODE_CONFIG_ ||
readconf ${DCACHE_HOME}/etc/door_config NODE_CONFIG_

DOMAINS="$(printAllDomains)"
SERVICES="$(printServices $DOMAINS)"
getServerId SERVER_ID

if isNameSpaceMountNeeded $DOMAINS; then
  logmessage INFO "This node will need to mount the name server."
fi


dcacheInstallSshKeys
dcacheInstallCreateWrappers
dcacheInstallMountPoints
dcacheInstallSrm
dcacheInstallPool

exit 0
