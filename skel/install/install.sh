#!/bin/bash

# Set up the preset values before we get to the command line
# Note these values can be changed with the command line
loglevel=20
ourHomeDir=/opt/d-cache





# Now we start the code proper
yaim_config_file_get_value()
{
# Returns 0 on success
# Returns 2 if key not found
local FILE
local Key
local cursor
local CursorLine
local MatchLine
FILE=$1
Key=$2
if [ ! -f ${FILE} ] ; then
    echo yaim_config_file_get_value called with no file, file=$FILE
    exit 1
fi
cursor=`grep -n "^[\t ]*${Key}[\t ]*=" $FILE | cut -d: -f1 | tail -n 1`
if [ "${cursor}X" == "X" ] ; then
  RET=""
  return 2
fi
CursorLine=`sed "${cursor}q;d" $FILE | cut -s -d= -f2-`
MatchLine="${CursorLine%%"\\"}\\"
RET="${CursorLine%%"\\"}"
while [ "${CursorLine}" == "${MatchLine}" ] 
do
    let cursor+=1
    CursorLine=`sed "${cursor}q;d" $FILE`
    MatchLine="${CursorLine%%"\\"}\\"
    RET="$RET ${CursorLine%%"\\"}"
done
}


logmessage()
{
  echo $*
}

check_shell()
{
  if [ "$BASH_VERSION" = "" ] ; then
    echo "ERROR: bash was not detected script exiting."    
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
  if [ "$osname" = "SunOS" ] ; then
    RET=SunOS
  fi
}

os_absolutePathOf() {
    case `uname` in
        Linux)
            readlink -f $1
            ;;
        *)
            path=`absfpath $1`
            while true
            do
                /bin/test -L "${path}"
                if [ $? -ne 0 ]
                then
                   break;
                fi

                newpath=`ls -ld  ${path} | awk '{ print $11 }'`
                echo ${newpath} | egrep "^/"  > /dev/null 2>&1
                if [ $? -eq 0 ]
                then
                    # absolute path
                    fullpath=${newpath}
                else
                    linkpath=`dirname ${path}`/${newpath}
                    fullpath=`absfpath ${linkpath}`
                fi
                path=${fullpath}
            done
            echo ${fullpath}
            ;;
    esac
}
check_install()
{
  if [ ! -r ${ourHomeDir}/etc/node_config ]; then
    echo "[ERROR] ${ourHomeDir}/etc/node_config missing."
    echo "[HINT]  Copy ${ourHomeDir}/etc/node_config.template to ${ourHomeDir}/etc/node_config and customize it "
    echo "        before running the install script. Exiting."
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
    esac
}

printConfig() {
    key=$1
    cat ${ourHomeDir}/etc/node_config \
        ${ourHomeDir}/etc/door_config 2>/dev/null |
    perl -e "
      while (<STDIN>) { 
         s/\#.*$// ;                        # Remove comments
         s/\s*$// ;                         # Remove trailing space
         if ( s/^\s*${key}\s*=*\s*//i ) {   # Remove key and equals
            print;                          # Print if key found
            last;                           # Only use first appearance
         }
      }
    "
}
fqdn_os() {
    case `uname` in
        Linux)
            echo `hostname --fqdn`
            ;;
        SunOS)
            echo `/usr/lib/mail/sh/check-hostname |cut -d" " -f7`
            ;;
    esac
}
dcacheInstallGetPnfsRoot()
{
  # To make shure to use the right dCache dir
  RET=`printConfig PNFS_ROOT` 
}

dcacheInstallGetServerId()
{
  local SERVER_ID
  SERVER_ID=`printConfig SERVER_ID`
  if [ -z "${SERVER_ID}" ] ; then
    SERVER_ID=`domainname_os`
    if [ $? -ne 0 -o -z "${SERVER_ID}" ] ; then
        SERVER_ID="`cat /etc/resolv.conf | sed -e 's/#.*$//' | grep 'search' | awk '{ print($2) }'`"
        if [ -z "${SERVER_ID}" ]; then
            SERVER_ID="`cat /etc/resolv.conf | sed -e 's/#.*$//' | grep 'domain' | awk '{ print($2) }'`"
        fi
    fi
    echo ""
    echo "[INFO]  No 'SERVER_ID' set in 'node_config'. Using SERVER_ID=${SERVER_ID}."
  fi
  RET=${SERVER_ID}
}

dcacheInstallGetPnfsMountPoint()
{
  local serverId
  local PnfsRoot
  dcacheInstallGetPnfsRoot
  PnfsRoot=$RET
  dcacheInstallGetServerId
  serverId=$RET
  RET=${PnfsRoot}/${serverId}
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



dcacheInstallGetHome()
{
  local DCACHE_HOME_Tmp
  RET=""
  # To make shure to use the right dCache dir
  DCACHE_HOME_Tmp=`printConfig DCACHE_HOME`
  if [ "x${DCACHE_HOME_Tmp}" == "x" ] ; then
    DCACHE_HOME_Tmp=`printConfig DCACHE_BASE_DIR`
    echo "WARNING: the variable DCACHE_HOME is not set."
    if [ "x${DCACHE_HOME_Tmp}" != "x" ] ; then
      echo "WARNING: Using deprecated value of DCACHE_BASE_DIR as DCACHE_HOME"
      RET=${DCACHE_HOME_Tmp}
    else
      echo "ERROR: Failed getting the value of DCACHE_HOME"
    fi
  else
    RET=${DCACHE_HOME_Tmp}
  fi
}

dcacheInstallGetUseGridFtp()
{
  # returns 1 if gridFTP is used 0 otherwise
  local UseGridFtp
  UseGridFtp=`printConfig GRIDFTP |  tr -s '[:upper:]' '[:lower:]'`
  if [ "${UseGridFtp}" == "yes" ] ; then
    return 1
  fi
  if [ "${UseGridFtp}" == "y" ] ; then
    return 1
  fi
  return 0
}

dcacheInstallGetIsAdminDoor()
{
  # returns 1 if is an adminDoor is used 0 otherwise
  local NodeType
  NodeType=`printConfig adminDoor |  tr -s '[:upper:]' '[:lower:]'`
  if [ "${NodeType}" == "yes" ] ; then
    return 1
  fi
  if [ "${NodeType}" == "y" ] ; then
    return 1
  fi
  return 0
}


dcacheInstallGetIsHttpDomain()
{
  # returns 1 if is an httpDomain is used 0 otherwise
  local NodeType
  NodeType=`printConfig httpDomain |  tr -s '[:upper:]' '[:lower:]'`
  if [ "${NodeType}" == "yes" ] ; then
    return 1
  fi
  if [ "${NodeType}" == "y" ] ; then
    return 1
  fi
  return 0
}


dcacheInstallGetIsLmDomain()
{
  # returns 1 if is an lmDomain is used 0 otherwise
  local NodeType
  NodeType=`printConfig lmDomain |  tr -s '[:upper:]' '[:lower:]'`
  if [ "${NodeType}" == "yes" ] ; then
    return 1
  fi
  if [ "${NodeType}" == "y" ] ; then
    return 1
  fi
  return 0
}

dcacheInstallGetIsPoolManager()
{
  # returns 1 if is an poolManager is used 0 otherwise
  local NodeType
  NodeType=`printConfig poolManager |  tr -s '[:upper:]' '[:lower:]'`
  if [ "${NodeType}" == "yes" ] ; then
    return 1
  fi
  if [ "${NodeType}" == "y" ] ; then
    return 1
  fi
  return 0
}

dcacheInstallGetIsUtilityDomain()
{
  # returns 1 if is an utilityDomain is used 0 otherwise
  local NodeType
  NodeType=`printConfig utilityDomain |  tr -s '[:upper:]' '[:lower:]'`
  if [ "${NodeType}" == "yes" ] ; then
    return 1
  fi
  if [ "${NodeType}" == "y" ] ; then
    return 1
  fi
  return 0
}

dcacheInstallGetIsPnfsManager()
{
  # returns 1 if is an pnfsManager is used 0 otherwise
  local NodeType
  NodeType=`printConfig pnfsManager |  tr -s '[:upper:]' '[:lower:]'`
  if [ "${NodeType}" == "yes" ] ; then
    return 1
  fi
  if [ "${NodeType}" == "y" ] ; then
    return 1
  fi
  return 0
}


dcacheInstallGetIsAdmin()
{
  # returns 1 if is an admin node is used 0 otherwise
  local NodeType
  NodeType=`printConfig NODE_TYPE |  tr -s '[:upper:]' '[:lower:]'`
  if [ "${NodeType}" == "admin" ] ; then
    return 1
  fi
  if [ "${NodeType}" == "custom" ] ; then
    local adminDoor
    local httpDomain
    local lmDomain
    local poolManager
    local utilityDomain
    # Get values
    dcacheInstallGetIsAdminDoor
    adminDoor=$?
    dcacheInstallGetIsHttpDomain
    httpDomain=$?
    dcacheInstallGetIsLmDomain
    lmDomain=$?
    dcacheInstallGetIsPoolManager
    poolManager=$?
    dcacheInstallGetIsUtilityDomain
    utilityDomain=$?
    if [ "${adminDoor}${httpDomain}${lmDomain}${poolManager}${utilityDomain}" == "11111" ] ; then
      return 1	
    fi
  fi
  return 0
}

dcacheInstallGetdCapPort()
{
  local DCACHE_HOME
  dcacheInstallGetHome
  DCACHE_HOME=$RET
  yaim_config_file_get_value ${DCACHE_HOME}/config/dCacheSetup dCapPort
}

dcacheInstallPnfsMountPointClient()
{ 
  local PNFS_ROOT
  local SERVER_ID
  local DCACHE_HOME
  local pnfsMountPoint
  local ADMIN_NODE
  local exportPoint
  dcacheInstallGetServerId
  SERVER_ID=$RET
  dcacheInstallGetPnfsRoot
  PNFS_ROOT=$RET
  dcacheInstallGetHome
  DCACHE_HOME=$RET 
  dcacheInstallGetPnfsMountPoint
  pnfsMountPoint=${RET}
  ADMIN_NODE="`printConfig ADMIN_NODE`"
  echo ""
  echo -n "[INFO]  Checking if ${pnfsMountPoint} mounted to the right export. ..."
  dcacheInstallGetExportPoint
  exportPoint=$RET
  if [ "${exportPoint}" = '/pnfsdoors' ] ; then
    echo "OK"
  else
    echo ""
    if [ "${exportPoint}" != "" ] ; then
      echo "[WARN]  ${pnfsMountPoint} mounted, however not to ${ADMIN_NODE}:/pnfsdoors."
      echo "        Unmounting it now:"
      umount ${pnfsMountPoint}
      echo ""
    fi

    if [ -L "${pnfsMountPoint}" ] ; then
      echo "[INFO]  Trying to remove symbolic link ${pnfsMountPoint} :"
      rm -f ${pnfsMountPoint}
      if [ $? -eq 0 ] ; then
        echo "[INFO]  'rm -f ${pnfsMountPoint}' went fine."
      else
        echo "[ERROR] 'rm -f ${pnfsMountPoint}' failed. Please move it out of the way manually"
        echo "        and run me again. Exiting."
        exit 1
      fi
    fi

    if [ ! -d "${pnfsMountPoint}" ]; then
      if [ -e "${pnfsMountPoint}" ] ; then
        echo "[ERROR] The file ${pnfsMountPoint} is in the way. Please move it out of the way"
        echo "        and call me again. Exiting."
        exit 1
      else
        echo "[INFO]  Creating pnfs mount point (${pnfsMountPoint})"
        mkdir -p ${pnfsMountPoint}
      fi
    fi
    echo "[INFO]  Will be mounted to ${ADMIN_NODE}:/pnfsdoors by dcache-core start-up script."
  fi

  echo ""
  if [ ! -L "${PNFS_ROOT}/ftpBase" -a ! -e "${PNFS_ROOT}/ftpBase" ] ; then
    echo "[INFO]  Creating link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} which is used by the GridFTP door."
    ln -s ${pnfsMountPoint} ${PNFS_ROOT}/ftpBase
  else
    local ftpBaseLinkedTo
    ftpBaseLinkedTo=`find ${PNFS_ROOT}/ftpBase -type l -printf '%l'`
    echo ftpBaseLinkedTo=$ftpBaseLinkedTo
    echo pnfsMountPoint=${pnfsMountPoint}
    if [ "${ftpBaseLinkedTo}" == "${pnfsMountPoint}" ] ; then
      echo "[INFO]  Link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} already there."
    else
      echo "[ERROR] Link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} cannot be created. Needed by the GridFTP door."
      echo "        Please move ${PNFS_ROOT}/ftpBase and run this script again. Exiting."
      exit 1
    fi
  fi

  if ! grep "^ftpBase=${PNFS_ROOT}/ftpBase" ${DCACHE_HOME}/config/dCacheSetup 2>/dev/null >/dev/null ; then
    echo ""
    echo "[WARN]  The file ${DCACHE_HOME}/config/dCacheSetup does not contain:"
    echo "           ftpBase=${PNFS_ROOT}/ftpBase"
    echo "        Make shure it is set correctly before you start dCache."
  fi

  if mount | grep pnfs 2>/dev/null >/dev/null ; then
    echo ""
    echo "[WARN]  A pnfs export is already mounted. The GridFTP door will only use the"
    echo "        mount at ${pnfsMountPoint} which will be mounted by the start-up script."
    echo "        You might want to remove any mounts not needed anymore."
  fi
  echo ""
  
}


dcacheInstallPnfsMountPointServer()
{
  local PNFS_ROOT
  local SERVER_ID
  local DCACHE_HOME
  local PNFS_INSTALL_DIR
  local pnfsMountPoint
  local serverIdLinkedTo
  local ftpBaseLinkedTo
  dcacheInstallGetServerId
  SERVER_ID=$RET
  dcacheInstallGetPnfsRoot
  PNFS_ROOT=$RET
  dcacheInstallGetHome
  DCACHE_HOME=$RET 
  PNFS_INSTALL_DIR=`printConfig PNFS_INSTALL_DIR`
  
  echo ""
  #    Checking and creating mountpoint and link
  #
  pnfsMountPoint=${PNFS_ROOT}/fs
  if [ ! -d "${pnfsMountPoint}" ]; then
    if [ -e "${pnfsMountPoint}" ] ; then
      echo "[ERROR] The file ${pnfsMountPoint} is in the way. Please move it out of the way"
      echo "        and call me again. Exiting."
      exit 1
    else
      echo "[INFO]  Creating pnfs mount point (${pnfsMountPoint})"
	mkdir -p ${pnfsMountPoint}
    fi
  fi
  echo "[INFO]  Will be mounted to ${pnfsServer}:/fs by dcache-core start-up script."

  cd ${PNFS_ROOT}
  if [ ! -L "${SERVER_ID}" ]; then
    if [ -e "${SERVER_ID}" ] ; then
      echo "[ERROR] The file/directory ${PNFS_ROOT}/${SERVER_ID} is in the way. Please move it out"
      echo "        of the way and call me again. Exiting."
    else
      echo "[INFO]  Creating link ${PNFS_ROOT}/${SERVER_ID} --> ${pnfsMountPoint}/usr/"
      ln -s fs/usr ${SERVER_ID}
    fi
  fi

  cd ${PNFS_ROOT}
  if [ ! -L "${SERVER_ID}" -a ! -e "${SERVER_ID}" ] ; then
    echo "[INFO]  Creating link ${PNFS_ROOT}/${SERVER_ID} --> ${PNFS_ROOT}/fs/usr."
    ln -s fs/usr ${SERVER_ID}
  else
    serverIdLinkedTo=`find ${PNFS_ROOT}/${SERVER_ID} -type l -printf '%l'`
    if [ "${serverIdLinkedTo}" = "fs/usr" -o "${serverIdLinkedTo}" = "${PNFS_ROOT}/fs/usr" ] ; then
      echo "[INFO]  Link ${PNFS_ROOT}/${SERVER_ID} --> ${PNFS_ROOT}/fs/usr already there."
    else
      echo "[ERROR] Link ${PNFS_ROOT}/${SERVER_ID} --> ${PNFS_ROOT}/fs/usr cannot be created."
      echo "        Please move ${PNFS_ROOT}/${SERVER_ID} and run me again. Exiting."
      exit 1
    fi
  fi

  cd ${PNFS_ROOT}
  if [ ! -L ${PNFS_ROOT}/ftpBase -a ! -e ${PNFS_ROOT}/ftpBase ] ; then
    echo "[INFO]  Creating link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} which is used by the GridFTP door."
    ln -s ${pnfsMountPoint} ${PNFS_ROOT}/ftpBase
  else
    ftpBaseLinkedTo=`find ${PNFS_ROOT}/ftpBase -type l -printf '%l'`
    if [ "${ftpBaseLinkedTo}" = "${pnfsMountPoint}" ] ; then
      echo "[INFO]  Link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} already there."
    else
      echo "[ERROR] Link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} cannot be created. and is needed by the GridFTP door."
      echo "        Please move ${PNFS_ROOT}/ftpBase and run me again. Exiting."
      exit 1
    fi
  fi

  # Checking for existance of rpcinfo
  #

  which rpcinfo 2>&1 > /dev/null
  RC=$?
  if [ ${RC} -ne 0 ] ; then 
    echo "ERROR: rpcinfo is not on the path"
    exit 1
  fi

  #    Checking and maybe starting pnfs server
  #

  RETVAL=0
  rpcinfo -u localhost 100003 >/dev/null 2>&1
  RETVAL=$?
  if [ ${RETVAL} -eq 1 ]; then
    echo ""
    if [ ! -x ${PNFS_INSTALL_DIR}/tools/pnfs.server ]; then
      echo ""
      echo "[ERROR] PNFS not installed but needed for dCache admin node installation. Exiting."
      exit 1
    fi
    echo ""
    echo "[INFO]  PNFS is not running. It is needed to prepare dCache. ... "
    yesno=`printConfig PNFS_START`
    if [ \( "${yesno}" = "n" \) -o \( "${yesno}" = "no" \) ] ; then
      echo "[ERROR] Not allowed to start it. Set PNFS_START in etc/node_config to 'yes' or start by hand. Exiting."
      exit 1
    elif [ \( "${yesno}" = "y" \) -o \( "${yesno}" = "yes" \) ] ; then
      echo "[INFO]  Trying to start it now:"
      ${PNFS_INSTALL_DIR}/tools/pnfs.server start
    fi
  fi

  #    Checking pnfs mount and possibly mounting
  #
  echo ""
  cp=`df ${pnfsMountPoint} 2>/dev/null | grep "${pnfsMountPoint}" | awk '{print $2}'`
  if [ -z ${cp} ]; then
    echo "[INFO]  ${pnfsMountPoint} mount point exists, but is not mounted - going to mount it now ..."
    mount -o intr,rw,noac,hard,nfsvers=2 ${pnfsServer}:/fs ${pnfsMountPoint}
  fi
  cp=`df ${pnfsMountPoint} 2>/dev/null | grep "${pnfsMountPoint}" | awk '{print $2}'`
  if [ -z $cp ]; then
    echo "[ERROR] Was not able to mount ${pnfsServer}:/fs to ${pnfsMountPoint}. Exiting."
    exit 1
  fi
  dcacheInstallPnfsConfigCheck
}

dcacheInstallPnfsMountPoints()
{
  # Creating /pnfs/fs and Symbolic Link /pnfs/fs/usr to /pnfs/<domain> 
  # (e.g. /pnfs/fnal.gov) for GridFTP
  local isAdmin
  local isGridFtp
  local isPnfsManager

  dcacheInstallGetIsAdmin
  isAdmin=$?
  dcacheInstallGetUseGridFtp
  isGridFtp=$?
  dcacheInstallGetIsPnfsManager
  isPnfsManager=$?

  #echo admin=${isAdmin}${isGridFtp}${isPnfsManager}
  if [ "${isAdmin}${isGridFtp}" == "01" ] ; then
    dcacheInstallPnfsMountPointClient
  fi

  if [ "${isAdmin}${isPnfsManager}" == "10" -o "${isPnfsManager}" == "1" ] ; then
    dcacheInstallPnfsMountPointServer
  fi  
}

dcacheInstallPnfsConfigCheck()
{
  # Checking if pnfs config exists
  #
  local PNFS_ROOT
  local SERVER_ID
  local fqHostname
  local WRITING_PNFS
  local dCapPort
  local yesno
  local serverRoot
  fqHostname=`fqdn_os`
  dcacheInstallGetServerId
  SERVER_ID=$RET
  dcacheInstallGetPnfsRoot
  PNFS_ROOT=$RET
  dcacheInstallGetdCapPort
  dCapPort=$RET
  echo ""
  echo "[INFO]  Checking on a possibly existing dCache/PNFS configuration ..."
  if [ -f ${PNFS_ROOT}/fs/admin/etc/config/serverRoot ]; then
    WRITING_PNFS=no
  else
    WRITING_PNFS=yes
  fi
  echo WRITING_PNFS=$WRITING_PNFS
  yesno=`printConfig PNFS_OVERWRITE`
  if [ \( "${yesno}" = "n" -o "${yesno}" = "no" \) -a "${WRITING_PNFS}" = "no" ] ; then
    echo "[INFO]  Found an existing dCache/PNFS configuration!"
    echo "[INFO]  Not allowed to overwrite existing configuration."
    echo ""
  elif [ \( "${yesno}" = "y" -o "${yesno}" = "yes" \) -a "${WRITING_PNFS}" = "no" ] ; then
    echo "[INFO]  Found an existing dCache/PNFS configuration!"
    echo ""
    echo "[WARN]  Overwriting existing dCache/PNFS configuration..."
    echo ""
    WRITING_PNFS=yes
    echo ""
    sleep 5
  fi

  #     Writing new pnfs configuration
  #
  if [ "${WRITING_PNFS}" = "yes" ] ; then

    cd ${PNFS_ROOT}/fs
    serverRoot=`cat ".(id)(usr)"`

    #     Writing Wormhole information
    #
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
    echo "${fqHostname}:${dCapPort}" > ./dcache.conf
    touch ".(fset)(dcache.conf)(io)(on)"
    echo "${fqHostname}:${dCapPort}" > ./dcache.conf

    #     Configure directory tags
    #
    cd ${PNFS_ROOT}/fs/usr/data
    echo "StoreName myStore" > ".(tag)(OSMTemplate)"
    echo "STRING" > ".(tag)(sGroup)"
  fi
  dcacheInstallPnfsMount
}


dcacheInstallPnfsMount()
{
  #  ----  Mount point for doors
  #    This is done, even if PNFS_OVERWRITE=no in order to
  #    cleanly upgrade
  #
  local PNFS_ROOT
  dcacheInstallGetPnfsRoot
  PNFS_ROOT=$RET
  echo ""
  cd ${PNFS_ROOT}/fs/admin/etc/exports
  if ! grep '^/pnfsdoors' * >/dev/null 2>/dev/null ; then
    echo "[INFO]  Configuring pnfs export '/pnfsdoors' (needed from version 1.6.6 on)"
    echo "        mountable by world."
    echo '/pnfsdoors /0/root/fs/usr/ 30 nooptions' >> '0.0.0.0..0.0.0.0'
  else
    echo "[INFO]  There already are pnfs exports '/pnfsdoors' in"
    echo "        /pnfs/fs/admin/etc/exports. The GridFTP doors need access to it."
    if grep '^/pnfsdoors' * | grep -v ':/pnfsdoors[^[:graph:]]\+/0/root/fs/usr/[^[:graph:]]\+' >/dev/null 2>/dev/null ; then
      echo "[WARN]  Make shure they all point to '/0/root/fs/usr/'! The GridFTP doors which"
      echo "        are not on the admin node will mount these from version 1.6.6 on."
    fi
  fi
  echo "[INFO]  You may restrict access to this export to the GridFTP doors which"
  echo "        are not on the admin node. See the documentation." 
}


dcacheInstallPnfs()
{
  dcacheInstallPnfsMountPoints
}


dcacheInstallSshKeys()
{
  local DCACHE_HOME
  local nodeType
  dcacheInstallGetHome
  DCACHE_HOME=$RET
  dcacheInstallGetIsAdmin
  nodeType=$?
  if [ "${nodeType}" = "1" ] ; then
    #     Install ssh keys for secure communication
    #
    echo ""
    echo "[INFO]  Generating ssh keys:"
    cd ${DCACHE_HOME}/config
    if [ -f ./server_key ]; then
      rm ./server_key; rm ./host_key
    fi
    ssh-keygen -b 768 -t rsa1 -f ./server_key -N ""
    ln -s /etc/ssh/ssh_host_key ./host_key
    echo ""
  fi
}

#    Pool configuration
#
dcacheInstallCheckPool() 
{
  local WRITING_POOL
  local rt
  local size
  local yesno
  local NUMBER_OF_MOVERS
  local DCACHE_HOME
  local shortHostname
  local pnl
  local x
  dcacheInstallGetHome
  DCACHE_HOME=$RET
  rt=`echo ${rrt} | awk '{print $1}'`
  size=`echo ${rrt} | awk '{print $2}'`
    
  if [ -d "${rt}/pool" ]; then
    WRITING_POOL=no
  else
    WRITING_POOL=yes
  fi
    
  if [ "${WRITING_POOL}" = "no" ] ; then
    yesno=`echo ${rrt} | awk '{print $3}'`
    if [ \( "${yesno}" = "n" \) -o \( "${yesno}" = "no" \) ] ; then
      echo "[INFO]  Not overwriting pool at ${rt}."	
    elif [ \( "${yesno}" = "y" \) -o \( "${yesno}" = "yes" \) ] ; then
      echo "[WARN]  Will overwrite pool at ${rt}."
      WRITING_POOL=yes
    else
      echo "[WARN]  Valid options for pool overwrite are y/yes or n/no. Assuming 'no'. "
    fi
  fi
    
  if [ "${WRITING_POOL}" = "yes" ] ; then	
    echo "[INFO]  Creating Pool" ${pn}
    rm -rf ${rt}/pool
    mkdir -p ${rt}/pool
    mkdir -p ${rt}/pool/data
    mkdir -p ${rt}/pool/control
    cp ${DCACHE_HOME}/config/setup.temp  ${rt}/pool/setup.orig
    cd ${rt}/pool
    NUMBER_OF_MOVERS=`printConfig NUMBER_OF_MOVERS`
    sed -e "s:set max diskspace 100:set max diskspace ${size}:g" setup.orig > setup.temp
    sed -e "s:mover set max active 10:mover set max active ${NUMBER_OF_MOVERS}:g" setup.temp > setup        
  fi
    
  cd ${rt}/pool
  ds=`eval df -k . | grep -v Filesystem | awk '{ print int($2/1048576) }'`
  let val=${ds}-${size}
  if [ ${val} -lt 0 ]; then
    printf " Pool size exceeds partition size "
  else
    shortHostname=`shortname_os`
    pnl=`grep "${pn}" ${DCACHE_HOME}/config/${shortHostname}.poollist | awk '{print $1}'`
    if [ -z "${pnl}" ]; then	   
      echo "${pn}  ${rt}/pool  sticky=allowed recover-space recover-control recover-anyway lfs=precious tag.hostname=${shortHostname}" \
        >> ${DCACHE_HOME}/config/${shortHostname}.poollist
    fi
  fi
}

dcacheInstallPool()
{  
  local DCACHE_HOME
  local shortHostname
  local x
  dcacheInstallGetHome
  DCACHE_HOME=$RET
  shortHostname=`shortname_os`
  if [ ! -r "${DCACHE_HOME}/config/${shortHostname}.poollist" ]; then
    rm -f "${DCACHE_HOME}/config/${shortHostname}.poollist"
    touch "${DCACHE_HOME}/config/${shortHostname}.poollist"
  fi
  if [ ! -r ${DCACHE_HOME}/etc/pool_path ]; then
    echo "[WARN]  ${DCACHE_HOME}/etc/pool_path does not exist. No pools will be configured."
  else
    # For all the static areas under rt make a pool
    let x=0
    while read rrt; do
      let x=${x}+1 
      pn=${shortHostname}"_"${x}
      dcacheInstallCheckPool
    done  < ${DCACHE_HOME}/etc/pool_path
  fi
}


dcacheInstallSrm()
{
  local DCACHE_HOME
  local java
  dcacheInstallGetHome
  DCACHE_HOME=$RET
  yaim_config_file_get_value ${DCACHE_HOME}/config/dCacheSetup java
  java=$RET


  #
  # check java:
  #     jdk >= 1.5 , ( javac needed by tomcat/SRM )
  #

  if [ -z "${java}" ]; then
    echo "[ERROR] java variable in ${DCACHE_HOME}/config/dCacheSetup not defined"
    exit 6
  fi

  #
  # resove java path eg. /usr/bin/java = /usr/j2se_1.4.2/bin/java
  #
  java=`os_absolutePathOf ${java}`
  if [ -z "${java}" ]; then
    echo "[ERROR] java variable in ${DCACHE_HOME}/config/dCacheSetup do not point to existing binary"
    exit 7
  fi

  ${java} -version 2>&1 | grep version | egrep "1\.[56]\." >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo "[ERROR] java variable in ${DCACHE_HOME}/config/dCacheSetup do not point to java version 1.5.x or 1.6.x"
    exit 6
  fi
  # standard javac location $JAVA_HOME/bin/java
  # check for javac
  JAVA_HOME=${java%/bin/*}
  if [ ! -x ${JAVA_HOME}/bin/javac ]; then
    # on some system (e.g. Debian), $JAVA_HOME/bin/java points
    # to $JAVA_HOME/jre/bin/java. Try to go up another level.
    JAVA_HOME=${java%/jre/bin/*}
    if [ ! -x ${JAVA_HOME}/bin/javac ]; then
	    echo "[ERROR] java installation looks like JRE, while JDK is needed."	
	    exit 7
    fi
  fi
  # install SRM 
  #

  # put correct JAVA_HOME into srm_setup.env
  ( 
    grep -v JAVA_HOME ${DCACHE_HOME}/etc/srm_setup.env
    echo "JAVA_HOME=${JAVA_HOME}"
  ) > ${DCACHE_HOME}/etc/srm_setup.env.$$
  mv ${DCACHE_HOME}/etc/srm_setup.env.$$ ${DCACHE_HOME}/etc/srm_setup.env

  if [ "`printConfig SRM`" = yes ] ; then
    ${DCACHE_HOME}/install/deploy_srmv2.sh ${DCACHE_HOME}/etc/srm_setup.env
  fi

}

dcacheInstallCreateWrappers()
{
  local DCACHE_HOME
  dcacheInstallGetHome
  DCACHE_HOME=$RET
  #
  # init package ( create wrappers in jobs directory )
  #
  ${DCACHE_HOME}/jobs/initPackage.sh ${DCACHE_HOME}
  if [ $? != 0 ]; then
    echo "Failed to initalize dCache installation, exiting."
    exit 2
  fi
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
  if [ $1 == "--prefix" ] 
  then
    # set dCache install location
    ourHomeDir=$2
    shift_size=2
  fi
  if [ $1 == "--loglevel" ] 
  then
    # set dCache install location
    let loglevel=$2
    shift_size=2
  fi
  shift $shift_size
done


# Now check for missing files
check_install

dcacheInstallGetHome
fred=$RET
if [ "$RET" != "${ourHomeDir}" ] ; then
  echo "ERROR: Dcache HOME is set incorrectly." 
  exit 1
fi

if [ "x${NAMESPACE}" != "xchimera" ]
then
  dcacheInstallPnfs
fi
dcacheInstallSshKeys
dcacheInstallCreateWrappers
dcacheInstallSrm
dcacheInstallPool
exit 0
