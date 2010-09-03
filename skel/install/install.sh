#!/bin/bash

# Default Log level
# Standard logging levels are
# ABORT ERROR WARNING INFO DEBUG
loglevel=14

# Default Log File
logfile=""

usage()
{
  echo "dCache install script."
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

loadConfig()
{
    . ${DCACHE_HOME}/share/lib/loadConfig.sh "$@"
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

dcacheCheckSshKeys()
{
    logmessage DEBUG "dcacheInstallSshKeys.start"
    if [ ! -f ${DCACHE_CONFIG}/server_key ]; then
        logmessage INFO "No SSH keys found. To use the admin service on this host generate the SSH keys like this:"
        logmessage INFO "ssh-keygen -b 768 -t rsa1 -f ${DCACHE_CONFIG}/server_key -N \"\""
        logmessage INFO "ssh-keygen -b 1024 -t rsa1 -f ${DCACHE_CONFIG}/host_key -N \"\""
        logmessage INFO "Please note that this message may not apply if the keyBase property has"
        logmessage INFO "been changed in the dCache configuration."
    fi
    logmessage DEBUG "dcacheInstallSshKeys.stop"
}

dcacheInstallInitConfig()
{
    logmessage DEBUG "dcacheInstallInitConfig.start"

    if [ ! -d ${DCACHE_CONFIG} ] ; then
        echo "Cannot find 'config' directory" >&2
        exit 5
    fi
    cd ${DCACHE_CONFIG}

    printf " Checking Users database .... "
    if [ ! -d users ]  ; then
        mkdir users >/dev/null 2>&1
        if [ $? -ne 0 ] ; then
            echo " Failed : can't create ${DCACHE_CONFIG}/users"
        else
            mkdir  users/acls users/meta users/relations >/dev/null 2>&1
            if [ $? -ne 0 ] ; then
                echo " Failed : can't create ${DCACHE_CONFIG}/users/acls|meta|relations"
            else
                echo " Created"
            fi
        fi
    else
        isok=0
        for c in acls relations meta ; do
            if [ ! -d $c ] ; then
                mkdir $c >/dev/null 2>/dev/null
                if [ $? -ne 0 ] ; then
                    echo " Failed : can't create ${DCACHE_CONFIG}/users/$c"
                    isok=1
                    break
                fi
            fi
        done
        if [ $isok -eq 0 ] ; then echo "Ok" ; fi
    fi

    logmessage DEBUG "dcacheInstallInitConfig.stop"
}

# now process the command line
while [ $# -ne 0 ]
do
    case "$1" in
        --prefix|-p)
            # set dCache install location
            DCACHE_HOME="$2"
            shift 2
            ;;

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
        *)
            shift 1
            ;;
    esac
done

# Set home path
if [ -z "$DCACHE_HOME" ]; then
    getCanonicalPath "$0"
    DCACHE_HOME=$(dirname $(dirname "$RET"))
fi

if [ ! -d "$DCACHE_HOME" ]; then
    echo "$DCACHE_HOME is not a directory"
    exit 2
fi

loadConfig
dcacheCheckSshKeys
dcacheInstallInitConfig
