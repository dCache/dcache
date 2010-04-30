#!/bin/bash

set -e

disclaimer()
{
  echo "${1}DISCLAIMER: Please inspect the generated configuration. The import is"
  echo "${1}            known to be incomplete; in particular if batch files have"
  echo "${1}            been modified or the symbolic links to config/dCacheSetup"
  echo "${1}            have been broken to make domain specific setups. This"
  echo "${1}            includes the case where services have been moved between"
  echo "${1}            domains or service specific configuration parameters are"
  echo "${1}            used in a 1.9.6 style installation."
}

usage()
{
  echo "Imports pre 1.9.7 configuration files."
  echo
  echo "-h --help             Show this help"
  echo "-d PATH               Path to dCache installation (default is /opt/dcache)"
  echo "-f --force            Overwrite existing configuration files, if any"
  echo
  disclaimer
}

addServiceIfNotInOwnDomain() # $1 domain $2 service
{
  if ! contains $2 $(printServices $(printAllDomains)); then
    echo "[$1/$2]"
  fi
}

fileNotPrecious() # $1 filename
{
  if [ ! -e "$1" ]; then
    return 0
  fi

  sed 's/^[ \t]*#.*$//;s/^[ \t]*$//' "$1" | while read line; do
    if [ -n "$line" ]; then
      return 1
    fi
  done
}

# Set home path
if [ -z "$DCACHE_HOME" ]; then
    DCACHE_HOME="/opt/d-cache"
fi

# Process the command line
while [ $# -ne 0 ]
do
    case "$1" in
        -d)
            DCACHE_HOME="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 1
            ;;
        -f|--force)
            force=1
            shift 1
            ;;
        *)
            shift 1
            ;;
    esac
done

if [ ! -d "$DCACHE_HOME" ]; then
    echo "No such directory: $DCACHE_HOME"
    exit 2
fi
ourHomeDir="$DCACHE_HOME"

if [ ! -f "${DCACHE_HOME}/etc/node_config" -a ! -f "${DCACHE_HOME}/etc/door_config" ]; then
    echo "Cannot proceed because ${DCACHE_HOME}/etc/node_config does not exist."
    exit 1
fi

if [ ! -f "${DCACHE_HOME}/config/dCacheSetup" ]; then
    echo "Cannot proceed because ${DCACHE_HOME}/config/dCacheSetup does not exist."
    exit 1
fi


if [ -z "$force" ]; then
    if ! fileNotPrecious "${DCACHE_HOME}/etc/dcache.conf"; then
        echo "Cannot proceed because ${DCACHE_HOME}/etc/dcache.conf already exists."
        exit 1
    fi

    if [ -e "${DCACHE_HOME}/etc/layouts/imported.conf" ]; then
        echo "Cannot proceed because ${DCACHE_HOME}/etc/layouts/imported.conf already exists."
        exit 1
    fi
fi

# Load old configuration
. ${DCACHE_HOME}/share/lib/paths.sh
. ${DCACHE_LIB}/utils.sh
. ${DCACHE_LIB}/config.sh
. ${DCACHE_LIB}/services.sh
. ${DCACHE_LIB}/pool.sh

readconf ${DCACHE_HOME}/etc/node_config NODE_CONFIG_ ||
readconf ${DCACHE_HOME}/etc/door_config NODE_CONFIG_ ||
fail 1 "Failed to read ${DCACHE_HOME}/etc/node_config"

loadConfigurationFile dCache dCache ||
fail 1 "Failed to read dCacheSetup file"
loadConfigurationFile pool pool || loadConfigurationFile dCache pool

determineHostName

# Create configuration file
echo "Converting ${DCACHE_HOME}/etc/dCacheSetup"
echo "to ${DCACHE_HOME}/etc/dcache.conf."

(
    echo "# Auto generated configuration file."
    echo "#"
    echo "# Source: ${DCACHE_HOME}/etc/dCacheSetup"
    echo "# Date: $(date)"
    echo "#"
    disclaimer "# "
    echo

    # Name space declaration
    case "$NODE_CONFIG_NAMESPACE" in
        chimera)
            echo "dcache.namespace=chimera"
            ;;

        *)
            echo "dcache.namespace=pnfs"
            ;;
    esac

    echo "dcache.layout=imported"
    echo
    echo "# The following is a verbatim copy of the old configuration file."
    echo "# Some configuration parameters may no longer apply."
    echo

    cat "${DCACHE_HOME}/config/dCacheSetup"
) > "${DCACHE_HOME}/etc/dcache.conf"

# Create layout file
echo
echo "Converting ${DCACHE_HOME}/etc/node_config"
echo "to ${DCACHE_HOME}/etc/layouts/imported.conf."
(
    echo "# Auto generated layout file."
    echo "#"
    echo "# Source: ${DCACHE_HOME}/etc/node_config"
    echo "# Date: $(date)"
    echo "#"
    disclaimer "# "
    echo

    # Define domains
    for domain in $(printAllDomains); do
        echo

        getService "$domain" SERVICE
        case "$SERVICE" in
            dcap)
                echo "[$domain]"
                echo "[$domain/dcap]"
                ;;
            gPlazma)
                echo "[$domain]"
                echo "[$domain/gplazma]"
                ;;
            xrootd)
                echo "[$domain]"
                echo "[$domain/xrootd]"
                ;;
            gridftp)
                echo "[$domain]"
                echo "[$domain/gridftp]"
                ;;
            kerberosftp)
                echo "[$domain]"
                echo "[$domain/kerberosftp]"
                ;;
            weakftp)
                echo "[$domain]"
                echo "[$domain/ftp]"
                ;;
            webdav)
                echo "[$domain]"
                echo "[$domain/webdav]"
                ;;
            gsidcap)
                echo "[$domain]"
                echo "[$domain/gsidcap]"
                ;;
            kerberosdcap)
                echo "[$domain]"
                echo "[$domain/kerberosdcap]"
                ;;
            srm)
                echo "[$domain]"
                echo "[$domain/srm]"
                addServiceIfNotInOwnDomain $domain spacemanager
                addServiceIfNotInOwnDomain $domain transfermanagers
                ;;
            pool)
                echo "[$domain]"
                printPoolsInDomain "$domain" | while read pool path params; do
                    echo "[$domain/pool]"
                    echo "name=$pool"
                    echo "path=$path"
                    echo 'waitForFiles=${path}/setup'

                    tags=
                    for param in $params; do
                        case "$param" in
                            lfs=*)
                                echo $param
                                ;;
                            recover*|sticky=*)
                                # No longer used
                                ;;
                            tag.*=*)
                                tags="${tags}${tags:+ }${param#tag.}"
                                ;;
                            *=*)
                                echo $param
                                echo "WARNING: The pool option $param for pool $pool is not" 1>&2
                                echo "         recognized. Please verify the pool configuration manually" 1>&2
                                echo "         after conversion." 1>&2
                                ;;
                            *)
                                echo "WARNING: The pool option $param for pool $pool is not" 1>&2
                                echo "         recognized. Please verify the pool configuration manually" 1>&2
                                echo "         after conversion." 1>&2
                                ;;
                        esac
                    done

                    if [ -n "$tags" ]; then
                        echo "tags=${tags}"
                    fi
                done
                ;;
            admin)
                echo "[$domain]"
                echo "[$domain/admin]"
                ;;
            dir)
                echo "[$domain]"
                echo "[$domain/dir]"
                ;;
            lm)
                if ! contains dCacheDomain $(printAllDomains); then
                    echo "WARNING: lmDomain no longer exists. The same functionality" 1>&2
                    echo "         is now embedded in dCacheDomain. Please update your" 1>&2
                    echo "         serviceLocatorHost setting." 1>&2
                fi
                ;;
            dCache)
                echo "[$domain]"
                echo "[$domain/poolmanager]"
                echo "[$domain/dummy-prestager]"
                echo "[$domain/broadcast]"
                echo "[$domain/loginbroker]"
                echo "[$domain/topo]"
                ;;
            dir)
                echo "[$domain]"
                echo "[$domain/dir]"
                ;;
            httpd)
                echo "[$domain]"
                echo "[$domain/httpd]"
                echo "[$domain/billing]"
                echo "[$domain/srm-loginbroker]"
                ;;
            utility)
                echo "[$domain]"
                echo "[$domain/gsi-pam]"
                echo "[$domain/pinmanager]"
                ;;
            statistics)
                echo "[$domain]"
                echo "[$domain/statistics]"
                ;;
            chimera|pnfs)
                echo "[namespaceDomain]"
                echo "[namespaceDomain/pnfsmanager]"
                echo "[namespaceDomain/cleaner]"
                echo "[namespaceDomain/acl]"
                ;;
            spacemanager)
                echo "[$domain]"
                echo "[$domain/spacemanager]"
                ;;
            transfermanagers)
                echo "[$domain]"
                echo "[$domain/transfermanagers]"
                ;;
            info)
                echo "[$domain]"
                echo "[$domain/info]"
                ;;
            *)
                echo "WARNING: $domain is unknown" 1>&2
        esac
    done
) > "${DCACHE_HOME}/etc/layouts/imported.conf"

echo
disclaimer
