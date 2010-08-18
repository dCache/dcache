#!/bin/bash

set -e

# Returns 0 if option is set to 'yes' or 'y' in node_config or
# door_config, 1 otherwise.
isNodeConfigEnabled() # in $1 = option name
{
    local value
    eval value=\"\$NODE_CONFIG_$1\"
    case "$(echo $value | tr '[A-Z]' '[a-z]')" in
        yes|y)
            return 0;
            ;;
        *)
            return 1;
            ;;
    esac
}

# Prints all domains for the given service. Services like 'pool' and
# 'dcap' may have more than one domain. Notice that this function is
# not limitted to configured domains.
printDomains() # in $1 = service
{
    local i
    local door
    local file

    case "$1" in
        dcap)
            for i in ${DCACHE_CONFIG}/door*Setup; do
                door=$(echo $i | sed -e "s#.*door\(.*\)Setup#\1#")
                printf "%s" "dcap${door}-${hostname}Domain "
            done
            ;;

        gPlazma)
            printf "%s" "gPlazma-${hostname}Domain "
            ;;

        xrootd)
            printf "%s" "xrootd-${hostname}Domain "
            ;;

        gridftp)
            for i in ${DCACHE_CONFIG}/gridftpdoor*Setup; do
                door=$(echo $i | sed -e "s#.*gridftpdoor\(.*\)Setup#\1#")
                printf "%s" "gridftp${door}-${hostname}Domain "
            done
            ;;

        weakftp)
            for i in ${DCACHE_CONFIG}/weakftpdoor*Setup; do
                door=$(echo $i | sed -e "s#.*weakftpdoor\(.*\)Setup#\1#")
                printf "%s" "weakftp${door}-${hostname}Domain "
            done
            ;;

        kerberosftp)
            for i in ${DCACHE_CONFIG}/kerberosftpdoor*Setup; do
                door=$(echo $i | sed -e "s#.*kerberosftpdoor\(.*\)Setup#\1#")
                printf "%s" "kerberosftp${door}-${hostname}Domain "
            done
            ;;

        webdav)
            printf "%s" "webdav-${hostname}Domain "
            ;;

        gsidcap)
            for i in ${DCACHE_CONFIG}/gsidcapdoor*Setup; do
                door=$(echo $i | sed -e "s#.*gsidcapdoor\(.*\)Setup#\1#")
                printf "%s" "gsidcap${door}-${hostname}Domain "
            done
            ;;

        kerberosdcap)
            for i in ${DCACHE_CONFIG}/kerberosdcapdoor*Setup; do
                door=$(echo $i | sed -e "s#.*kerberosdcapdoor\(.*\)Setup#\1#")
                printf "%s" "kerberosdcap${door}-${hostname}Domain "
            done
            ;;

        srm)
            printf "%s" "srm-${hostname}Domain "
            ;;

        pool)
            for i in $(printAllPoolDomains); do
                getPoolListFile $i file
                if ! isFileEmpty "$file"; then
                    printf "%s" "$i "
                fi
            done
            ;;

        admin)
            printf "adminDoorDomain "
            ;;

        dir)
            printf "dirDomain "
            ;;

        *Domain)
            printf "%s" "$1 "
            ;;

        *)
            printf "%s" "${1}Domain "
            ;;
    esac
}

# Prints domains used by all node types as defined by legacy
# node_config variables.
printLegacyDomains()
{
    if isNodeConfigEnabled replicaManager; then
        printDomains replica
    fi
    if isNodeConfigEnabled DCAP; then
        printDomains dcap
    fi
    if isNodeConfigEnabled XROOTD; then
        printDomains xrootd
    fi
    if isNodeConfigEnabled GRIDFTP; then
        printDomains gridftp
    fi
    if isNodeConfigEnabled GSIDCAP; then
        printDomains gsidcap
    fi
    if isNodeConfigEnabled SPACEMANAGER; then
        printDomains spacemanager
    fi
    if isNodeConfigEnabled TRANSFERMANAGERS; then
        printDomains transfermanagers
    fi
    if isNodeConfigEnabled SRM; then
        printDomains srm
    fi
}

# Prints domains used by admin and custom nodes as defined by legacy
# node_config variables.
printLegacyAdminDomains()
{
    if isNodeConfigEnabled info; then
        printDomains info
    fi
    if isNodeConfigEnabled statistics; then
        printDomains statistics
    fi
}

# Prints domains used by custom nodes as defined by legacy node_config
# variables.
printLegacyCustomDomains()
{
    if isNodeConfigEnabled lmDomain; then
        printDomains lm
    fi
    if isNodeConfigEnabled poolManager; then
        printDomains dCache
    fi
    if isNodeConfigEnabled dirDomain; then
        printDomains dir
    fi
    if isNodeConfigEnabled adminDoor; then
        printDomains admin
    fi
    if isNodeConfigEnabled httpDomain; then
        printDomains httpd
    fi
    if isNodeConfigEnabled utilityDomain; then
        printDomains utility
    fi
    if isNodeConfigEnabled gPlazmaService; then
        printDomains gPlazma
    fi
    if isNodeConfigEnabled pnfsManager; then
        printNameSpaceDomains
    fi
}

# Prints domain hosting PnfsManager
printNameSpaceDomains()
{
    case "$NODE_CONFIG_NAMESPACE" in
        chimera)
            printDomains chimera
            ;;
        *)
            printDomains pnfs
            ;;
    esac
}

# Prints all configured domains
printAllDomains()
{
    case "$NODE_CONFIG_NODE_TYPE" in
        admin)
            printDomains lm
            printDomains dCache
            printDomains dir
            printDomains admin
            printDomains httpd
            printDomains utility
            printDomains gPlazma
            printNameSpaceDomains
            printDomains pool
            printLegacyAdminDomains
            printLegacyDomains
            ;;

        custom)
            printLegacyCustomDomains
            printDomains pool
            printLegacyAdminDomains
            printLegacyDomains
            ;;
        pool)
            printDomains pool
            printLegacyDomains
            ;;

        door)
            printLegacyDomains
            ;;
    esac

    for service in $NODE_CONFIG_SERVICES; do
        printDomains $service
    done
}

# Prints the list of all configured pool domains, including empty
# domains.
printAllPoolDomains()
{
    if [ -f "${pool_config}/${hostname}.domains" ]; then

        while read domain; do
            if [ ! -f "${pool_config}/${domain}.poollist" ]; then
                printp "Requested pool list file not found (skipped):
                        ${domain}.poollist" 1>&2
            else
                printf "%s" "${domain}Domain "
            fi
        done < "${pool_config}/${hostname}.domains"

    elif [ -f "${pool_config}/${hostname}.poollist" ]; then
        printf "%s" "${hostname}Domain "
    fi
}

# Returns the service name of a domain. The service name corresponds
# to the name of the batch file of that service, without the file
# suffix.
getService() # in $1 = domain name, out $2 = service
{
    local ret
    if contains $1 $(printAllPoolDomains); then
        ret="pool"
    else
        case "$1" in
            dcap*-*Domain)   ret="dcap" ;;
            gPlazma-*Domain) ret="gPlazma" ;;
            xrootd-*Domain)  ret="xrootd" ;;
            gridftp*-*Domain) ret="gridftp" ;;
            weakftp*-*Domain) ret="weakftp" ;;
            kerberosftp*-*Domain) ret="kerberosftp" ;;
            webdav-*Domain)  ret="webdav" ;;
            gsidcap*-*Domain) ret="gsidcap" ;;
            kerberosdcap*-*Domain) ret="kerberosdcap" ;;
            srm-*Domain)     ret="srm" ;;
            adminDoorDomain) ret="admin" ;;
            *Domain)         ret="${1%Domain}" ;;
            *)               return 1 ;;
        esac
    fi
    eval $2=\"$ret\"
}

# Given a list of domains, prints a list of corresponding service
# names.
printServices() # in $* = list of domains
{
    local service
    for domain in $*; do
        getService $domain service
        printf "${service} "
    done
}

# Returns the name of the pool list file for the given pool domain.
getPoolListFile() # in $1 = domain name, out $2 = pool list file
{
    eval $2=\"${pool_config}/${1%Domain}.poollist\"
}

# Returns the setup file path for a service or domain
getConfigurationFile() # in $1 = service or domain, out $2 = configuration file
{
    local filename
    local name
    name="$1"

    case "${name}" in
        srm|srm-*Domain)
            filename="srmSetup"
            ;;
        dcap)
            filename="doorSetup"
            ;;
        dcap*-*Domain)
	    name=${name%%-*Domain}
            filename="door${name#dcap}Setup"
            ;;
        xrootd|xrootd-*Domain)
            filename="xrootdDoorSetup"
            ;;
        weakftp|weakftp*-*Domain)
	    tmp=${name%%-*Domain}
            filename="weakftpdoor${tmp#weakftp}Setup"
            ;;
        kerberosftp|kerberosftp*-*Domain)
	    tmp=${name%%-*Domain}
            filename="kerberosftpdoor${tmp#kerberosftp}Setup"
            ;;
        gridftp|gridftp*-*Domain)
	    tmp=${name%%-*Domain}
            filename="gridftpdoor${tmp#gridftp}Setup"
            ;;
        kerberosdcap|kerberosdcap*-*Domain)
	    tmp=${name%%-*Domain}
            filename="kerberosdcapdoor${tmp#kerberosdcap}Setup"
            ;;
        gsidcap|gsidcap*-*Domain)
	    tmp=${name%%-*Domain}
            filename="gsidcapdoor${tmp#gsidcap}Setup"
            ;;
        admin|adminDoorDomain)
            filename="adminDoorSetup"
            ;;
        gPlazma|gPlazma-*Domain)
            filename="gPlazmaSetup"
            ;;
        webdav|webdav-*Domain)
            filename="webdavSetup"
            ;;
        *Domain)
	    if contains $name $(printAllPoolDomains); then
		filename="poolSetup"
	    else
		filename="${name%Domain}Setup"
	    fi
            ;;
        *)
            filename="${name}Setup"
            ;;
    esac

    if [ -f "${DCACHE_CONFIG}/${filename}" ] ; then
        eval $2=\"${DCACHE_CONFIG}/${filename}\"
    elif [ -f "${DCACHE_CONFIG}/${filename}-$(uname -n)" ] ; then
        eval $2=\"${DCACHE_CONFIG}/${filename}-$(uname -n)\"
    elif [ -f "/etc/${filename}" ] ; then
        eval $2=\"/etc/${filename}\"
    else
        return 1
    fi
}

# Loads a setup into environment variables. All environment variables
# are prefixed by $2 followed by an underscore. Only loads the setup
# file the first time the function is called for a given prefix.
loadConfigurationFile() # in $1 = service or domain, in $2 = prefix
{
    local file
    if eval "[ -z \"\$SETUP_$2\" ]"; then
        getConfigurationFile "$1" file || return
        readconf "$file" "$2_" || return
        eval "SETUP_$2=1"
    fi
}

# Returns configuration value for a service
getConfigurationValue() # in $1 = service or domain, in $2 = key, out $3 = value
{
    local prefix
    prefix=$(echo $1 | sed -e 's/_/__/g' -e 's/-/_/g')
    loadConfigurationFile $1 $prefix && eval $3=\"\${${prefix}_$2}\"
}

# Writes the poollist file of the given pool domain to stdout. The
# format is 'name directory parameters'. Aborts if the file does not
# exist.
printPoolsInDomain() # in $1 = Pool domain
{
    local poolFile

    getPoolListFile $1 poolFile
    if [ ! -f ${poolFile} ]; then
        printp "Pool file not found: ${poolFile}" 1>&2
        exit 4
    fi
    cat $poolFile
}

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

renameToPreMigration() # $1 = file name
{
  if [ -f "$1" ]; then
    printp "Renaming $1 to $1.pre-197"
    mv "$1" "$1.pre-197"
  fi
}

copyIfNew() # $1 = src, $2 = dst
{
  if [ -f "$1" ] && [ ! -f "$2" ]; then
    printp "Copying $1 to $2"
    cp -p "$1" "$2"
    renameToPreMigration "$1"
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

# Load libraries
. ${DCACHE_HOME}/share/lib/paths.sh
. ${DCACHE_LIB}/utils.sh
. ${DCACHE_LIB}/config.sh

# Check preconditions
if [ ! -f "${DCACHE_HOME}/etc/node_config" -a ! -f "${DCACHE_HOME}/etc/door_config" ]; then
    fail 1 "Cannot proceed because ${DCACHE_HOME}/etc/node_config does not exist."
fi

if [ ! -f "${DCACHE_HOME}/config/dCacheSetup" ]; then
    fail 1 "Cannot proceed because ${DCACHE_HOME}/config/dCacheSetup does not exist."
fi


if [ -z "$force" ]; then
    if ! fileNotPrecious "${DCACHE_HOME}/etc/dcache.conf"; then
        fail 1 "Cannot proceed because ${DCACHE_HOME}/etc/dcache.conf already exists."
    fi

    if [ -e "${DCACHE_HOME}/etc/layouts/imported.conf" ]; then
        fail 1 "Cannot proceed because ${DCACHE_HOME}/etc/layouts/imported.conf already exists."
    fi
fi

# Load old configuration
readconf ${DCACHE_HOME}/etc/node_config NODE_CONFIG_ ||
readconf ${DCACHE_HOME}/etc/door_config NODE_CONFIG_ ||
fail 1 "Failed to read ${DCACHE_HOME}/etc/node_config"

loadConfigurationFile dCache dCache ||
fail 1 "Failed to read dCacheSetup file"
loadConfigurationFile pool pool || loadConfigurationFile dCache pool

determineHostName

# Create configuration file
printp "Converting ${DCACHE_HOME}/etc/dCacheSetup
        to ${DCACHE_HOME}/etc/dcache.conf."
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
renameToPreMigration "${DCACHE_HOME}/config/dCacheSetup"
echo

# Create layout file
printp "Converting ${DCACHE_HOME}/etc/node_config
        to ${DCACHE_HOME}/etc/layouts/imported.conf."
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
                            lfs=*|poolIoQueue=*|checkRepository=*|gsiftpAllowMmap=*|metaDataRepository=*|metaDataRepositoryImport=*|gsiftpReadAhead=*|allowCleaningPreciousFiles=*|poolupDestination=*|pnfsmanager=*|flushMessageTarget=*|sweeper=*)
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
renameToPreMigration "${DCACHE_HOME}/etc/node_config"
renameToPreMigration "${DCACHE_HOME}/etc/door_config"
echo

copyIfNew "${DCACHE_JOBS}/dcache.local.sh" "${DCACHE_BIN}/dcache.local.sh"
copyIfNew "${DCACHE_JOBS}/dcache.local.run.sh" "${DCACHE_BIN}/dcache.local.run.sh"

echo
disclaimer
