#!/bin/bash

set -e


#  Look to see if there are ssh related files located in the config
#  directory.  If there are, migrate them to $DCACHE_HOME/etc.
migrateSshFiles()
{
    migrateFileFromConfigToEtc authorized_keys
    migrateFileFromConfigToEtc server_key
    migrateFileFromConfigToEtc server_key.pub
    migrateFileFromConfigToEtc host_key
    migrateFileFromConfigToEtc host_key.pub
}


migrateFileFromConfigToEtc() # in $1 = name of file
{
    config_path=$(getProperty dcache.paths.config)
    etc_path=$(getProperty dcache.paths.etc)

    copyIfNew "$config_path/$1" "$etc_path/$1"
}


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
    local hostname

    hostname="$(getProperty host.name)"

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
    local hostname
    hostname="$(getProperty host.name)"
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


# Convert the chimera.properties defaults to a XML file
buildChimeraDefaultsXml()  # out $1 = variable to store filename of XML file
{
    _file=$(mktemp)
    sed -n '1 i <defaults>
s%.*\(url\|user\|password\|dialect\|driver\) *= *\([^ ]*\) *$%  <\1>\2</\1>%p
$ a </defaults>' < "$(getProperty dcache.paths.share)/defaults/chimera.properties" > $_file
    cmd=$1=$_file
    eval $cmd
}


# Print the Chimera options that are different from the defaults
printChimeraOptions()
{
    buildChimeraDefaultsXml xmlFile

    saxonDir=$(getProperty saxonDir)
    xsltDir="$(getProperty dcache.paths.share)/xml/xslt"

    chimeraConfig=$(findFirstFileOf ${DCACHE_CONFIG}/chimera-config.xml \
	${DCACHE_CONFIG}/chimera-confg.xml.rpmsave \
	${DCACHE_HOME}/share/migration/chimera-config-from-195.xml)

    "${JAVA}" -classpath "${saxonDir}/saxon.jar" com.icl.saxon.StyleSheet  \
        "${chimeraConfig}"  \
        "${xsltDir}/convert-chimera-config.xsl" \
	"defaults-uri=$xmlFile"

    rm $xmlFile
}


#  Print the first available file from a list of candidates and return 0. Returns
#  non-zero if none of the files exist.
findFirstFileOf()
{
    local searched_files

    searched_files=
    while [ $# -gt 0 ]; do
      if [ -f "$1" ]; then
        echo "$1"
        return 0
      fi
      searched_files="$searched_files $1"
      shift
    done

    printp "Cannot find any of $searched_files" 1>&2
    return 1
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
  echo "-d PATH               Path to dCache installation (default is @dcache.home@)"
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
    printpi "Renaming $1 to $(basename $1).pre-197" "^Renaming"
    mv "$1" "$1.pre-197"
  fi
}

copyIfNew() # $1 = src, $2 = dst
{
  if [ -f "$1" ] && [ ! -f "$2" ]; then
    printpi "Copying $1 to $2" "^Copying"
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

# emit lines of a dCacheSetup file suitable for dcache.conf
emitDcacheSetup() # in $1 filename
{
    cat "$1" | \
        filterOutDataCopiedFromTemplate | \
        filterFixPropertyReferences | \
        filterOutMultipleAssigments
}


filterOutDataCopiedFromTemplate()
{
    tmp=$(mktemp)
    sed -f "${DCACHE_LIB}/config.sed" > $tmp
    diff -u "$(getProperty dcache.paths.share)/migration/dCacheSetup-from-195.canonical" $tmp | \
        doTail +4 | \
        grep ^+ | \
        cut -b2- | \
        sed -e 's/="\(.*\)"$/=\1/'
    rm -f $tmp
}

doTail()
{
    case "$(uname -s)" in
        SunOS)
            tail $1
            ;;
        *)
            tail -n$1
            ;;
    esac
}

filterFixPropertyReferences()
{
    sed -f "$(getProperty dcache.paths.share)/migration/deprecated.sed"
}


# print lines of a dCacheSetup file so assigments that have their
# property reassigned a different value later on are commented-out
filterOutMultipleAssigments() # in $1 filename
{
    tmp=$(mktemp)
    cat - > $tmp

    redefined_properties_file=$(mktemp)
    line_number=0
    cat $tmp | while IFS="" read -r line_text; do
        line_number=$(( $line_number + 1 ))

        if shouldLineBeExcluded $tmp $line_number "$line_text"; then
            echo "# $line_text"
        else
            echo "$line_text"
        fi
    done

    redefinedProperties=$(cat $redefined_properties_file)
    rm -f $redefined_properties_file $tmp
}


shouldLineBeExcluded() # in $1 filename, $2 line number, $3 line text
{
    if isLineAPropertyAssignment "$3"; then
        extractPropertyFromAssignment property "$3"

        if isPropertyRedefinedLater "$1" $2 "$property"; then
            acceptRedefinedProperty "$property"
            return 0
        fi
    fi

    return 1
}

acceptRedefinedProperty() # in $1 property name
{
    if ! contains "$property" $redefinedProperties; then
	redefinedProperties="$redefinedProperties $property"
	echo -n "$property " >> $redefined_properties_file
    fi
}

isLineAPropertyAssignment() # $1 line of text
{
    echo "$1" | grep '^ *[^#=][^=]*=' > /dev/null
}

extractPropertyFromAssignment() # out $1 property, in $2 line of config file.
{
    ret="$(echo "$2" | sed 's/^ *//;s/ *=.*$//')"
    eval $1=\"$ret\"
}

isPropertyRedefinedLater() # $1 filename, $2 line number of assignment, $3 property name
{
    makeReFromString re_fragment "$3"
    doTail +$(($2 + 1)) "$1" | grep "^[ \t]*$re_fragment[ \t]*=" >/dev/null
}

makeReFromString() # out $1 RE, in $2 an arbitrary string
{
    ret="$(echo "$2" | sed 's/\./\\./g;s/\$/\\$/g;s/\[/\\[/g')"
    eval $1=\"$ret\"
}

# Set home path
if [ -z "$DCACHE_HOME" ]; then
    DCACHE_HOME="@dcache.home@"
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

# Load libraries
. @dcache.paths.bootloader@/loadConfig.sh
. ${DCACHE_LIB}/utils.sh

# Determine file names
layout_file="${DCACHE_ETC}/layouts/$(getProperty host.name).conf"
config_file="${DCACHE_ETC}/dcache.conf"
setup_file="${DCACHE_CONFIG}/dCacheSetup"
node_config_file="${DCACHE_ETC}/node_config"
door_config_file="${DCACHE_ETC}/door_config"

# Check preconditions
require sed cat tr mv cp basename tail date uname mktemp

# Migrate SSH admin files from config to etc
migrateSshFiles

if [ ! -f "${node_config_file}" -a ! -f "${door_config_file}" ]; then
    fail 1 "Cannot proceed because ${node_config_file} does not exist."
fi

if [ ! -f "${setup_file}" ]; then
    fail 1 "Cannot proceed because ${setup_file} does not exist."
fi

if [ -z "$force" ]; then
    if ! fileNotPrecious "${config_file}"; then
        fail 1 "Cannot proceed because ${config_file} already exists."
    fi

    if [ -e "${layout_file}" ]; then
        fail 1 "Cannot proceed because ${layout_file} already exists."
    fi
fi

# Load old configuration
ourHomeDir="${DCACHE_HOME}"
readconf "${node_config_file}" NODE_CONFIG_ ||
readconf "${door_config_file}" NODE_CONFIG_ ||
fail 1 "Failed to read ${node_config_file}"

loadConfigurationFile dCache dCache ||
fail 1 "Failed to read dCacheSetup file"
loadConfigurationFile pool pool || loadConfigurationFile dCache pool

# Keep the java setting
if [ -n "$dCache_java" ]; then
    if [ -d "/etc/default/" ]; then
        echo "Generating /etc/default/dcache"
        echo "JAVA=\"$dCache_java\"" >> /etc/default/dcache
    else
        echo "Generating /etc/dcache.env"
        echo "JAVA=\"$dCache_java\"" >> /etc/dcache.env
    fi
fi

# Create configuration file
printp "Generating ${config_file}"

generateDcacheConf() # in $1 dCacheSetup path
{
    echo "# Auto generated configuration file."
    echo "#"
    echo "# Source: $1"
    echo "# Date: $(date)"
    echo "#"
    disclaimer "# "
    echo

    echo "dcache.layout=\${host.name}"

    # Name space declaration
    case "$NODE_CONFIG_NAMESPACE" in
        chimera)
            echo "dcache.namespace=chimera"
            printChimeraOptions
            ;;

        *)
            echo "dcache.namespace=pnfs"
            ;;
    esac

    echo
    echo "# The following is taken from the old dCacheSetup file."
    echo "# Some configuration parameters may no longer apply."
    echo

    emitDcacheSetup "$1"
} > "${config_file}"

generateDcacheConf "${setup_file}"
renameToPreMigration "${setup_file}"
if [ "$NODE_CONFIG_NAMESPACE" = "chimera" ]; then
    renameToPreMigration "${DCACHE_CONFIG}/chimera-config.xml"
fi

# Create layout file
printp "Generating ${layout_file}"
(
    echo "# Auto generated layout file."
    echo "#"
    echo "# Source: ${node_config_file}"
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
) > "${layout_file}"
renameToPreMigration "${node_config_file}"
renameToPreMigration "${door_config_file}"
echo

copyIfNew "${DCACHE_JOBS}/dcache.local.sh" "${DCACHE_BIN}/dcache.local.sh"
copyIfNew "${DCACHE_JOBS}/dcache.local.run.sh" "${DCACHE_BIN}/dcache.local.run.sh"

echo
disclaimer

if [ -n "$redefinedProperties" ]; then
  echo
  echo "NOTICE: the dCacheSetup file contains at least one property that is"
  echo "        assigned a value multiple times.  For such properties,"
  echo "        previous versions of dCache would use the assignment closest"
  echo "        to the end of dCacheSetup file (the last assignment \"wins\")."
  echo
  echo "        A dcache.conf file may not have more than one line that"
  echo "        assigns a value to the same property.  If the file attempts"
  echo "        to redefine a property's value then an error message is"
  echo "        reported and dCache will not start."
  echo
  echo "        The migration script has created a dcache.conf file, based on"
  echo "        dCacheSetup, except that lines where a property is assigned a"
  echo "        value that is subsequent redefined have been commented out."
  echo "        The last assignment line (the one that would \"win\") is left"
  echo "        uncommented.  The resulting dcache.conf file should give the"
  echo "        same behaviour as the dCacheSetup file."
  echo
  echo "        This affects the following properties: $redefinedProperties"
fi

echo
printp "Checking the migrated configuration for any problems."
echo
rc=0
${DCACHE_BIN}/dcache check-config || rc=$?
case $rc in
    2)
        echo
        printp "dCache can't work with the current configuration.
                Please fix (at least) all the ERRORs and rerun the
                check with the command:"
        echo
        echo "    ${DCACHE_BIN}/dcache check-config"
        exit 2
        ;;
    1)
        echo
        printp "The migrated configuration seem OK.  The WARNINGs
                are worth investigating, but dCache should work fine.
                You can re-check the configuration with the command:"
        echo
        echo "    ${DCACHE_BIN}/dcache check-config"
        exit 1
        ;;
    0)
        ;;
esac
