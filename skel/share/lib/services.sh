# Useful functions for working with services and domains.

# The concept of a service is an abstraction used in dCache scripts. A
# service is the type of a domain. A service can have zero or more
# domains. A service corresponds to a job file in the jobs directory.
#
# This script defines functions for mapping services to domains,
# domains to services, starting and stopping domains, etc.
#

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
            printf "%s" "gridftp-${hostname}Domain "
            ;;

        webdav)
            printf "%s" "webdav-${hostname}Domain "
            ;;

        gsidcap)
            printf "%s" "gsidcap-${hostname}Domain "
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

# Given a mixed list of domains and services, prints all domains for
# those services and domains. I.e. the domains are printed literally,
# and the services are expanded to the list of domains for those
# services. If no arguments are specified, a list of all configured
# domains is printed.
printExpandedServiceAndDomainList() # in $* = list of services and domains
{
    if [ $# -eq 0 ]; then
        printAllDomains
    else
        for s in $*; do
            printDomains $s
        done
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
            gridftp-*Domain) ret="gridftp" ;;
            webdav-*Domain)  ret="webdav" ;;
            gsidcap-*Domain) ret="gsidcap" ;;
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

# If domain runs, provides its PID. Returns 0 if domain is running, 1
# otherwise.
getPidOfDomain() # in $1 = Domain name, out $2 = pid
{
    local domain
    local pidFile
    local pidDir
    local service
    local ret

    domain="$1"

    getConfigurationValue "$domain" pidDir pidDir || return
    getService "$domain" service || return

    if [ "$service" = "srm" ]; then
        pidFile="${pidDir:-$DCACHE_PID}/dcache.$domain.pid"
    else
        pidFile="${pidDir:-$DCACHE_PID}/dcache.$domain-daemon.pid"
    fi
    [ -f "${pidFile}" ] || return

    ret=$(cat "${pidFile}")
    isRunning "${ret}" || return

    eval $2=\"$ret\"
}

# If domain runs, provides the PID of its Java process.  Returns 0 if
# domain is running, 1 otherwise.
getJavaPidOfDomain() # in $1 = Domain name, out $2 = pid
{
    local domain
    local pidFile
    local pidDir
    local service
    local ret

    domain="$1"

    getConfigurationValue "$domain" pidDir pidDir || return
    getService "$domain" service || return

    if [ "$service" = "srm" ]; then
        pidFile="${pidDir:-$DCACHE_PID}/dcache.$domain.pid"
    else
        pidFile="${pidDir:-$DCACHE_PID}/dcache.$domain-java.pid"
    fi
    [ -f "${pidFile}" ] || return

    ret=$(cat "${pidFile}")
    isRunning "${ret}" || return

    eval $2=\"$ret\"
}

# Returns the name of the log file used by a domain.
getLogOfDomain() # in $1 = Domain name, out $2 = log of domain
{
    local domain
    local service
    local logArea
    local ret

    domain="$1"

    getService "${domain}" service || return

    case "$service" in
        srm)
            # Getting the location of the SRM stuff is unfortunately
            # somewhat messy...
            if [ -r ${DCACHE_ETC}/srm_setup.env ] && [ -r ${DCACHE_HOME}/bin/dcache-srm ]; then
                . ${DCACHE_ETC}/srm_setup.env
                eval $(grep "export CATALINA_HOME" ${DCACHE_BIN}/dcache-srm)
                ret="${CATALINA_HOME}/logs/catalina.out"
            fi
            ;;
        *)
            getConfigurationValue $domain logArea logArea || return
            ret="${logArea:-$DCACHE_LOG}/${domain}.log"
            ;;
    esac

    eval $2=\"$ret\"
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
    local tmp
    name="$1"

    case "${name}" in
        srm|srm-*Domain)
            filename="srmSetup"
            ;;
        dcap)
            filename="doorSetup"
            ;;
        dcap*-*Domain)
	    tmp=${name%%-*Domain}
            filename="door${tmp#dcap}Setup"
            ;;
        xrootd|xrootd-*Domain)
            filename="xrootdDoorSetup"
            ;;
        gridftp|gridftp-*Domain)
            filename="gridftpdoorSetup"
            ;;
        gsidcap|gsidcap-*Domain)
            filename="gsidcapdoorSetup"
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
        ourHomeDir="${DCACHE_HOME}" readconf "$file" "$2_" || return
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

# Returns the program path used when starting and stopping a service.
getJob() # in $1 = service, out $2 = job
{
    local ret
    case "${service}" in
        srm)
            ret="${DCACHE_BIN}/dcache-srm"
            ;;
        dcap)
            # $domain has the format 'dcap${door}-${host}Domain'
            door=${domain#dcap}
            door=${door%-${hostname}Domain}
            ret="${DCACHE_JOBS}/door${door}"
            ;;
        xrootd)
            ret="${DCACHE_JOBS}/xrootdDoor"
            ;;
        gridftp)
            ret="${DCACHE_JOBS}/gridftpdoor"
            ;;
        gsidcap)
            ret="${DCACHE_JOBS}/gsidcapdoor"
            ;;
        admin)
            ret="${DCACHE_JOBS}/adminDoor"
            ;;
        *)
            ret="${DCACHE_JOBS}/${service}"
            ;;
    esac
    eval $2=\"$ret\"
}

# Starts or stops a given domain.
runDomain() # in $1 = domain, in $2 = action
{
    local domain
    local action
    local service
    local program
    local door

    domain=$1
    action=$2

    getService "$1" service || return
    getJob "$service" program || return

    if [ ! -x "$program" ]; then
        fail 1 "$program not found. The dCache domain $domain is
                probably not configured on this host. If you recently
                configured it, then you may need to rerun the
                install.sh script to enable it."
    fi

    case "${service}" in
        pool)
            "${program}" "-pool=${domain%Domain}" ${action} || return
            ;;
        srm)
            "${program}" ${action} || return
            ;;
        *)
            "${program}" "-domain=${domain}" ${action} || return
            ;;
    esac
}
