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
printDomains() # $1 = service
{
    local i
    local door

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

        gsidcap)
            printf "%s" "gsidcap-${hostname}Domain "
            ;;

        srm)
            printf "%s" "srm-${hostname}Domain "
            ;;

        pool)
            for i in $(printAllPoolDomains); do
                getPoolListFile $i
                if ! isFileEmpty "$RET"; then
                    printf "%s" "$i "
                fi
            done
            ;;

        admin)
            printf "adminDoorDomain "
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
    if isNodeConfigEnabled SRM; then
        printDomains srm
    fi
}

# Prints domains used by admin and custom nodes as defined by legacy
# node_config variables.
printLegacyAdminDomains()
{
    if isNodeConfigEnabled infoProvider; then
        printDomains infoProvider
    fi
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
    if [ -f ${pool_config}/${hostname}.domains ]; then

        while read domain; do
            if [ ! -f ${pool_config}/${domain}.poollist ]; then
                printp "Requested pool list file not found (skipped):
                        ${domain}.poollist" 1>&2
            else
                printf "%s" "${domain}Domain "
            fi
        done < ${pool_config}/${hostname}.domains

    elif [ -f ${pool_config}/${hostname}.poollist ]; then
        printf "%s" "${hostname}Domain "
    fi
}

# Given a mixed list of domains and services, prints all domains for
# those services and domains. I.e. the domains are printed literally,
# and the services are expanded to the list of domains for those
# services. If no arguments are specified, a list of all configured
# domains is printed.
printExpandedServiceAndDomainList() # $* = list of services and domains
{
    if [ $# -eq 0 ]; then
        printAllDomains
    else
        for s in $*; do
            printDomains $s
        done
    fi
}

# Provides the service name of a domain in RET. The service name
# corresponds to the name of the batch file of that service, without
# the file suffix.
getService() # $1 = domain name
{
    if contains $1 $(printAllPoolDomains); then
        RET="pool"
    else
        case "$1" in
            dcap*-*Domain)
                RET="dcap"
                ;;

            gPlazma-*Domain)
                RET="gPlazma"
                ;;

            xrootd-*Domain)
                RET="xrootd"
                ;;

            gridftp-*Domain)
                RET="gridftp"
                ;;

            gsidcap-*Domain)
                RET="gsidcap"
                ;;

            srm-*Domain)
                RET="srm"
                ;;

            adminDoorDomain)
                RET="admin"
                ;;

            *Domain)
                RET="${1%Domain}"
                ;;

            *)
                return 1
                ;;
        esac
    fi
    return 0
}

# Given a list of domains, prints a list of corresponding service
# names.
printServices() # $* = list of domains
{
    for domain in $*; do
        getService $domain
        printf "${RET} "
    done
}

# If domain runs, RET is set to its PID.  Returns 0 if domain is
# running, 1 otherwise.
getPidOfDomain() # $1 = Domain name
{
    local domain
    local pidFile
    local pid

    domain="$1"

    getService "$domain" || return
    getServiceConfigurationValue "$RET" pidDir || return

    pidFile="${RET:-$DCACHE_PID}/dcache.$domain.pid"
    [ -f ${pidFile} ] || return 1

    pid=$(cat ${pidFile})
    isRunning ${pid} || return 1

    RET=${pid}
    return 0
}

# Provides the name of the log file used by a domain in RET.
getLogOfDomain() # $1 = Domain name
{
    local domain
    local service
    domain=$1

    getService ${domain} || return; service="$RET"

    case "$service" in
        srm)
            # Getting the location of the SRM stuff is unfortunately
            # somewhat messy...
            if [ -r ${DCACHE_ETC}/srm_setup.env ] && [ -r ${DCACHE_HOME}/bin/dcache-srm ]; then
                . ${DCACHE_ETC}/srm_setup.env
                eval $(grep "export CATALINA_HOME" ${DCACHE_BIN}/dcache-srm)
                RET="${CATALINA_HOME}/logs/catalina.out"
            fi
            ;;
        *)
            getServiceConfigurationValue $service logArea
            RET="${RET:-$DCACHE_LOG}/${domain}.log"
            ;;
    esac

    return 0
}


# Stores the name of the pool list file for the given pool domain in
# RET.
getPoolListFile() # $1 = domain name
{
    RET="${pool_config}/${1%Domain}.poollist"
}

# Returns the setup file path for a service.
getServiceConfigurationFile() # $1 = service
{
    local filename
    local service
    service="$1"

    case "${service}" in
        srm)
            filename="srmSetup"
            ;;
        dcap)
            filename="doorSetup"
            ;;
        xrootd)
            filename="xrootdDoorSetup"
            ;;
        gridftp)
            filename="gridftpdoorSetup"
            ;;
        gsidcap)
            filename="gsidcapdoorSetup"
            ;;
        admin)
            filename="adminDoorSetup"
            ;;
        *)
            filename="${service}Setup"
            ;;
    esac

    if [ -f "${DCACHE_CONFIG}/${filename}" ] ; then
        RET="${DCACHE_CONFIG}/${filename}"
    elif [ -f "${DCACHE_CONFIG}/${filename}-$(uname -n)" ] ; then
        RET="${DCACHE_CONFIG}/${filename}-$(uname -n)"
    elif [ -f "/etc/${filename}" ] ; then
        RET="/etc/${filename}"
    else
        return 1
    fi
}

# Loads a service setup into environment variables prefixed with the
# service name. Only loads the setup file the first time the function
# is called.
loadServiceConfigurationFile() # $1 = service
{
    if eval "[ -z \"\$SETUP_$1\" ]"; then
        getServiceConfigurationFile "$1" || return
        ourHomeDir="${DCACHE_HOME}" readconf "$RET" "$1_" || return
        eval "SETUP_$1=1"
    fi
}

# Returns configuration value for a service
getServiceConfigurationValue() # $1 = service, $2 = key
{
    loadServiceConfigurationFile $1 && eval RET="\${$1_$2}"
}

# Returns the program path used when starting and stopping a service.
getJob() # $1 = service
{
    case "${service}" in
        srm)
            RET="${DCACHE_BIN}/dcache-srm"
            ;;
        dcap)
            # $domain has the format 'dcap${door}-${host}Domain'
            door=${domain#dcap}
            door=${door%-${hostname}Domain}
            RET="${DCACHE_JOBS}/door${door}"
            ;;
        xrootd)
            RET="${DCACHE_JOBS}/xrootdDoor"
            ;;
        gridftp)
            RET="${DCACHE_JOBS}/gridftpdoor"
            ;;
        gsidcap)
            RET="${DCACHE_JOBS}/gsidcapdoor"
            ;;
        admin)
            RET="${DCACHE_JOBS}/adminDoor"
            ;;
        *)
            RET="${DCACHE_JOBS}/${service}"
            ;;
    esac
}

# Starts or stops a given domain.
runDomain() # $1 = domain, $2 = action
{
    local domain
    local action
    local service
    local program
    local door

    domain=$1
    action=$2

    getService "$1" || return
    service="$RET"

    getJob "$service"
    program="$RET"

    if [ ! -x $program ]; then
        fail 1 "$program not found. The dCache domain $domain is
                probably not configured on this host. If you recently
                configured it, then you may need to rerun the
                install.sh script to enable it."
    fi

    case "${service}" in
        pool)
            ${program} -pool=${domain%Domain} ${action}
            ;;
        srm)
            ${program} ${action}
            ;;
        *)
            ${program} -domain=${domain} ${action}
            ;;
    esac
}
