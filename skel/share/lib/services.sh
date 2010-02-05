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

# Returns the PID stored in pidfile
getPidFromFile() # in $1 = pidfile, out $2 = pid
{
    local ret
    [ -f "$1" ] && ret=$(cat "$1") && [ "$ret" ] && eval $2=\"$ret\"
}

# Returns the PID directory of domain
getPidDir() # in $1 = domain, out $2 = pid dir
{
    getConfigurationValue "$1" pidDir $2 && eval $2=\"\${$2:-$DCACHE_PID}\"
}

# The stop file is the file used to suppress domain restart
getStopFile() # in $1 = domain, out $2 = stop file
{
    eval $2=\"/tmp/.dcache-stop.${1}\"
}

# The java pid file stores the PID of the java process
getJavaPidFile() # $1 = domain, out $2 = pid file
{
    local dir
    local service

    getService "$domain" service || return
    getPidDir "$1" dir || return

    if [ "$service" = "srm" ]; then
        eval $2=\"${dir}/dcache.$domain.pid\"
    else
        eval $2=\"${dir}/dcache.${domain}-java.pid\"
    fi
}

# The daemon pid file stores the PID of the daemon wrapper
getDaemonPidFile() # $1 = domain, out $2 = pid file
{
    local dir
    local service

    getService "$domain" service || return
    getPidDir "$1" dir || return

    if [ "$service" = "srm" ]; then
        eval $2=\"${dir}/dcache.$domain.pid\"
    else
        eval $2=\"${dir}/dcache.${domain}-daemon.pid\"
    fi
}

# If domain runs, provides the PID of its daemon wrapper
# script. Returns 0 if domain is running, 1 otherwise.
getPidOfDomain() # in $1 = Domain name, out $2 = pid
{
    local file
    local pidOfDomain

    getDaemonPidFile "$1" file || return
    getPidFromFile "$file" pidOfDomain || return
    isRunning "$pidOfDomain" || return
    eval $2=\"$pidOfDomain\"
}

# If domain runs, provides the PID of its Java process.  Returns 0 if
# domain is running, 1 otherwise.
getJavaPidOfDomain() # in $1 = Domain name, out $2 = pid
{
    local file
    local pidOfDomain

    getJavaPidFile "$1" file || return
    getPidFromFile "$file" pidOfDomain || return
    isRunning "$pidOfDomain" || return
    eval $2=\"$pidOfDomain\"
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

getBatchFile() # in $1 = service or domain, out $2 = batch file
{
    local filename
    local name
    name="$1"

    if ! getConfigurationValue "$name" batch filename || [ -z "$filename" ]; then
	case "${name}" in
            srm|srm-*Domain)
		filename="srm.batch"
		;;
            dcap)
		filename="door.batch"
		;;
            dcap*-*Domain)
		name=${name%%-*Domain}
		filename="door${name#dcap}.batch"
		;;
            xrootd|xrootd-*Domain)
		filename="xrootdDoor.batch"
		;;
            gridftp|gridftp-*Domain)
		filename="gridftpdoor.batch"
		;;
            gsidcap|gsidcap-*Domain)
		filename="gsidcapdoor.batch"
		;;
            admin|adminDoorDomain)
		filename="adminDoor.batch"
		;;
            gPlazma|gPlazma-*Domain)
		filename="gPlazma.batch"
		;;
            webdav|webdav-*Domain)
		filename="webdav.batch"
		;;
            *Domain)
		if contains $name $(printAllPoolDomains); then
		    filename="pool.batch"
		else
		    filename="${name%Domain}.batch"
		fi
		;;
            *)
		filename="${name}.batch"
		;;
	esac
    fi

    if [ -f "${DCACHE_CONFIG}/${filename}" ] ; then
        eval $2=\"${DCACHE_CONFIG}/${filename}\"
    else
        return 1
    fi
}

# Starts or stops a given domain.
runDomain() # in $1 = domain, in $2 = action
{
    local domain
    local action
    local service
    local program
    local door
    local poolFile

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
            case "$action" in
                start)
		    getPoolListFile "$domain" poolFile
		    if [ ! -f "${poolFile}" ] ; then
			fail 4 "Pool file not found: ${poolFile}"
		    fi

		    ( domainStart "$domain" "pool=${poolFile}" ) || return
                    ;;
                stop)
                    ( domainStop "$domain" ) || return
                    ;;
            esac
            ;;
        srm)
            "${program}" ${action} || return
            ;;
        *)
            case "$action" in
                start)
		    ( domainStart "$domain" ) || return
                    ;;
                stop)
                    ( domainStop "$domain" ) || return
                    ;;
            esac
            ;;
    esac
}

# Start domain. Use runDomain rather than calling this function
# directly.
domainStart() # $1 = domain, $2+ = domain parameters
{
    local domain
    local log
    local tmp
    local java_options
    local domain_domains
    local stopFile
    local javaPidFile
    local daemonPidFile
    local setupFile
    local batchFile
    local pid
    local java

    domain="$1"
    shift

    # Don't do anything if already running
    if getPidOfDomain "$domain" tmp; then
        fail 1 "${domain} is already running"
    fi

    # Construct Java options
    getConfigurationValue "$domain" java_options java_options || true
    java_options="${java_options} -Djava.endorsed.dirs=${DCACHE_HOME}/classes/endorsed"

    # Activate GSI key pair caching
    if ! echo "${java_options}" | grep -q "\-Dorg\.globus\.jglobus\.delegation\.cache\.lifetime="; then
        java_options="${java_options} -Dorg.globus.jglobus.delegation.cache.lifetime=30000"
    fi

    # Telnet port (what is this used for?)
    if getConfigurationValue "$domain" telnetPort tmp && [ "$tmp" ]; then
        domain_options="$domain_options -telnet ${tmp}"
    elif getConfigurationValue "$domain" telnet tmp && [ "$tmp" ]; then
        domain_options="$domain_options -telnet ${tmp}"
    fi

    # Debug switch
    if getConfigurationValue "$domain" debug tmp && [ "${tmp}" ]; then
        domain_options="$domain_options -debug"
    fi

    # Locate setup file
    if ! getConfigurationFile "$domain" setupFile; then
        fail 1 "Failed to find setup file for $domain"
    fi

    # Determine batch file
    if ! getBatchFile "$domain" batchFile; then
        fail 5 "Cannot find batch file for ${domain}"
    fi

    # Build classpath
    classpath="${DCACHE_HOME}/classes/cells.jar:${DCACHE_HOME}/classes/dcache.jar"
    if getConfigurationValue "$domain" classpath tmp && [ "$tmp" ]; then
        classpath="${tmp}:${classpath}"
    fi
    if [ -r "${DCACHE_HOME}/classes/extern.classpath" ]; then
        . "${DCACHE_HOME}/classes/extern.classpath"
        classpath="${classpath}:${externalLibsClassPath}"
    fi

    # LD_LIBRARY_PATH override
    if getConfigurationValue "$domain" libbrarypath LD_LIBRARY_PATH && [ "$LD_LIBRARY_PATH" ]; then
        export LD_LIBRARY_PATH
    fi

    # Unpriviledged user
    getConfigurationValue "$domain" user user || true

    # Find JRE
    if ! getConfigurationValue "$domain" java java || [ ! -x "$java" ]; then
	if [ -x "${DCACHE_HOME}/jre/bin/java" ]; then
            java="${DCACHE_HOME}/jre/bin/java"
	else
	    fail 1 "Could not find Java executable"
	fi
    fi

    # Prepare log file
    getLogOfDomain "$domain" log
    if getConfigurationValue "$domain" logMode tmp && [ "$tmp" = "new" ]; then
        mv -f "${log}" "${log}.old"
    fi
    touch "${log}" || fail 1 "Could not write to log file ${log}"

    # Delay between automatic restarts
    if getConfigurationValue "$domain" config tmp && [ -f "${tmp}/delay" ] ; then
        delay=$(cat "${tmp}/delay")
    else
        delay=10
    fi

    # Various control files
    getJavaPidFile "$domain" javaPidFile
    getDaemonPidFile "$domain" daemonPidFile
    getStopFile "$domain" stopFile

    # Source dcache.local.sh
    if [ -f "${DCACHE_JOBS}/dcache.local.sh" ]  ; then
        . "${DCACHE_JOBS}/dcache.local.sh"
    fi

    # Execute dcache.local.run.sh
    if [ -f "${DCACHE_JOBS}/dcache.local.run.sh" ]; then
        if ! "${DCACHE_JOBS}/dcache.local.run.sh" $action; then
            fail $? "Site local script ${DCACHE_JOBS}/dcache.local.run.sh failed: errno = $?"
        fi
    fi

    # Start daemon
    rm -f "$stopFile"
    cd $DCACHE_HOME
    CLASSPATH="$classpath" /bin/sh ${DCACHE_HOME}/share/lib/daemon ${user:+-u} ${user:+"$user"} -r "$stopFile" -d "$delay" -f -c "$javaPidFile" -p "$daemonPidFile" -o "$log" "$java" ${java_options} dmg.cells.services.Domain "${domain}" ${domain_options} -batch "$batchFile" -param "ourHomeDir=${DCACHE_HOME}" "setupFile=${setupFile}" "$@" 

    # Wait for confirmation
    printf "Starting ${domain} "
    for c in 6 5 4 3 2 1 0; do
        if getPidFromFile "$javaPidFile" pid && isRunning "$pid"; then
            echo "done"
            return
        fi
        sleep 1
        printf "$c "
    done

    echo "failed"
    grep PANIC "${log}"
    exit 4
}

# Stop domain. Use runDomain rather than calling this function
# directly.
domainStop() # $1 = domain
{
    local domain
    local javaPid
    local daemonPid
    local stopFile

    domain="$1"

    if ! getPidOfDomain "$domain" daemonPid; then
        return 0
    fi

    # Fail if we don't have permission to signal the daemon
    if ! kill -0 $daemonPid; then
	fail 1 "Failed to kill ${domain}"
    fi

    # Stopping a dCache domain for good requires that we supress the
    # automatic restart by creating a stop file.
    getStopFile "$domain" stopFile
    touch "$stopFile" 2>/dev/null

    if getJavaPidOfDomain "$domain" javaPid; then
	kill -TERM $javaPid 1>/dev/null 2>/dev/null || true
    fi

    printf "Stopping ${domain} (pid=$daemonPid) "
    for c in  0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
        if ! isRunning $daemonPid; then
            rm -f "$daemonPidFile" "$javaPidFile"
            echo "done"
            return
        fi
        printf "$c "
        sleep 1
        if [ $c -eq 9 ] ; then
	    if getJavaPidOfDomain "$domain" javaPid; then
		kill -9 $javaPid 1>/dev/null 2>/dev/null || true
	    fi
        fi
    done
    echo
    fail 4 "Giving up. ${domain} might still be running."
}
