# Useful functions for working with domains.

# Prints all domains that match a given pattern. Prints all domains if
# no patterns are provided.
printDomains() # $1+ = patterns
{
    local domain
    local domains
    domains=$(getProperty dcache.domains)
    if [ $# -eq 0 ]; then
        echo $domains
    else
        for domain in $domains; do
            if matchesAny "$domain" "$@"; then
                echo $domain
            fi
        done
    fi
}

# Returns the PID stored in pidfile. Fails if the file does not exist,
# is empty or doesn't contain a valid PID.
#
# Return code is according to that of System V init scripts.
printPidFromFile() # $1 = file
{
    local pid

    [ -f "$1" ] || return 3      # program is not running
    pid=$(cat "$1") || return 4  # program or service staus is unknown
    [ -n "$pid" ] || return 4    # program or service staus is unknown
    isRunning "$pid" || return 1 # program is dead and pid file exists

    printf "%s" "$pid"
}

# Print "stopped", "running" or "restarting" depending on the status
# of the domain.
#
# Return code is according to that of System V init scripts.
printDomainStatus() # $1 = domain
{
    local rc

    if printJavaPid "$1" > /dev/null; then
        if printDaemonPid "$1" > /dev/null; then
            printf "running"
        else
            printf "orphaned"
        fi
        return 0
    else
        if printDaemonPid "$1" > /dev/null; then
            printf "restarting"
            return 4                 # program or service status is unknown
        else
            rc=$?
            printf "stopped"
            return $rc
        fi
    fi
}

# Prints the PID of the daemon wrapper script
printDaemonPid() # $1 = domain
{
    printPidFromFile "$(getProperty dcache.pid.daemon "$1")"
}

# Prints the PID of the Java process
printJavaPid() # $1 = domain
{
    printPidFromFile "$(getProperty dcache.pid.java "$1")"
}

# Start domain. Use runDomain rather than calling this function
# directly.
domainStart() # $1 = domain
{
    local domain
    local classpath
    local JAVA_LIBRARY_PATH
    local LOG_FILE
    local RESTART_FILE
    local RESTART_DELAY
    local USER
    local PID_JAVA
    local PID_DAEMON
    local JAVA_OPTIONS
    local plugins
    local jar
    local plugin

    domain="$1"

    # Don't do anything if already running
    if [ "$(printDomainStatus "$domain")" != "stopped" ]; then
        echo "${domain} is already running" 1>&2
        return 1
    fi

    # Build classpath
    classpath="$(getProperty dcache.paths.classpath "$domain")"
    plugins="$(getProperty dcache.paths.plugins "$domain")"
    while [ -n "$plugins" ]; do
        # plugins is a colon separated path of directories
        case "$plugins" in
            *:*)
                plugin="${plugins%%:*}"
                plugins="${plugins#*:}"
                ;;
            *)
                plugin="$plugins"
                plugins=
                ;;
        esac
        if [ -d "$plugin" ]; then
            for jar in "$plugin"/*/*.jar; do
                classpath="${jar}:${classpath}"
            done
        fi
    done

    # LD_LIBRARY_PATH override
    JAVA_LIBRARY_PATH="$(getProperty dcache.java.library.path "$domain")"
    if [ "$JAVA_LIBRARY_PATH" ]; then
	LD_LIBRARY_PATH="$JAVA_LIBRARY_PATH"
        export LD_LIBRARY_PATH
    fi

    # Prepare log file
    LOG_FILE="$(getProperty dcache.log.file "$domain")"
    if [ "$(getProperty dcache.log.mode "$domain")" = "new" ]; then
        mv -f "${LOG_FILE}" "${LOG_FILE}.old"
    fi
    touch "${LOG_FILE}" || fail 1 "Could not write to ${LOG_FILE}"

    # Source dcache.local.sh
    if [ -f "${DCACHE_BIN}/dcache.local.sh" ]  ; then
        . "${DCACHE_BIN}/dcache.local.sh"
    fi

    # Execute dcache.local.run.sh
    if [ -f "${DCACHE_BIN}/dcache.local.run.sh" ]; then
        if ! "${DCACHE_BIN}/dcache.local.run.sh" $action; then
            fail $? "Site local script ${DCACHE_BIN}/dcache.local.run.sh failed: errno = $?"
        fi
    fi

    if [ "$(getProperty dcache.terracotta.enabled "$domain")" = "true" ] ; then
        # TC_INSTALL_DIR and TC_CONFIG_PATH need to be defined for a
        # successful execution of dso-env.sh
        TC_INSTALL_DIR="$(getProperty dcache.terracotta.install.dir "$domain")"
        TC_CONFIG_PATH="$(getProperty dcache.terracotta.config.path "$domain")"
        export TC_INSTALL_DIR
        export TC_CONFIG_PATH

        # Result of this script execution is the definition of the
        # options needed for startup of the defined in ${TC_JAVA_OPTS}
        if [ -f "${TC_INSTALL_DIR}/bin/dso-env.sh" ] ; then
            . "${TC_INSTALL_DIR}/bin/dso-env.sh"
        elif [ -f "${TC_INSTALL_DIR}/platform/bin/dso-env.sh" ] ; then
           . "${TC_INSTALL_DIR}/platform/bin/dso-env.sh"
        else
            fail 1 "Terracotta is enabled, but dso-env.sh can't be found"
        fi
    fi

    # Start daemon
    RESTART_FILE="$(getProperty dcache.restart.file "$domain")"
    RESTART_DELAY="$(getProperty dcache.restart.delay "$domain")"
    USER="$(getProperty dcache.user "$domain")"
    PID_JAVA="$(getProperty dcache.pid.java "$domain")"
    PID_DAEMON="$(getProperty dcache.pid.daemon "$domain")"
    JAVA_OPTIONS="$(getProperty dcache.java.options "$domain")"

    rm -f "$RESTART_FILE"
    cd "$DCACHE_HOME"
    CLASSPATH="$classpath" /bin/sh "${DCACHE_LIB}/daemon" ${USER:+-u} ${USER:+"$USER"} -l -r "$RESTART_FILE" -d "$RESTART_DELAY" -f -c "$PID_JAVA" -p "$PID_DAEMON" -o "$LOG_FILE" "$JAVA" ${JAVA_OPTIONS} ${TC_JAVA_OPTS} "-Ddcache.home=$DCACHE_HOME" "-Ddcache.paths.defaults=$DCACHE_PATHS_DEFAULTS" org.dcache.boot.BootLoader  start "$domain"

    # Wait for confirmation
    printf "Starting ${domain} "
    for c in 6 5 4 3 2 1 0; do
	if [ "$(printDomainStatus "$domain")" != "stopped" ]; then
            echo "done"
            return
        fi
        sleep 1
        printf "$c "
    done

    echo "failed"
    grep PANIC "$LOG_FILE"
    exit 4
}

# Stop domain. Use runDomain rather than calling this function
# directly.
domainStop() # $1 = domain
{
    local domain
    local daemonPid
    local javaPid
    local RESTART_FILE
    local PID_JAVA
    local PID_DAEMON

    domain="$1"

    if [ "$(printDomainStatus "$domain")" = "stopped" ]; then
        return 0
    fi

    if daemonPid=$(printDaemonPid "$domain"); then
        # Fail if we don't have permission to signal the daemon
        if ! kill -0 $daemonPid; then
	    fail 1 "Failed to signal process ${daemonPid}"
        fi
    fi

    # Stopping a dCache domain for good requires that we supress the
    # automatic restart by creating a stop file.
    RESTART_FILE="$(getProperty dcache.restart.file "$domain")"
    touch "$RESTART_FILE" || return

    if javaPid=$(printJavaPid "$domain"); then
	if ! kill -TERM "$javaPid"; then
	    fail 1 "Failed to signal process ${javaPid}"
        fi
    fi

    printf "Stopping ${domain} "
    for c in  0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
        if [ "$(printDomainStatus "$domain")" = "stopped" ]; then
            PID_JAVA="$(getProperty dcache.pid.java "$domain")"
            PID_DAEMON="$(getProperty dcache.pid.daemon "$domain")"
            rm -f "$PID_DAEMON" "$PID_JAVA" "$RESTART_FILE"
            echo "done"
            return
        fi
        printf "$c "
        sleep 1
        if [ $c -eq 9 ] ; then
            if javaPid=$(printJavaPid "$domain"); then
                kill -9 $javaPid 1>/dev/null 2>/dev/null || true
                printf "[K] "
            fi
        fi
    done
    echo 1>&2
    echo "Giving up. ${domain} might still be running." 1>&2
    return 4
}

# Check prerequisites
require cat mv rm
