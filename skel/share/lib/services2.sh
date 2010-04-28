# Useful functions for working with domains.

# Invokes the dCache boot loader
bootLoader()
{
    local quietFlag
    shouldBootLoaderBeQuiet && quietFlag="-q"
    $JAVA -client -cp "$DCACHE_HOME/classes/dcache.jar:$DCACHE_HOME/classes/cells.jar:$DCACHE_HOME/classes/log4j/*:$DCACHE_HOME/classes/slf4j/*" "-Ddcache.home=$DCACHE_HOME" org.dcache.boot.BootLoader -f="$DCACHE_SETUP" $quietFlag "$@"
}

shouldBootLoaderBeQuiet()
{
    [ -n "$BOOTLOADER_IS_QUIET" ]
}

setBootLoaderQuiet()
{
    BOOTLOADER_IS_QUIET=1
}

setBootLoaderNoisy()
{
    unset BOOTLOADER_IS_QUIET
}

# Prints all domains that match a given pattern
printDomains() # $1+ = patterns
{
    bootLoader list "$@" || fail 1 "Failed to invoke dCache"
}

# Returns the PID stored in pidfile. Fails if the file does not exist,
# is empty or doesn't contain a valid PID.
printPidFromFile() # $1 = file
{
    local pid

    [ -f "$1" ] || return
    pid=$(cat "$1") || return
    [ -n "$pid" ] || return
    isRunning "$pid" || return

    printf "%s" "$pid"
}

# Reads the given configuration parameters into environment
# variables. The environment variables used are upper case versions of
# the keys.
loadConfig() # $1+ = keys
{
    eval $(bootLoader config -shell ${DOMAIN:+"-domain=$DOMAIN"} "$@")
}

# Print "stopped", "running" or "restarting" depending on the status
# of the domain.
printDomainStatus()
{
    local pid

    if ! pid=$(printPidFromFile "$DCACHE_PID_DAEMON"); then
        printf "stopped"
    elif ! pid=$(printPidFromFile "$DCACHE_PID_JAVA"); then
        printf "restarting"
    else
        printf "running"
    fi
}

# Prints the PID of the daemon wrapper script
printDaemonPid()
{
    printPidFromFile "$DCACHE_PID_DAEMON"
}

# Prints the PID of the Java process
printJavaPid()
{
    printPidFromFile "$DCACHE_PID_JAVA"
}

# Start domain. Use runDomain rather than calling this function
# directly.
domainStart()
{
    # Don't do anything if already running
    if [ "$(printDomainStatus)" != "stopped" ]; then
        fail 1 "${DOMAIN} is already running"
    fi

    # Build classpath
    classpath="${DCACHE_HOME}/classes/cells.jar:${DCACHE_HOME}/classes/dcache.jar"
    if [ "$DCACHE_JAVA_CLASSPATH" ]; then
        classpath="${classpath}:${DCACHE_JAVA_CLASSPATH}"
    fi
    if [ -r "${DCACHE_HOME}/classes/extern.classpath" ]; then
        . "${DCACHE_HOME}/classes/extern.classpath"
        classpath="${classpath}:${externalLibsClassPath}"
    fi

    # LD_LIBRARY_PATH override
    if [ "$DCACHE_JAVA_LIBRARY_PATH" ]; then
	LD_LIBRARY_PATH="$DCACHE_JAVA_LIBRARY_PATH"
        export LD_LIBRARY_PATH
    fi

    # Prepare log file
    if [ "$DCACHE_LOG_MODE" = "new" ]; then
        mv -f "${DCACHE_LOG_FILE}" "${DCACHE_LOG_FILE}.old"
    fi
    touch "${DCACHE_LOG_FILE}" || fail 1 "Could not write to ${DCACHE_LOG_FILE}"

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

    if [ "${DCACHE_TERRACOTTA_ENABLED:-false}" = "true" ] ; then
        local terracotta_dso_env_sh="${DCACHE_TERRACOTTA_INSTALL_DIR}/bin/dso-env.sh"
        if [ ! -f "${terracotta_dso_env_sh}" ] ; then
            fail 1 "Terracotta is enabled, but ${terracotta_dso_env_sh} was not found"
        fi
        if [ ! -f "${DCACHE_TERRACOTTA_CONFIG_PATH}" ] ; then
            fail 1 "Terracotta is enabled, but terracotta config file ${DCACHE_TERRACOTTA_CONFIG_PATH} was not found"
        fi
        # TC_INSTALL_DIR and TC_CONFIG_PATH need to be defined for a successful execution of dso-env.sh
        export TC_INSTALL_DIR=${DCACHE_TERRACOTTA_INSTALL_DIR}
        export TC_CONFIG_PATH=${DCACHE_TERRACOTTA_CONFIG_PATH}
        # Result of this script execution is the definition of the options needed for startup of the jvm
        # defined in ${TC_JAVA_OPTS}
        . ${terracotta_dso_env_sh}
    fi

    # Start daemon
    rm -f "$DCACHE_RESTART_FILE"
    cd "$DCACHE_HOME"
    CLASSPATH="$classpath" /bin/sh "${DCACHE_HOME}/share/lib/daemon" ${DCACHE_USER:+-u} ${DCACHE_USER:+"$DCACHE_USER"} -l -r "$DCACHE_RESTART_FILE" -d "$DCACHE_RESTART_DELAY" -f -c "$DCACHE_PID_JAVA" -p "$DCACHE_PID_DAEMON" -o "$DCACHE_LOG_FILE" "$JAVA" ${DCACHE_JAVA_OPTIONS} ${TC_JAVA_OPTS} "-Ddcache.home=$DCACHE_HOME" org.dcache.boot.BootLoader -f="$DCACHE_SETUP" start "$DOMAIN"

    # Wait for confirmation
    printf "Starting ${DOMAIN} "
    for c in 6 5 4 3 2 1 0; do
	if [ "$(printDomainStatus)" != "stopped" ]; then
            echo "done"
            return
        fi
        sleep 1
        printf "$c "
    done

    echo "failed"
    grep PANIC "$DCACHE_LOG_FILE"
    exit 4
}

# Stop domain. Use runDomain rather than calling this function
# directly.
domainStop() # $1 = domain
{
    local daemonPid
    local javaPid

    if [ "$(printDomainStatus)" = "stopped" ]; then
        return 0
    fi

    daemonPid=$(printDaemonPid) || return

    # Fail if we don't have permission to signal the daemon
    if ! kill -0 $daemonPid; then
	fail 1 "Failed to kill ${DOMAN}"
    fi

    # Stopping a dCache domain for good requires that we supress the
    # automatic restart by creating a stop file.
    touch "$DCACHE_RESTART_FILE" || return

    if javaPid=$(printJavaPid); then
	kill -TERM "$javaPid" 1>/dev/null 2>/dev/null || :
    fi

    printf "Stopping ${DOMAIN} (pid=$daemonPid) "
    for c in  0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
        if ! isRunning $daemonPid; then
            rm -f "$DCACHE_PID_DAEMON" "$DCACHE_PID_JAVA"
            echo "done"
            return
        fi
        printf "$c "
        sleep 1
        if [ $c -eq 9 ] ; then
	    if javaPid=$(printJavaPid); then
		kill -9 $javaPid 1>/dev/null 2>/dev/null || true
	    fi
        fi
    done
    echo
    fail 4 "Giving up. ${DOMAIN} might still be running."
}
