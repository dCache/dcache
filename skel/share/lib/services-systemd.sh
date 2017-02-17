# Useful functions for working with domains using systemd

printProperty() # $1 = domain, $2 = property
{
    local out
    out=$(systemctl show dcache@$1 -p $2)
    printf "${out#*=}"
}

# Print "stopped", "running" or "restarting" depending on the status
# of the domain.
#
# Return code is according to that of System V init scripts.
printSimpleDomainStatus() # $1 = domain
{
    case $(printProperty $1 ActiveState) in
    activating)
        if [ "$(printProperty $1 SubState)" = "auto-restart" ]; then
            printf restarting
            return 4
        else
            printf running
            return 0
        fi
        ;;
    active)
        printf running
        return 0
        ;;
    deactivating)
        printf running
        return 0
        ;;
    *)
        printf stopped
        return 0
        ;;
    esac
}

# Print a more detailed description of the domains status.  This
# may include the duration the domain has been in that state.
printDetailedDomainStatus() # $1 = domain
{
    local rc state

    rc=0
    state="$(printSimpleDomainStatus "$1")" || rc=$?

    if [ "$state" != "stopped" ]; then
       printf "$state ($(age $(date --date="$(printProperty $1 StateChangeTimestamp)" +%s)))"
    else
       printf "$state"
    fi

    return $rc
}

# Prints the PID of the Java process
printJavaPid() # $1 = domain
{
    local pid
    pid=$(printProperty $1 MainPID)
    [ "$pid" -eq 0 ] || printf "%d" $pid
}

# Start domain. Use runDomain rather than calling this function
# directly.
domainStart() # $1 = domain
{
    printf "Starting ${domain} "

    systemctl --no-ask-password start dcache@$1

    if [ "$(printSimpleDomainStatus "$domain")" != "stopped" ]; then
        printf "done\n"
    else
        printf "failed\n"
        journalctl --unit=dcache@$1 --since=-2min | grep PANIC
        exit 4
    fi
}

# Stop domain. Use runDomain rather than calling this function
# directly.
domainStop() # $1 = domain
{
    if [ "$(printSimpleDomainStatus "$domain")" = "stopped" ]; then
        return 0
    fi

    printf "Stopping ${domain} "

    systemctl --no-ask-password stop dcache@$1 &

    for c in  0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25; do
        if [ "$(printSimpleDomainStatus "$domain")" = "stopped" ]; then
            printf "done\n"
            return
        fi
        printf "$c "
        sleep 1
    done
    printf "\nGiving up. ${domain} might still be running.\n" 1>&2
    return 4
}

# Check prerequisites
require systemctl
