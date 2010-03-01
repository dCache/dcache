# Useful functions for querying the dCache configuration files.
#
# Relies on the functions in utils.sh, config.sh and services.sh,
# which must be loaded prior to calling any of the following
# functions.

# Returns 0 if any of the domains require a PNFS mount, returns 1
# otherwise.
isNameSpaceMountNeeded() # $* = domains
{
    for domain in $*; do
        getService $domain
        case "$RET" in
            pnfs)
                return 0
                ;;
            *)
                ;;
        esac
    done
    return 1;
}

# Mount name server export $2 on local directory $1
mountNameSpace() # $1 = mount point, $2 = server export, $3 = nfs protocol version
{
    local mountpoint
    local export
    local server
    local version
    local nfsversion

    mountpoint="$1"
    export="$2"
    nfsversion=$3

    # Check if already mounted
    if [ -f "${mountpoint}/.(tags)()" ]; then
        return;
    fi

    # Check that mount point exists
    if [ ! -d "${mountpoint}" ]; then
        fail 1 "Mount point $mountpoint does not exist.
                Please rerun install.sh."
    fi

    # Determine the name space server; defaults to localhost
    getNameSpaceServer; server=${RET:-localhost}

    # Solaris specific fix
    case $(uname) in
        SunOS)
            version="vers"
            ;;
        *)
            version="nfsvers"
            ;;
    esac

    # Mount name space
    printp "Mounting ${mountpoint}"
    if ! mount -o intr,rw,noac,hard,${version}=${nfsversion} ${server}:${export} ${mountpoint}; then
        fail 1 "Failed to mount name space. The following command failed:" \
               "mount -o intr,rw,noac,hard,${version}=${nfsversion} ${server}:${export}
                ${mountpoint}"
    fi

    if [ ! -f "${mountpoint}/.(tags)()" ]; then
	fail 1 "Failed to mount name space."
    fi
}

# Mount name space if not already mounted. Where and how the name
# space is mounted depends on the configured service and the name
# space.
autoMountNameSpace()
{
    local root
    local namespace
    local serverId

    # Where should we mount it and what kind of name space do we use?
    root="${NODE_CONFIG_PNFS_ROOT:-$DCACHE_PNFS_ROOT}"
    namespace="${NODE_CONFIG_NAMESPACE:-pnfs}"

    case "$namespace" in
        chimera)
            mountNameSpace "${root}" "/pnfs" 3
            ;;
        pnfs)
            if contains pnfsDomain $(printAllDomains); then
                loadConfigurationFile pnfs
                if [ -z "$pnfs_pnfs" ]; then
                    mountNameSpace "${root}/fs" "/fs" 2
                else
                    mountNameSpace "${pnfs_pnfs}" "/fs" 2
                fi
            else
                getServerId; serverId="$RET"
                mountNameSpace "${root}/${serverId}" "/pnfsdoors" 2
            fi
            ;;
    esac
}
