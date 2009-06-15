# Useful functions for querying the dCache configuration files.
#
# Relies on the utility functions in utils.sh, which must be loaded
# prior to calling any of the following functions.

# Sets RET to the name space server. Blank if name space server is
# not configured.
getNameSpaceServer()
{
    RET="$NODE_CONFIG_NAMESPACE_NODE"
    if [ -z "${RET}" ] ; then
        RET="$NODE_CONFIG_ADMIN_NODE"
        if [ ! -z "${RET}" ] ; then
            printp "[WARNING] ADMIN_NODE is deprecated. Please use
                    NAMESPACE_NODE instead." 1>&2
        fi
    fi
}

# Stores the server ID in RET. The server ID is taken from
# node_config. If not defined, the server ID is the domain name taken
# from /etc/resolv.conf.
getServerId()
{
    RET="$NODE_CONFIG_SERVER_ID"
    if [ -z "${RET}" ]; then
        RET=$domainname
        if [ -z "${RET}" ]; then
            RET=$(sed -e 's/#.*$//' /etc/resolv.conf | awk '/^[ \t]*search/ { print $2 }')
            if [ -z "${RET}" ]; then
                RET=$(sed -e 's/#.*$//' /etc/resolv.conf | awk '/^[ \t]*domain/ { print $2 }')
            fi
        fi
    fi
}

# Returns 0 if option is set to 'yes' or 'y' in node_config or
# door_config, 1 otherwise.
isNodeConfigEnabled() # $1 = option name
{
    local value
    eval value="\$NODE_CONFIG_$1"
    case "$(echo $value | tr '[A-Z]' '[a-z]')" in
        yes|y)
            return 0;
            ;;
        *)
            return 1;
            ;;
    esac
}
