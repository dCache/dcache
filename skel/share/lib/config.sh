# Useful functions for querying the dCache configuration files.
#
# Relies on the utility functions in utils.sh, which must be loaded
# prior to calling any of the following functions.

# Returns the name of the name space server. Defaults to localhost if
# name space server is not configured.
getNameSpaceServer() # out $1 = name space server
{
    local ret
    ret="$NODE_CONFIG_NAMESPACE_NODE"
    if [ -z "${ret}" ] ; then
        ret="$NODE_CONFIG_ADMIN_NODE"
        if [ -z "${ret}" ] ; then
            ret="localhost"
        else
            printp "[WARNING] ADMIN_NODE is deprecated. Please use
                    NAMESPACE_NODE instead." 1>&2
        fi
    fi
    eval $1=\"$ret\"
}

# Return the server ID. The server ID is taken from node_config. If
# not defined, the server ID is the domain name taken from
# /etc/resolv.conf.
getServerId() # out $1 = server id
{
    local ret
    ret="$NODE_CONFIG_SERVER_ID"
    if [ -z "${ret}" ]; then
        ret=$domainname
        if [ -z "${ret}" ]; then
            ret=$(sed -e 's/#.*$//' /etc/resolv.conf | awk '/^[ \t]*search/ { print $2 }')
            if [ -z "${ret}" ]; then
                ret=$(sed -e 's/#.*$//' /etc/resolv.conf | awk '/^[ \t]*domain/ { print $2 }')
                if [ -z "${ret}" ]; then
                    return 1
                fi
            fi
        fi
    fi
    eval $1=\"$ret\"
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
