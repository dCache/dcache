# Useful functions for querying the dCache configuration files.
#
# Relies on the utility functions in utils.sh, which must be loaded
# prior to calling any of the following functions.

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

