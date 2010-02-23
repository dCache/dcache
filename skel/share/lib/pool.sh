# Useful functions for working with pools
#
# Relies on the functions in utils.sh, config.sh and
# services.sh. These must be loaded prior to calling any of the
# following functions.


# Prints the list of pools in the given pool domains.
printAllPools() # in $* = list of domains
{
    for domain in "$@"; do
        printPoolsInDomain $domain | while read pool path param; do
            printf "%s " $pool
        done
    done
}

# Writes the poollist file of the given pool domain to stdout. The
# format is 'name directory parameters'. Aborts if the file does not
# exist.
printPoolsInDomain() # in $1 = Pool domain
{
    local poolFile

    getPoolListFile $1 poolFile
    if [ ! -f ${poolFile} ]; then
        printp "Pool file not found: ${poolFile}" 1>&2
        exit 4
    fi
    cat $poolFile
}

# If pool exists in one of the given domains, then the domain
# containing the pool is provided and the return code is 0. If the
# pool is not found, the return code is 1.
getDomainOfPool() # out $1 = domain, in $2 = pool, in $3+ = list of domains
{
    local pool
    local out
    local ret

    out=$1
    pool=$2
    shift 2

    for ret in "$@"; do
        if contains "$pool" $(printPoolsInDomain "$ret"); then
            eval $out=\"$ret\"
            return 0
        fi
    done

    return 1
}

getPoolPath() # in $1 = pool, in $2 = domain, out $3 = path
{
    local pool
    local domain
    local poolFile
    local ret

    pool="$1"
    domain="$2"

    getPoolListFile "$domain" poolFile
    ret=$(sed -n -e "s/^${pool} *\([^ ]*\)*.*$/\1/p" < "${poolFile}")

    eval $3=\"$ret\"

    [ -n "${ret}" ]
}

getPoolSetting() # in $1 = pool path, in $2 = key, out $3 = value
{
    local path
    local key

    path=$1
    key=$2

    if [ ! -f "${path}/setup" ]; then
        printp "Setup file not found in $1" 1>&2
        exit 4
    fi

    #                    Comments      Trailing space  Print value
    #                    vvvvvvvv      vvvvvvv         vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    eval $3=$(sed -n -e 's/#.*$//' -e 's/[  ]*$//' -e "s/^[         ]*${key}[       ]*\(.*\)/\1/p" "${path}/setup")
}

# Extracts the size of a pool in GiB.
getSizeOfPool() # in $1 = pool path, out $2 = size
{
    local diskspace
    getPoolSetting "$1" "set max diskspace" diskspace
    stringToGiB "$diskspace" $2
}

createPool() # in $1 = size, in $2 = path
{
    local size
    local path
    local ds
    local movers
    local set_size
    local set_movers
    local parent

    stringToGiB "$1" size
    path="$2"

    # Path must not exist
    if [ -e "${path}" ]; then
        fail 1 "${path} already exists. Operation aborted."
    fi

    # Make sure the parent path exists
    parent=$(dirname "${path}")
    if [ ! -d "${parent}" ]; then
        mkdir -p "${parent}" || fail 1 "Failed to create $parent"
    fi

    # We need to have enough free space
    getFreeSpace "${parent}" ds

    if [ "${ds}" -lt "${size}" ]; then
        fail 1 "Pool size exceeds available space. ${path} only
                has ${ds} GiB of free space. Operation aborted."
    fi

    # Why does node_config contain default values for new pools? That
    # will for sure confuse our users, since they are made to believe
    # they can adjust the number of movers by changing node_config;
    # which is not the case!
    movers="$NODE_CONFIG_NUMBER_OF_MOVERS"

    mkdir -p "${path}" "${path}/data" "${path}/control" ||
    fail 1 "Failed to create directory tree"

    set_size="s:set max diskspace 100g:set max diskspace ${size}g:g"
    set_movers="s:mover set max active 10:mover set max active ${movers}:g"
    sed -e "$set_size" -e "$set_movers" ${pool_config}/setup.temp > ${path}/setup || exit 1

    printp "Created a $size GiB pool in $path. The pool cannot be used
            until it has been added to a domain. Use 'pool add' to do so."\
           "Please note that this script does not set the owner of the
            pool directory. You may need to adjust it."
}

addPool() # in $1 = pool, in $2 = path, in $3 = domain, in $4 = use fqdn, in $5 = lfs
{
    local pool
    local path
    local domain
    local use_fqdn
    local lfs
    local file
    local param

    pool="$1"
    path="$2"
    domain="$3"
    use_fqdn="$4"
    lfs="$5"

    sanitisePath "$path" path

    # Check that LFS mode is valid
    if ! contains "$lfs" none precious hsm volatile transient; then
        fail 2 "Invalid LFS mode."
    fi

    # Check that path is absolute
    if [ "${path#/}" = "${path}" ]; then
        fail 2 "$path does not appear to be an absolute path. In order
                to add the pool, the absolute path is needed."
    fi

    # Check that pool exists
    if [ ! -f "${path}/setup" ]; then
        fail 1 "No pool found in ${path}. Operation aborted."
    fi

    # Check that pool is not already a member of a domain
    for d in $(printAllPoolDomains); do
        printPoolsInDomain $d | while read _pool _path _param; do
            if [ "$path" = "$(sanitisePath $_path)" ]; then
                fail 1 "Pool ${path} already present in ${d}.
                        Operation aborted."
            fi

            if [ "$pool" = "$_pool" ]; then
                fail 1 "Pool name already used in ${d}. Operation
                        aborted."
            fi
        done
        if [ $? -eq 1 ]; then
            exit 1
        fi
    done

    # A pool domain must have an ending of 'Domain'
    if [ "${domain%Domain}Domain" != "${domain}" ]; then
        fail 2 "${domain} is not a valid pool domain name, because
                it does not end with 'Domain'. You may try to use
                ${domain}Domain."
    fi

    # It must not be used for any other service
    if ! contains "$domain" $(printAllPoolDomains) ; then
        if contains "$domain" $(printAllDomains); then
            fail 1 "${domain} is not a valid pool domain name, because
                    the domain is already used for other purposes."
        fi
    fi

    # Create the domain if it doesn't already exist
    file="${pool_config}/${hostname}.domains"
    if [ ! -f "$file" ]; then
        echo "${domain%Domain}" >> "$file" || exit 1
    elif ! grep "${domain%Domain}" "$file" >/dev/null; then
        echo "${domain%Domain}" >> "$file" || exit 1
    fi

    # Determine pool parameters
    param="sticky=allowed recover-space recover-control recover-anyway"
    if [ "$lfs" != "none" ]; then
        param="$param lfs=$lfs"
    fi
    if [ "$use_fqdn" -eq 1 ]; then
        param="$param tag.hostname=$fqdn"
    else
        param="$param tag.hostname=$hostname"
    fi

    # Add pool to domain
    getPoolListFile "$domain" file
    echo "$pool  $path  $param" >> "$file" || exit 1

    # Tell the user what we did
    printp "Added pool ${pool} in ${path} to ${domain}."\
           "The pool will not be operational until the domain has
            been started. Use 'start ${domain}' to start
            the pool domain."
}

removePool() # in $1 = pool name
{
    local pool
    local pl
    local dl
    local pid

    pool="$1"

    # Find the domain containing pool and remove the pool from it
    for domain in $(printAllPoolDomains); do
        printPoolsInDomain $domain | while read _pool _path _param; do
            if [ "$pool" = "$_pool" ]; then
                # Check if domain is still running
                if getPidOfDomain "${domain}" pid; then
                    fail 1 "A pool named $pool was found in the domain
                            $domain. $domain is still running. Shut it
                            down before removing the pool from the
                            configuration. Operation aborted."
                fi

                # Remove pool from pool list file
                getPoolListFile "$domain" pl
                sed -e "/^${pool}.*/ d" "${pl}" > "${pl}.$$" || exit
                mv -f "${pl}.$$" "${pl}" || exit

                # Disable domain if empty
                if isFileEmpty "$pl"; then
                    rm "$pl" || exit
                    dl="${pool_config}/${hostname}.domains"
                    sed -e "/^${domain%Domain}\$/ d" "${dl}" > "${dl}.$$" || exit
                    mv -f "${dl}.$$" "${dl}" || exit
                    printp "Removed pool ${pool} from ${domain}. ${domain}
                            is now empty and has been removed."
                else
                    printp "Removed pool ${pool} from ${domain}."
                fi

                # Since this runs in a subshell, we return with a a
                # non-zero return code to signal that removePool
                # should return too.
                exit 1
            fi
        done
        [ $? -eq 0 ] || return
    done

    fail 1 "No pool named ${pool} could be found. Operation aborted."
}

# Reconstruct the meta data Berkeley DB of a pool
reconstructMeta() # in $1 = src meta dir, in $2 = dst meta dir
{
    local src
    local dst
    local dump
    local load
    local databases

    src="$1"
    dst="$2"
    dump="${java} -cp ${DCACHE_JE} com.sleepycat.je.util.DbDump"
    load="${java} -cp ${DCACHE_JE} com.sleepycat.je.util.DbLoad"
    databases="java_class_catalog state_store storage_info_store"

    for db in ${databases}; do
        ${dump} -h "$src" -r -d "$dst" -v -s $db || return
    done

    for db in ${databases}; do
        ${load} -f "${dst}/${db}.dump" -h "$dst" -s $db || return
        rm "${dst}/${db}.dump"
    done
}
