# Useful functions for working with pools
#
# Relies on the functions in utils.sh, config.sh and
# services.sh. These must be loaded prior to calling any of the
# following functions.

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

    mkdir -p "${path}" "${path}/data" "${path}/control" ||
    fail 1 "Failed to create directory tree"

    set_size="s:set max diskspace 100g:set max diskspace ${size}g:g"
    sed -e "$set_size" ${DCACHE_CONFIG}/setup.temp > ${path}/setup || exit 1

    printp "Created a $size GiB pool in $path. The pool cannot be used
            until it has been added to a domain. Use 'pool add' to do so."\
           "Please note that this script does not set the owner of the
            pool directory. You may need to adjust it."
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
