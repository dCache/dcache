# Useful utility functions for shell programming

# Returns true if $1 is contained as a word in $2.
contains() # $1 = word, $2+ = list
{
    local word
    word=$1
    shift
    for i in "$@"; do
	if [ "$word" = "$i" ]; then
	    return 0
	fi
    done
    return 1
}

# Reverses a list of words
reverse() # $1 = space delimited list of words
{
    RET=''
    for s in $*; do
        RET="${s} ${RET}"
    done
}

# Normalises a path such that it does not contain double or trailing
# slashes.
sanitisePath() # $1 = path
{
    RET=$(echo $1 | sed -e 's_//*_/_g' -e 's_/$__')
}

# Returns the maximum width of any word in a given list.
maxWidth() # $* = list of words
{
    local max
    local width

    max=0
    for i in $*; do
        width=${#i}
        if [ $max -lt $width ]; then
            max=$width
        fi
    done

    RET=$max
}

# Utility function for printing to stdout with a line width
# maximum of 75 characters. Longer lines are broken into several
# lines. Each argument is interpreted as a separate paragraph.
printp() # $* = list of paragraphs
{
    local line
    local line2

    while [ $# -gt 0 ]; do
	# If line is non empty, then we need to print a
	# paragraph separator.
	if [ -n "$line" ]; then
	    echo
	fi
	line=
	for word in $1; do
	    line2="$line $word"
	    if [ ${#line2} -gt 75 ]; then
		echo $line
		line=$word
	    else
		line=$line2
	    fi
	done
	echo $line
	shift
    done
}

# Prints an error message to stderr and exist with status $1
fail() # $1 = exit status, $2- = list of paragraphs, see printp
{
    local n
    n=$1
    shift
    printp "$@"
    exit $n
} 1>&2

# Returns 0 if the given file is empty, 1 otherwise. The file must
# exist.
isFileEmpty() # $1 = file
{
    if [ $(wc -l < $1) -eq 0 ]; then
        return 0;
    else
        return 1;
    fi
}

# Returns whether a process with a given PID is running
isRunning()# $1 = pid
{
    rc=0
    ps -p "$1" 1>/dev/null 2>/dev/null || rc=1
    return $rc
}


# Searches for executables and exists with an error message if any of
# them are not found on the PATH.
require() # $1 = executable
{
    local tool
    for tool in $*; do
	if ! type ${tool} > /dev/null 2>&1; then
	    fail 1 "Could not find ${tool}. ${tool} is a required tool."
	fi
    done
}


# Sets the fqdn, hostname, and domainname variables
determineHostName()
{
    case $(uname) in
        SunOS)
            fqdn=$(/usr/lib/mail/sh/check-hostname |cut -d" " -f7)
            ;;
         Darwin)
            fqdn=$(hostname)
            ;;
        *)
            fqdn=$(hostname --fqdn)
            ;;
    esac

    hostname=${fqdn%%.*}

    if [ "$hostname" = "$fqdn" ]; then
        domainname=
    else
        domainname=${fqdn#*.}
    fi
}


# Converts a string describing some amount of disk space (using an
# optional suffix of k, K, m, M, g, G, t, T, for powers of 1024) to an
# integer number of GiB.
stringToGiB() # $1 = size
{
    case $1 in
        *k)
            RET=$((${1%?}/(1024*1024)))
            ;;

        *K)
            RET=$((${1%?}/(1024*1024)))
            ;;

        *m)
            RET=$((${1%?}/1024))
            ;;

        *M)
            RET=$((${1%?}/1024))
            ;;

        *g)
            RET=$((${1%?}))
            ;;

        *G)
            RET=$((${1%?}))
            ;;

        *t)
            RET=$((${1%?}*1024))
            ;;

        *T)
            RET=$((${1%?}*1024))
            ;;

        *)
            RET=$(($1/(1024*1024*1024)))
            ;;
    esac
}

# Extracts the amount of free space in GiB.
getFreeSpace() # $1 = path
{
    [ -d "$1" ] || return 1

    RET=$(df -k "${1}" | awk 'NR == 2 { if (NF < 4) { getline; x = $3 } else { x = $4 }; printf "%d", x / (1024 * 1024)}')
}

# Reads configuration file into shell variables. The shell variable
# names can optionally be prefixed. Returns 1 if file does not exist.
readconf() # $1 = file $2 = prefix
{
    [ -f "$1" ] &&
    eval $(sed -f "${DCACHE_LIB}/config.sed" "$1"  |
        sed -e "s/\([^=]*\)=\(.*\)/$2\1=\2/")
}
