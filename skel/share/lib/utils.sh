# Useful utility functions for shell programming

# Returns true if $1 is contained as a word in $2.
contains() # in $1 = word, in $2+ = list
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
reverse() # out $1 = reverse list, in $2+ = space delimited list of words,
{
    local out
    local ret
    out=$1
    shift
    for s in "$@"; do
        ret="${s} ${ret}"
    done
    eval $out=\"$ret\"
}

# Simplified cross platform version of the BSD column utility. Formats
# stdin into columns.
column()
{
    awk -F"\t" '
    {
        lengths[NR] = NF;
        for (i = 1; i <= NF; i++) {
            fields[sprintf("%d,%d", NR, i)] = $i;
            if (length($i) > widths[i])
                widths[i]=length($i)
        }
    }

    END {
        for (nr =1 ; nr <= NR; nr++) {
            for (i = 1; i <= lengths[nr]; i++)
                printf("%-" widths[i] "s ", fields[sprintf("%d,%d", nr, i)]);
            print "";
        }
    }'
}

# Utility function for printing to stdout with a line width
# maximum of 75 characters. Longer lines are broken into several
# lines. Each argument is interpreted as a separate paragraph.
printp() # in $1+ = list of paragraphs
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


# Indent aware version of printp.  Lines longer than a maximum
# of 75 characters and split into several lines.
printpi() # in $1 = a paragraph, $2 = a pattern to indicate an indentation point
{
    local line
    local line2
    local indent
    local rc

    line=
    indent=
    for word in $1; do
        if [ ${#line} -gt 0 ]; then
	    line2="$line $word"
        else
	    line2="$word"
        fi
	if [ ${#line2} -gt 75 ]; then
	    echo "$line"
	    line="$indent$word"
	else
	    line=$line2
	    if [ "$indent" = "" ]; then
		rc=0
		echo "$line" | grep -E "$2" > /dev/null || rc=$?
		if [ $rc -eq 0 ]; then
                    indent=" $(spaces ${#line})"
		fi
	    fi
	fi
    done
    echo "$line"
}

# Prints an error message to stderr and exist with status $1
fail() # in $1 = exit status, in $2- = list of paragraphs, see printp
{
    local n
    n=$1
    shift
    printp "$@"
    exit $n
} 1>&2

# Returns 0 if the given file is empty, 1 otherwise. The file must
# exist.
isFileEmpty() # in $1 = file
{
    [ $(wc -l < $1) -eq 0 ]
}

spaces() # in $1 = number of spaces
{
    local i
    local output

    i=0
    output=
    while [ $i -lt $1 ]; do
	output="$output "
	i=$(( $i + 1 ))
    done

    echo "$output"
}

# Returns whether a process with a given PID is running
isRunning()# in $1 = pid
{
    rc=0
    ps -p "$1" 1>/dev/null 2>/dev/null || rc=1
    return $rc
}


# Searches for executables and exists with an error message if any of
# them are not found on the PATH.
require() # in $1+ = executable
{
    local tool
    for tool in "$@"; do
	if ! type "${tool}" > /dev/null 2>&1; then
	    fail 1 "Could not find ${tool}. ${tool} is a required tool."
	fi
    done
}

# Tries to locate a Java tool. The name of the tool is provided as an
# argument to the function. Sets the variable of the same name to the
# path to that tool unless the variable was already initialized and
# pointed to the location of the tool.
#
# For instance calling 'requireJavaTool jmap' will set the variable
# jmap to the full path of the jmap utility, unless the variable jmap
# already contained the path to the jmap utility.
#
# Returns with a non-zero exit code if the tool could not be found.
findJavaTool() # in $1 = tool
{
    eval local path=\$$1

    # Check for cached or predefined result
    if [ -n "$path" ]; then
        if [ ! -x "$path" ]; then
            return 0
        fi
    fi

    # See if JAVA is predefined
    if [ -n "$JAVA" ]; then
        path="$(dirname ${JAVA})/$1"
        if [ -x "$path" ]; then
            eval $1=\"$path\"
            return 0
        fi
    fi

    # Look in JAVA_HOME
    if [ -n "$JAVA_HOME" ]; then
        path="$JAVA_HOME/bin/$1"
        if [ -x "$path" ]; then
            eval $1=\"$path\"
            return 0
        fi
    fi

    # Check if it is on the path
    path="$(which $1)"
    if [ -x "$path" ]; then
        eval $1=\"$path\"
        return 0
    fi

    return 1
}

# Converts a string describing some amount of disk space (using an
# optional suffix of k, K, m, M, g, G, t, T, for powers of 1024) to an
# integer number of GiB.
stringToGiB() # in $1 = size, out $2 = size in GiB
{
    local gib
    case $1 in
        *k)
            checkInt "${1%?}"
            gib=$((${1%?}/(1024*1024)))
            ;;

        *K)
            checkInt "${1%?}"
            gib=$((${1%?}/(1024*1024)))
            ;;

        *m)
            checkInt "${1%?}"
            gib=$((${1%?}/1024))
            ;;

        *M)
            checkInt "${1%?}"
            gib=$((${1%?}/1024))
            ;;

        *g)
            checkInt "${1%?}"
            gib=$((${1%?}))
            ;;

        *G)
            checkInt "${1%?}"
            gib=$((${1%?}))
            ;;

        *t)
            checkInt "${1%?}"
            gib=$((${1%?}*1024))
            ;;

        *T)
            checkInt "${1%?}"
            gib=$((${1%?}*1024))
            ;;

        *)
            checkInt "${1%?}"
            gib=$(($1/(1024*1024*1024)))
            ;;
    esac
    eval $2=\"$gib\"
}

checkInt() # in $1 = string
{
   if ! [ "$1" -eq "$1" ] 2> /dev/null; then
      fail 1 "$1 is not an integer."
   fi
}

# Extracts the amount of free space in GiB.
getFreeSpace() # in $1 = path
{
    [ -d "$1" ] && ( df -k "${1}" | awk 'NR == 2 { if (NF < 4) { getline; x = $3 } else { x = $4 }; printf "%d", x / (1024 * 1024)}' )
}

# Reads configuration file into shell variables. The shell variable
# names can optionally be prefixed. Returns 1 if file does not exist.
readconf() # in $1 = file in $2 = prefix
{
    [ -f "$1" ] &&
    eval $(sed -f "$(getProperty dcache.paths.share.lib)/config.sed" "$1"  |
        sed -e "s/\([^=]*\)=\(.*\)/$2\1=\2/")
}

matchesAny() # $1 = word, $2+ = patterns
{
    local word
    word="$1"
    shift
    while [ $# -gt 0 ]; do
        if [ -z "${word##$1}" ]; then
            return 0
        fi
        shift
    done
    return 1
}

processUser() # $1 = process ID
{
    case "$1" in
        [0-9]*)
            ps -o user -p "$1" | sed -ne '2p'
            ;;
        *)
            ;;
    esac
}

# Check prerequisites
require awk df sed wc dirname which ps grep
