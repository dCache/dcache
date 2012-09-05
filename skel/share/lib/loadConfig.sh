# Invokes the dCache boot loader to compile the configuration files to
# a shell function called getProperty. getProperty has three
# parameters. The first parameter is the name of a property. The
# second parameter is optional and is the name of a domain. The third
# parameter is optional and is the name of a cell.

# Called by getProperty
undefinedCell()
{
    echo "Cell $3 is not defined in $2"
    exit 1
} 1>&2

# Called by getProperty
undefinedDomain()
{
    echo "Domain $2 is not defined"
    exit 1
} 1>&2

# Called by getProperty
undefinedProperty()
{
    :
}

findJava()
{
    if [ -x "$JAVA" ]; then
        return 0
    fi

    if [ -n "$JAVA_HOME" ]; then
        JAVA="$JAVA_HOME/bin/java"
        if [ -x "$JAVA" ]; then
            return 0
        fi
    fi

    JAVA="$(which java)"
    if [ -x "$JAVA" ]; then
        return 0
    fi

    return 1
}

bootLoader()
{
    $JAVA -client -cp "${DCACHE_CLASSPATH}" "-Dlog=${DCACHE_LOG:-warn}" "-Ddcache.home=${DCACHE_HOME}" "-Ddcache.paths.defaults=${DCACHE_DEFAULTS}" org.dcache.boot.BootLoader "$@"
}

isCacheValid()
{
    local f
    for f in "$@"; do
        test "$f" -ot "$DCACHE_CACHED_CONFIG" || return;
    done
}

loadConfig()
{
    local oracle
    oracle=$(DCACHE_LOG=error bootLoader -q compile -shell)
    eval "$oracle"

    if [ "$(getProperty "dcache.config.cache")" = "true" ]; then
        echo "$oracle" > "$DCACHE_CACHED_CONFIG"
    fi
}

# Get java location
if ! findJava || ! "$JAVA" -version 2>&1 | egrep -e 'version "1\.[7]' >/dev/null ; then
    echo "Could not find usable Java VM. Please set JAVA_HOME to the path to Java 7"
    echo "or newer."
    exit 1
fi

if [ -e $DCACHE_CACHED_CONFIG ]; then
    . $DCACHE_CACHED_CONFIG
   if ! eval isCacheValid $(getProperty dcache.config.files) /etc/hostname; then
       loadConfig
   fi
else
   loadConfig
fi
