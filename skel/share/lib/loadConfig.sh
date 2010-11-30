# Invokes the dCache boot loader to compile the configuration files to
# a shell function called getProperty. getProperty has two
# parameters. The first parameter is the name of a property. The
# second parameter is optional and is the name of a domain.

# Called by getProperty
undefinedDomain()
{
    echo "Domain $1 is not defined"
    exit 1
}

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

if ! findJava || ! "$JAVA" -version 2>&1 | egrep -e 'version "1\.[6]' >/dev/null ; then
    echo "Could not find usable Java VM. Please set JAVA_HOME to the path to Java 6"
    echo "or newer."
    exit 1
fi

DCACHE_SETUP="${DCACHE_HOME}/share/defaults:${DCACHE_HOME}/etc/dcache.conf"

getProperty=$($JAVA -client -cp "$DCACHE_HOME/classes/*:$DCACHE_HOME/classes/logback/*:$DCACHE_HOME/classes/lib/*" "-Ddcache.home=$DCACHE_HOME" org.dcache.boot.BootLoader -f="$DCACHE_SETUP" -q compile)
eval $getProperty

DCACHE_LIB="$(getProperty dcache.paths.share.lib)"
DCACHE_CONFIG="$(getProperty dcache.paths.config)"
DCACHE_ETC="$(getProperty dcache.paths.etc)"
DCACHE_BIN="$(getProperty dcache.paths.bin)"
DCACHE_JOBS="$(getProperty dcache.paths.jobs)"
