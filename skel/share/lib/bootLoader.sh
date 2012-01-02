# Declare support methods for invoking the dCache boot loader.

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
    $JAVA -client -cp "$DCACHE_HOME/classes/*" "-Dlog=${DCACHE_LOG:-warn}" "-Ddcache.home=$DCACHE_HOME" "-Ddcache.paths.defaults=$DCACHE_PATHS_DEFAULTS" org.dcache.boot.BootLoader "$@"
}

if ! findJava || ! "$JAVA" -version 2>&1 | egrep -e 'version "1\.[6]' >/dev/null ; then
    echo "Could not find usable Java VM. Please set JAVA_HOME to the path to Java 6"
    echo "or newer."
    exit 1
fi

DCACHE_PATHS_DEFAULTS="@dcache.paths.defaults@"
