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

# Wrapper for getProperty that prefixes the property name with
# the service name.
getScopedProperty() # $1 = property, $2 = domain, $3 = cell
{
   getProperty "$(getProperty dcache.domain.service "$2" "$3").$1" "$2" "$3"
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

isJavaVersionOk()
{
    version=$($JAVA -version 2>&1)
    case $version in
        *1.[78]*)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

bootLoader()
{
    CLASSPATH="${DCACHE_CLASSPATH}" "$JAVA" -client -XX:+TieredCompilation \
	-XX:TieredStopAtLevel=1  "-Dlog=${DCACHE_LOG:-warn}" \
	"-Ddcache.home=${DCACHE_HOME}" \
	"-Ddcache.paths.defaults=${DCACHE_DEFAULTS}" \
	org.dcache.boot.BootLoader "$@"
}

shortHostname()
{
    local host
    host=$(hostname)
    echo ${host%%.*}
}

# Prints the classpath to include plugins for a given domain
printPluginClassPath() # $1 = domain
{
    local plugins
    local plugin
    local jar

    plugins="$(getProperty dcache.paths.plugins "$1")"
    while [ -n "$plugins" ]; do
        # plugins is a colon separated path of directories
        case "$plugins" in
            *:*)
                plugin="${plugins%%:*}"
                plugins="${plugins#*:}"
                ;;
            *)
                plugin="$plugins"
                plugins=
                ;;
        esac
        if [ -d "$plugin" ]; then
            for jar in "$plugin"/*/*.jar; do
                classpath="${jar}:${classpath}"
            done
        fi
    done

    echo ${classpath%:}
}

# Prints the classpath of a domain
printClassPath() # $1 = domain
{
    local classpath
    local plugin_classpath

    classpath="$(getProperty dcache.paths.classpath "$1")"

    plugin_classpath="$(printPluginClassPath "$1")"
    if [ -n "$plugin_classpath" ]; then
        classpath="${plugin_classpath}:${classpath}"
    fi

    echo $classpath
}

# Print the classpath of a CLI.  The dCache jar files are limited to
# those matching the supplied list.
printLimitedClassPath() # $1..$n = list of jar files
{
    local classpath
    local classes_path
    local jar

    classpath="$(printPluginClassPath)"
    classes_path="$(getProperty dcache.paths.classes)"
    for name in "$@"; do
        for jar in $classes_path/$name-*.jar; do
            classpath="${classpath}:${jar}"
        done
    done

    echo ${classpath#:}
}

quickJava()
{
    export CLASSPATH
    "$JAVA" $(getProperty dcache.java.options.short-lived) "$@"
}

isCacheValidForFiles()
{
    local f
    for f in "$@"; do
        test -f "$f" && test "$f" -ot "$DCACHE_CACHED_CONFIG" || return
    done
}

isCacheValidForDirs()
{
    local d
    for d in "$@"; do
        test ! -e "$d" || test "$d" -ot "$DCACHE_CACHED_CONFIG" || return
    done
}

loadConfig()
{
    local oracle
    oracle=$(DCACHE_LOG=error bootLoader -q compile -shell)
    eval "$oracle"

    if [ "$(getProperty "dcache.config.cache")" = "true" ]; then
        echo "$oracle" 2> /dev/null > "$DCACHE_CACHED_CONFIG" || :
    fi
}

# Get java location
if ! findJava || ! isJavaVersionOk; then
    echo "Could not find usable Java VM. Please set JAVA_HOME to the path to Java 7"
    echo "or newer."
    exit 1
fi

if [ -s $DCACHE_CACHED_CONFIG ]; then
    . $DCACHE_CACHED_CONFIG
   # NB. "hostname -s" does not work on Solaris machines
   if ! eval isCacheValidForFiles $(getProperty dcache.config.files) ||
      ! eval isCacheValidForDirs $(getProperty dcache.config.dirs) ||
      [ "$(getProperty host.name)" != "$(shortHostname)" ]; then
       loadConfig
   fi
else
   loadConfig
fi
