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

. ${DCACHE_HOME}/share/lib/bootLoader.sh

getProperty=$(bootLoader -q compile -shell)
eval "$getProperty"

DCACHE_LIB="$(getProperty dcache.paths.share.lib)"
DCACHE_CONFIG="$(getProperty dcache.paths.config)"
DCACHE_ETC="$(getProperty dcache.paths.etc)"
DCACHE_BIN="$(getProperty dcache.paths.bin)"
DCACHE_JOBS="$(getProperty dcache.paths.jobs)"
