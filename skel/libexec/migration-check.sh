#!/bin/sh
#
#  A simple wrapper around the PNFS-to-Chimera migration test tool.
#
#  To use this tool:
#
#  1. Make a file containing a list of the PNFS IDs of all
#     files you wish to test, something like:
#
#        pnfsDump -o files /tmp/all-files-in-PNFS -f
#
#     NB. pnfsDump must be run on the same machine as the PNFS dbserver
#         daemons.
#
#     NB. As an optimisation, pnfsDump can produce multiple output streams at
#         the same time.  Check manual for details.
#
#  2. Adjust dCache configuration for chimera.  Normally, such configuration is
#     only needed if a remote node is running the PostgreSQL server that hosts
#     chimera.  In such cases, adjust 'chimera.db.host' and possibly
#     'chimera.db.user' and 'chimera.db.password'.
#
#  3. Run this script giving it the filename containing the PNFS IDs;
#     for example:
#
#        ./migration-check.sh /tmp/all-files-in-PNFS
#
#  4. the migration-check.sh will accept an option "-k".
#
#        ./migration-check.sh -k /tmp/all-files-in-PNFS
#
#     Specifying this option will prevent migration-check from halting on the
#     first error.
#
#  5. Use the "-h" option for further help.


# Initialize environment. /etc/default/ is the normal place for this
# on several Linux variants. For other systems we provide
# /etc/dcache.env. Those files will typically declare JAVA_HOME and
# DCACHE_HOME and nothing else.
[ -f /etc/default/dcache ] && . /etc/default/dcache
[ -f /etc/dcache.env ] && . /etc/dcache.env

# Set home path
if [ -z "$DCACHE_HOME" ]; then
    DCACHE_HOME="@dcache.home@"
fi
if [ ! -d "$DCACHE_HOME" ]; then
    echo "$DCACHE_HOME is not a directory"
    exit 2
fi

. @dcache.paths.bootloader@/loadConfig.sh

CLASSPATH="$(getProperty dcache.paths.classpath)" \
    ${JAVA} $(getProperty dcache.java.options) \
    "-Ddcache.home=$DCACHE_HOME" \
    "-Ddcache.paths.defaults=$DCACHE_PATHS_DEFAULTS" \
    "-Dlogback.configurationFile=$(getProperty dcache.paths.share)/xml/logback-cli.xml" \
    org.dcache.chimera.migration.Comparator "$@"
