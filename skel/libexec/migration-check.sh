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
#     daemons.
#
#  2. Adjust /opt/d-cache/config/chimera-config.xml so it points to the
#     Chimera database or inform the comparator to use a different file.
#     This should only be necessary if Chimera database is not running
#     on localhost.
#
#  3. Run this script giving it the filename containing the PNFS IDs;
#     for example:
#
#        ./migration-check.sh /tmp/all-files-in-PNFS
#
#  4. the migration-check.sh will accept an option "-k".  Specifying
#     this option will prevent migration-check from halting on the
#     first error.
#
#  5. Use the "-h" option for further help.


#  Default location for dCache.
default_home=/opt/d-cache

#  Absolute path to the directory this script is running from.
BASE_DIR=$(cd $(dirname $0); pwd)

#  Ask dCache for the externalLibsClassPath.
${DCACHE_HOME:=$default_home}
. $DCACHE_HOME/share/lib/loadConfig.sh

LOG4J_FILE=${DCACHE_CONFIG}/log4j.properties

COMPARATOR=org.dcache.chimera.migration.Comparator
JVM_OPTIONS=-Dlog4j.configuration=file:$LOG4J_FILE

java -cp $(getProperty dcache.paths.classpath) $JVM_OPTIONS $COMPARATOR "$*"
