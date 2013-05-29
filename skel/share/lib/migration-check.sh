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


@DCACHE_LOAD_CONFIG@

CLASSPATH="$(getProperty dcache.paths.classpath)" java \
    -Dlog=${DCACHE_LOG:-warn} \
    org.dcache.chimera.migration.Comparator "$@"
