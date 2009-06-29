# Default paths

[ -n "$DCACHE_LIB" ]       || DCACHE_LIB="${DCACHE_HOME}/share/lib"
[ -n "$DCACHE_CONFIG" ]    || DCACHE_CONFIG="${DCACHE_HOME}/config"
[ -n "$DCACHE_ETC" ]       || DCACHE_ETC="${DCACHE_HOME}/etc"
[ -n "$DCACHE_BIN" ]       || DCACHE_BIN="${DCACHE_HOME}/bin"
[ -n "$DCACHE_JOBS" ]      || DCACHE_JOBS="${DCACHE_HOME}/jobs"
[ -n "$DCACHE_LOCK" ]      || DCACHE_LOCK="/var/lock/subsys/dcache"
[ -n "$DCACHE_PID" ]       || DCACHE_PID="/var/run"
[ -n "$DCACHE_LOG" ]       || DCACHE_LOG="/var/log"
[ -n "$DCACHE_PNFS_ROOT" ] || DCACHE_PNFS_ROOT="/pnfs"
[ -n "$DCACHE_JE" ]        || DCACHE_JE="${DCACHE_HOME}/classes/berkeleyDB/je-3.2.76.jar"