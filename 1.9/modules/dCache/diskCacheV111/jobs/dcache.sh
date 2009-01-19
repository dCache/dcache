#!/bin/sh
#
#  $Id: dcache.sh,v 1.15 2002-01-09 14:23:53 cvs Exp $
#
DCACHE_BASE=/usr/d-cache
DCACHE_JOBS=${DCACHE_BASE}/jobs
DCACHE_CONFIG=${DCACHE_BASE}/config
#
domains=${DCACHE_CONFIG}/`hostname`.domains
#

handleDomains() {
   if [ ! -f ${domains} ] ; then
     echo "No domains defined for host :  `hostname`" >&2
     exit 3
   fi
  for c in `cat ${domains}` ; do
    if [ ! -f ${DCACHE_CONFIG}/${c}.poollist ] ; then
       echo "ERROR : definition for Domain $c not found. skipped ..." >&2
       continue
    fi
    ${DCACHE_JOBS}/pools -pool=$c $1
  done
  return 0
}

case $1 in 
  start)
     handleDomains start
  ;;
  stop)
     handleDomains stop
  ;;
  *)
    echo "Usage : dcache.sh  start|stop"
    exit 4
esac

exit 0
