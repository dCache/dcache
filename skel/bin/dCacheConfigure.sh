#!/bin/sh

if [ -z "${DCACHE_HOME}" ] ; then
DCACHE_HOME=/opt/d-cache
fi
export DCACHE_CONFIGURE_HOME=${DCACHE_HOME}/share/lib/
export DCACHE_CONFIGURE_TARGETS=${DCACHE_CONFIGURE_HOME}/dCacheConfigure/targets
export DCACHE_CONFIGURE_MODULES=${DCACHE_CONFIGURE_HOME}/dCacheConfigure/modules
export DCACHE_CONFIGURE_PMODS=${DCACHE_CONFIGURE_HOME}/dCacheConfigure/pmods

python ${DCACHE_CONFIGURE_HOME}/dCacheConfigure/ui.py $*
