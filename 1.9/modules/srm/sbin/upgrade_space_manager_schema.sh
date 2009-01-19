#!/bin/sh
#
# script that updates SRM space manager schema 
#

ourHomeDir=/opt/d-cache
if [ -r ${ourHomeDir}/etc/srm_setup.env ] ; then
  . ${ourHomeDir}/etc/srm_setup.env
fi
export JAVA_HOME
for jar in `find ${ourHomeDir}/classes/ -name '*jar'`; do  CLASSPATH=${CLASSPATH}:${jar}; done
export CLASSPATH
$JAVA_HOME/bin/java diskCacheV111.services.space.Manager $*
rc=$?
exit $rc
