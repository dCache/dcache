#!/bin/sh
###############################################################
#
#
CONTROL=/rms-mufasa/config/opimap
#
checkForJava() {

   java=`findBinary java /usr/java/bin /usr/lib/java/bin`
   if [ $? -ne 0 ] ; then echo "Couldn't find java" ; exit 4 ; fi
   JAVA=${java}
#   echo "Using java : $JAVA"
   xx=`$JAVA -X 2>&1 | grep maxjitcode`
   if [ ! -z "$xx" ] ; then JAVA_OPTIONS=-Xmaxjitcodesize0 ; fi
   xx=`$JAVA 2>&1 | grep nojit`
   if [ ! -z "$xx" ] ; then JAVA_OPTIONS=-nojit ; fi
   export JAVA_OPTIONS
#   echo "Using javaoptions : $JAVA_OPTIONS"
   return 0
}
#
if [ ! -f "${CONTROL}" ] ; then
   echo "Control file not present $CONTROL" 1>&2
   exit 4
fi
#
checkVar cellPath
#
checkForJava || exit 4
#
export CLASSPATH
CLASSPATH=${cellPath}
#
${JAVA} ${JAVA_OPTIONS} \
        dmg.apps.libraryServer.MissionControl \
        ${CONTROL} $* 1>/dev/null 2>/dev/null &

exit 0
