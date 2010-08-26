#!/bin/sh

set -e

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#
#     were are we ? who are we ?
#
#
if [ -z "$initDone" ] ; then
#echo "DEBUG : running <init>"
initDone=true
irixFix=$0
#
#
   makeRealPath() {
      XXAWK=awk
      tp=`uname -s`
      if [ $tp = "SunOS" ] ; then XXAWK=nawk ; fi
      echo $1 | $XXAWK '{
         split($1,a,"/")
         res=0
         for(n=1; n in a ; n++){
            if( ( a[n] == "." ) || ( a[n] == "" ) ){
            }else if( a[n] == ".." ){
               res--
            }else{
              st[res++]= "/" a[n]
            }

         }
         for( n = 0 ; n < res ; n++ )
             m=m st[n] ;
         print m
      }'

      return 0
   }
   weAreSolaris() {
   #----------------
      PRG="`/usr/bin/which \"$0\"`" >/dev/null 2>&1
      J_HOME="`/usr/bin/dirname \"$PRG\"`"
      progname=`/usr/bin/basename "$0"`

      while [ -h "$PRG" ]; do
          ls=`/usr/bin/ls -ld "$PRG"`
          link=`/usr/bin/expr "$ls" : '^.*-> \(.*\)$'`
          if /usr/bin/expr "$link" : '^/' > /dev/null; then
             prg="$link"
          else
             prg="`/usr/bin/dirname \"$PRG\"`/$link"
          fi
          PRG=`which "$prg"` >/dev/null 2>&1
          J_HOME=`/usr/bin/dirname "$PRG"`
      done
      thisDir=$J_HOME
      weAre=$progname
      return 0
   }
   #--------------------------------------------------------------
   #
   weAreLinux() {
   #--------------
      #
      #         get our location
      #
      PRG=`which $0` >/dev/null 2>&1
      while [ -L "$PRG" ]
      do
          newprg=`expr "\`/bin/ls -l "$PRG"\`" : ".*$PRG -> \(.*\)"`
          expr "$newprg" : / >/dev/null || newprg="`dirname $PRG`/$newprg"
          PRG="$newprg"
      done
      #
      thisDir=`dirname $PRG`
      weAre=`basename $0`
      return 0
   }
   #
   #--------------------------------------------------------------
   weAreIrix() {

      PRG=`whence $0 2>/dev/null`
      while [ -L "$PRG" ]
      do
          newprg=`expr "\`/bin/ls -l "$PRG"\`" : ".*$PRG -> \(.*\)"`
          expr "$newprg" : "\/" >/dev/null || newprg="`dirname $PRG`/$newprg"
          PRG="$newprg"
      done
      #
      thisDir=`dirname $PRG`
      weAre=`basename $irixFix`
      return 0

   }
   os=`uname -s 2>/dev/null` || \
       ( echo "Can\'t determine OS Type" 1>&2 ; exit 4 ) || exit $?


   ECHO=echo
   if [ "$os" = "SunOS" ]  ; then
      weAreSolaris
      ECHO=/usr/ucb/echo
   elif [ "$os" = "Linux" ] ; then
      weAreLinux
   elif [ "$os" = "IRIX64" ] ; then
      weAreIrix
   elif [ "$os" = "Darwin" ] ; then
      weAreLinux
   else
      echo "Sorry, no support for $os" 1>&2
      exit 3
   fi
   #
   expr ${thisDir} : "/.*"  >/dev/null || thisDir=`pwd`/${thisDir}
   bins=${thisDir}/../bin
   jobs=$thisDir
   info=${thisDir}/../info
   ourHomeDir=${thisDir}/..
   config=${thisDir}/../config
   pidDir=/var/run

   #
   #  run some needful things
   #
   if [ -z "$needFulThings" ]  ; then
      needFulThings=loaded
      export needFulThings
      if [ ! -f "$jobs/needFulThings.sh" ] ; then
         $ECHO "Panic, not found : $jobs/needFulThings.sh"
         exit 4
      fi
      . $jobs/needFulThings.sh
   fi


   #
   # load external claspath
   #
   if [ -r ${ourHomeDir}/classes/extern.classpath ]
   then
      .  ${ourHomeDir}/classes/extern.classpath
   fi
   #
   #   try to find a setupfile
   #
   ourBaseName=`echo ${weAre} | awk -F. '{ printf "%s",$1 }' 2>/dev/null`
   if [ -z "$setup" ] ; then
      setupFileName=${ourBaseName}Setup
   else
      setupFileName=${setup}
   fi
   if [ -z "${setupFileName}" ] ; then
      echo "Cannot determine setupFileName" 1>&2
      exit 4
   fi
   if [ -f ${config}/${setupFileName} ] ; then
      setupFilePath=${config}/${setupFileName}
   elif [ -f ${config}/${setupFileName}-`uname -n` ] ; then
      setupFilePath=${config}/${setupFileName}-`uname -n`
   elif [ -f /etc/${setupFileName} ] ; then
      setupFilePath=/etc/${setupFileName}
   else
     echo "Setupfile <${setupFileName}> not found in (/etc/.. ${config}/.. " 1>&2
     exit 4
   fi
   setupFilePath=`getFull $setupFilePath`
   setupFilePath=`makeRealPath $setupFilePath`
   ourHomeDir=`makeRealPath ${thisDir}/..`
   logArea="${ourHomeDir}/log"
#   echo "Using setupfile : $setupFilePath"
#   echo "ourHomeDir : ${ourHomeDir}"
   . ${setupFilePath}

   # Sanitycheck for serviceLocatorHost
   if [ -z "${serviceLocatorHost}" ] || [ "${serviceLocatorHost}" = "SERVER" ]; then
       echo "serviceLocatorHost in ${setupFilePath} has to be set."
       exit 4
   fi
#
#
# split the arguments into the options -<key>=<value> and the
# positional arguments.
#
   args=""
   opts=""
   while [ $# -gt 0 ] ; do
     if expr "$1" : "-.*" >/dev/null ; then
        a=`expr "$1" : "-\(.*\)" 2>/dev/null`
        key=`echo "$a" | awk -F= '{print $1}' 2>/dev/null`
        value=`echo "$a" | awk -F= '{print $2 }' 2>/dev/null`
        if [ -z "$value" ] ; then a="${key}=" ; fi
        eval "$a"
        a="export ${key}"
        eval "$a"
        opts="${opts} $1"
     else
        args="${args} $1"
     fi
     shift 1
   done
#
if [ ! -z "$args" ] ; then
   set `echo "$args" | awk '{ for(i=1;i<=NF;i++)print $i }'`
fi
#
# now, were we have the config variables and the command line arguments
# we check for local exceptions.
#
if [ -f ${jobs}/dcache.local.sh ]  ; then

   . ${jobs}/dcache.local.sh

fi
SITE_LOCAL=${jobs}/dcache.local.run.sh
if [ -f ${SITE_LOCAL} ]  ; then

   if [ ! -x ${SITE_LOCAL} ]  ; then
     echo "Site local script is not executable ${SITE_LOCAL}" >&2
     exit 2
   fi

   ${SITE_LOCAL} $* || {
      echo "Site local script (${SITE_LOCAL}) failed : errno = $?"
      exit $?
   }
fi
#
#     end of init
#
fi
lib=${ourBaseName}.lib.sh
if [ ! -f ${jobs}/${lib} ] ; then
   echo "Library not  found : $lib"
   exit 4
fi
#
# in order for the toplink to create and/or use its 
# scema creation and deletion definition files
# we need to be in a well known place and the path 
# to these files is relative to the current working dir
#
cd ${ourHomeDir}
. ${jobs}/${lib}
x="procSwitch $*"
eval $x
