#!/bin/sh
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#
#     were are we ? who are we ?
#
#
if [ -z "$initDone" ] ; then 
#echo "DEBUG : running <init>"
initDone=true
D0=$0
#
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
      ourHome=$J_HOME/..
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
      PRG=`type -p $0` >/dev/null 2>&1
      while [ -L "$PRG" ]
      do
          newprg=`expr "\`/bin/ls -l "$PRG"\`" : ".*$PRG -> \(.*\)"`
          expr "$newprg" : / >/dev/null || newprg="`dirname $PRG`/$newprg"
          PRG="$newprg"
      done
      #
      thisDir=`dirname $PRG`
      ourHome=${thisDir}/..
      weAre=`basename $0`
      return 0
   }
   #
   #--------------------------------------------------------------
   weAreIrix() {
      PRG="`/usr/bsd/which \"$D0\"`" >/dev/null 2>&1
      J_HOME="`/usr/bin/dirname \"$PRG\"`"
      progname=`/sbin/basename "$D0"`

      while [ -h "$PRG" ]; do
          ls=`/sbin/ls -ld "$PRG"`
          link=`/sbin/expr "$ls" : '^.*-> \(.*\)$'`
          if /sbin/expr "$link" : '^/' > /dev/null; then
             prg="$link"
          else
             prg="`/usr/bin/dirname \"$PRG\"`/$link"
          fi
          PRG=`/usr/bsd/which "$prg"` >/dev/null 2>&1
          J_HOME=`/usr/bin/dirname "$PRG"`
      done
      thisDir=$J_HOME
      ourHome=$J_HOME/..
      weAre=$progname
      return 0
   }
   weAreIrixxxx() {

      PRG=`whence $0 2>/dev/null`
      while [ -L "$PRG" ]
      do
          newprg=`expr "\`/bin/ls -l "$PRG"\`" : ".*$PRG -> \(.*\)"`
          expr "$newprg" : "\/" >/dev/null || newprg="`dirname $PRG`/$newprg"
          PRG="$newprg"
      done
      #
      thisDir=$PRG
      ourHome=$PRG/..
      weAre=$progname
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
   else
      echo "Sorry, no support for $os" 1>&2
      exit 3
   fi
   #
   expr ${thisDir} : "/.*"  >/dev/null || thisDir=`pwd`/${thisDir}
#   echo "thisDir : ${thisDir}"
#   echo "ourHOme : ${ourHome}"
#   echo "weAre   : ${weAre}"
#   exit 0
   bins=$ourHome/bin
   jobs=$thisDir
   info=$ourHome/info
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
   #   try to find a setupfile
   #
   ourBaseName=`echo ${weAre} | awk -F. '{ printf "%s",$1 }' 2>/dev/null`
   setupFileName=${ourBaseName}Setup
   if [ -z "${setupFileName}" ] ; then
      echo "Cannot determine setupFileName" 1>&2
      exit 4
   fi 
   if [ -f ${jobs}/${setupFileName} ] ; then
      setupFilePath=${jobs}/${setupFileName}
   elif [ -f ${jobs}/${setupFileName}-`uname -n` ] ; then
      setupFilePath=${jobs}/${setupFileName}-`uname -n`
   elif [ -f /etc/${setupFileName} ] ; then
      setupFilePath=/etc/${setupFileName}
   else
     echo "Setupfile <${setupFileName}> not found in (/etc/.. ${jobs}/.. " 1>&2
     exit 4
   fi
   setupFilePath=`getFull $setupFilePath`
#   echo "Using setupfile : $setupFilePath"
   . ${setupFilePath}
#
#     end of init
#
fi
lib=${ourBaseName}.lib.sh
if [ ! -f ${jobs}/${lib} ] ; then
   echo "Library not  found : $lib"
   exit 4
fi
#echo "Running ${lib}"
. ${jobs}/${lib}
x="${ourBaseName}Switch $*"
#echo "Execute : $x"
eval $x
