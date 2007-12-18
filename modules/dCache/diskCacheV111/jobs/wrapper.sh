#!/bin/sh
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#
#     were are we ? who are we ?
#
#
if [ -z "$initDone" ] ; then 
#echo "DEBUG : running <init>"
initDone=true
#
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
      ourHomeDir=$J_HOME/..
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
      ourHomeDir=${thisDir}/..
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
      thisDir=$PRG
      ourHomeDir=$PRG/..
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
   bins=$ourHomeDir/bin
   jobs=$thisDir
   info=$ourHomeDir/info
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
   if [ -z "$setup" ] ; then
      setupFileName=${ourBaseName}Setup
   else
      setupFileName=${setup}
   fi
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
#
# split the arguments into the options -<key>=<value> and the 
# positional arguments.
#
   args=""
   while [ $# -gt 0 ] ; do
     if expr "$1" : "-.*" >/dev/null ; then
        a=`expr "$1" : "-\(.*\)" 2>/dev/null`
        key=`echo "$a" | awk -F= '{print $1}' 2>/dev/null`
        value=`echo "$a" | awk -F= '{print $2 }' 2>/dev/null`
        if [ -z "$value" ] ; then a="${key}=" ; fi
        eval "$a"
        a="export ${key}"
        eval "$a"
     else
        args="${args} $1"
     fi
     shift 1
   done
#
if [ ! -z "$args" ] ; then
   set `echo "$args" | awk '{ for(i=1;i<=NF;i++)print $i }'`
fi
#     end of init
#
fi
lib=${ourBaseName}.lib.sh
if [ ! -f ${jobs}/${lib} ] ; then
   echo "Library not  found : $lib"
   exit 4
fi
. ${jobs}/${lib}
x="${ourBaseName}Switch $*"
eval $x
