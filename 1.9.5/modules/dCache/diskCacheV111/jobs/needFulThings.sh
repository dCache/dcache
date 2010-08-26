#=========================================================================
#
#         needful things
#
#-------------------------------------------------------------------------
#
#           
getFull() {
   xdc=$1 
   expr "$xdc" : "\/" >/dev/null 2>&1 || xdc="`pwd`/$xdc"
   echo $xdc
   return 0
}
#=========================================================================
#
#         needful things
#
autodetectPnfs() {
  df -k | awk '{ if( NR > 1 )print $NF }'  | while read mounted 
  do
     if [ ! -f "$mounted/.(const)(magic)" ] ; then continue ; fi
     echo "$mounted"
     exit 44
  done
  if [ $? -eq 44 ] ; then exit 0 ; else exit 1 ; fi
}
#
findBinary() {
    z="echo \${$1}"
    x=`( eval  $z )`
    xkey=$1
    if [ \( -z "${x}" \) -o \( "${x}" = "$1" \) ] ; then
#       echo "$1 path not defined, trying to find it." 1>&2
       RES=`which $1 2>/dev/null`
       if [ $? -eq 1 ] ; then
#          echo "$1 not found in PATH, trying ... " 1>&2
          tmp=$PATH
          shift
          while [ $# -gt 0 ] ; do PATH=$PATH:$1 ; shift ; done
          PATH=$PATH:${thisDir}/../bin
          RES=`which $xkey 2>/dev/null`
          if [ $? -eq 1 ] ; then
             echo "Couldn't find $1 at all" 1>&2
             return 4
          fi
       fi
    else
       if [ ! -x ${x} ] ; then
          echo "The specified java path (${x}) couldn't be found" 2>&1
          return 4
       else
          RES=${x}
       fi
    fi
    echo $RES
    return 0
}
findBinary2() {
    xkey=$1
    z="echo \${$2}"
    x=`( eval  $z )`
    if [ \( -z "${x}" \) -o \( "${x}" = "$xkey" \) ] ; then
#       echo "$1 path not defined, trying to find it." 1>&2
       RES=`which $1 2>/dev/null`
       if [ $? -eq 1 ] ; then
#          echo "$1 not found in PATH, trying ... " 1>&2
          tmp=$PATH
          shift
          while [ $# -gt 0 ] ; do PATH=$PATH:$1 ; shift ; done
          PATH=$PATH:${thisDir}/../bin
          RES=`which $xkey 2>/dev/null`
          if [ $? -eq 1 ] ; then
             echo "Couldn't find $1 at all" 1>&2
             return 4
          fi
       fi
    else
       if [ ! -x ${x} ] ; then
          echo "The specified java path (${x}) couldn't be found" 1>&2
          return 4
       else
          RES=${x}
       fi
    fi
    echo $RES
    return 0
}
checkVar() {
  while [ $# -ne 0 ] ; do
    z="echo \${$1}"
    x=`( eval  $z )`
    if [ -z "$x" ] ; then
      echo "Fatal : Variable not defined in $setupFile : $1" >&2
      return 1
    fi
    shift
  done
}
#############################################################
#
#   try to find  ssh
#
checkForSsh() {
   ssh=`findBinary ssh /usr/bin`
   if [ $? -ne 0 ] ; then echo "Couldn't find ssh" 1>&2 ; return 4 ; fi
   SSH=${ssh}
   SSH_OPTIONS="-c blowfish"
   export SSH_OPTIONS
   #
   #  the ssh keys
   #
   if [ -z "${keyBase}"  ]  ; then
      echo " ! The keyBase is not defined" 1>&2
      echo " ! Please read ${outBaseName}Setup(.temp) for the neccessary infos !" 1>&2 
      return 3
   fi
   if [ \( ! -f "${keyBase}/server_key" \) -o \
        \( ! -f "${keyBase}/host_key"   \)    ] ; then
     echo " ! Trying to create new keys (may take awhile)"
     sshkeygen=`findBinary2 ssh-keygen sshkeygen /usr/bin`   
     if [ $? -ne 0 ] ; then echo "Couldn't find ssh-keygen" 1>&2 ; return 4 ; fi
     SSHKEYGEN=${sshkeygen}
     echo "   keyBase : $keyBase"
     echo " ! Creating server key"
     $SSHKEYGEN -b  768 -f ${keyBase}/server_key -N "" 1>/dev/null
     if [ $? -ne 0 ] ; then
        echo " * Keygen of server key failed" 1>&2
        return 4
     fi
     echo " ! Creating host key"
     $SSHKEYGEN -b 1024 -f ${keyBase}/host_key   -N "" 1>/dev/null
     if [ $? -ne 0 ] ; then
        echo " * Keygen of host key failed" 1>&2
        return 4
     fi
   fi
   return 0
}
checkJava() {
   java=`findBinary java /usr/lib/ibmjava/bin:/usr/lib/java/bin`
   export JAVA_COMPILER
   JAVA_COMPILER=NONE
   JAVA=${java}
   jv=`$JAVA -version 2>&1| head -n1 `
   if [ $? -ne 0 ] ; then
     echo "Fatal : can't find java runtime" 2>&1
     return 7
   fi
   echo "   Using java : $jv"
   xx=`$JAVA -X 2>&1 | grep maxjitcode`
   if [ ! -z "$xx" ] ; then JAVA_OPTIONS=-Xmaxjitcodesize0 ; fi
   xx=`$JAVA 2>&1 | grep nojit`
   if [ ! -z "$xx" ] ; then JAVA_OPTIONS=-nojit ; fi
   export JAVA_OPTIONS
   if [ -z "$JAVA_OPTIONS" ] ; then
       echo "   Can't find option to switch off jit"
   else
       echo "   Using javaoptions : $JAVA_OPTIONS"
   fi
   export CLASSPATH
   CLASSPATH=${dcacheClasses}:${jarDir}/dcache.jar:${jarDir}/dCache.jar:${jarDir}/cells.jar
   #
   # we have to check if we are able to find all classes
   # we need. This is somehow nasty because Linux java
   # returns a $?=0 even if it can't find all the classes.
   #
   err=`$JAVA diskCacheV111.util.Version 2>&1 1>/dev/null`
   if [ "$err" = "Cells not found" ] ; then
       echo " * Cell classes not found in $CLASSPATH" 1>&2
       return 4
   elif [ ! -z "$err" ] ; then
       echo " * DiskCache classes not found in $CLASSPATH" 1>&2
       return 5
   fi
   return 0 ;
}
getCellsLocation() {

    
    echo $CLASSPATH | \
    awk -F: '{ for( i = 1 ; i <= NF ; i++ )print $i }' |
    while read p ; do
       if [ \( -z "$p" \) -o \( ! -f "$p" \) ] ; then continue ; fi
       n=`basename $p`
       if [ "$n" = "cells.jar" ] ; then
          echo $p
          return 0
       fi
    done
    return 1
}
javaClasspath() {
   $JAVA -version 1>/dev/null 2>/dev/null
   if [ $? -ne 0 ] ; then
     echo " * Fatal : can't find java runtime" 2>&1
     return 7
   fi
   checkVar cellClasses || exit 4
   export CLASSPATH
   x1=`getFull $cellClasses`
   x2=`getFull $eurogateHome/../eurogate.jar`
   x3=`getFull $eurogateHome/../..`
   CLASSPATH=$x1:$x2:$x3
   #
   # we have to check if we are able to find all classes
   # we need. This is somehow nasty because Linux java
   # returns a $?=0 even if it can't find all the classes.
   #
   err=`$JAVA eurogate.misc.Version 2>&1 1>/dev/null`
   if [ "$err" = "Cells not found" ] ; then
       echo "Cell classes not found in $CLASSPATH" 1>&2
       exit 4
   elif [ ! -z "$err" ] ; then
       echo "Eurogate classes not found in $CLASSPATH" 1>&2
       exit 5
   fi
   echo $CLASSPATH
   return 0 ;
}
#
findCellsInPath() {
   echo ${CLASSPATH} | awk -F: '{ for(i=1;i<=NF;i++)print $i }' | \
   while read pp ; do
      n=`basename ${pp} 2>/dev/null`
      if [ \( "${n}" = "cells.jar" \) -a  \
           \( -f "${pp}"           \)  ] ; then
      
          echo "${pp}"
      fi
   done
   return 0
}
checkSsh() {
#   checkVar sshPort || exit 4
   which $SSH >/dev/null 2>/dev/null
   if [ $? -ne 0 ] ; then
     echo "Please add 'ssh (versionI)' to your PATH"
     return 4
   fi
   return 0
}
#
killproc() {            # kill the named process(es)
        pid=`/usr/bin/ps -e |
             /usr/bin/grep $1 |
             /usr/bin/sed -e 's/^  *//' -e 's/ .*//'`
        [ "$pid" != "" ] && kill $pid
}
getpids() {            # kill the named process(es)
        pid=`/usr/bin/ps -e |
             /usr/bin/grep $1 |
             /usr/bin/sed -e 's/^  *//' -e 's/ .*//'`
        echo  "$pid"
        return 0
}
