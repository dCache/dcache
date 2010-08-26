#!/bin/sh
#
# by defining these variable you can enable optional dcache components building
#
#
goUp() {
#
# on linux we could use 'realpath'
#
  m=`pwd`
  m=`basename $m`
  cd ..
  x=`ls -ld $m 2>/dev/null | awk '/ -> /{ print $NF }' 2>/dev/null`
  if [ ! -z "$x" ]  ; then
     m=$x
     m=`echo $m | awk -F/ '{ for(i=1;i<NF;i++)printf "%s/",$i }'`
     cd $m
  fi
}
printf " Checking JVM ........ "
which java 1>/dev/null 2>/dev/null
if [ $? -ne 0 ] ; then
  echo "Failed : Java stuff not found on PATH" 
  exit 4
fi
version=`java -version 2>&1 | grep version | awk '{print $3}'`
x=`expr $version : "\"\(.*\)\"" | awk -F. '{ print $2 }'`
if [ "$x" -lt 4 ] ; then
   echo "Failed : Insufficient Java Version : $version"
   exit 4
fi
echo "Ok"
#
#
if [ "$build_gsiftp" -o "$build_srm" -o "$build_srmv2" ]; then
    printf " Checking cells ...... "
    if [ ! -f "classes/cells.jar" ] ; then
      echo "Failed : cells.jar not found in classes"
      exit 4
    fi
    printf " Checking Globus ...... "
    if [ ! -f "classes/globus/cog-jglobus.jar" ] ; then
      echo "Failed : cog-jglobus.jar not found in classes/globus"
      exit 4
    fi
    if [ ! -f "classes/globus/cog-axis.jar" ] ; then
      echo "Failed : cog-jglobus.jar not found in classes/globus"
      exit 4
    fi
    if [ ! -f "classes/globus/cryptix-asn1.jar" ] ; then
      echo "Failed : cryptix-asn1.jar not found in classes/globus"
      exit 4
    fi
    if [ ! -f "classes/globus/cryptix32.jar" ] ; then
      echo "Failed : cryptix32.jar  not found in classes/globus"
      exit 4
    fi
    if [ ! -f "classes/globus/puretls.jar" ] ; then
      echo "Failed : .puretls.jar not found in classes/globus"
      exit 4
    fi
    if [ ! -f "classes/globus/cryptix.jar" ] ; then
      echo "Failed : cryptix.jar not found in classes/globus"
      exit 4
    fi
    if [ ! -f "classes/globus/log4j-1.2.8.jar" ] ; then
      echo "Failed : log4j-1.2.8.jar not found in classes/globus"
      exit 4
    fi
    if [ ! -f "classes/globus/jce-jdk13-120.jar" ] ; then
      echo "Failed : jce-jdk13-120.jar not found in classes/globus"
      exit 4
    fi
    echo "Ok"
fi
#    
if [ "$build_srm" -o "$build_srmv2" ]; then
    printf " Checking GLUE ...... "
    if [ ! -f "classes/glue/GLUE-STD.jar" ] ; then
      echo "Failed : GLUE-STD.jar not found in classes"
      exit 4
    fi
    if [ ! -f "classes/glue/dom.jar" ] ; then
      echo "Failed : dom.jar not found in classes"
      exit 4
    fi
    if [ ! -f "classes/glue/jnet.jar" ] ; then
      echo "Failed : jnet.jar not found in classes"
      exit 4
    fi
    if [ ! -f "classes/glue/jsse.jar" ] ; then
      echo "Failed : jsse.jar not found in classes"
      exit 4
    fi
    if [ ! -f "classes/glue/jcert.jar" ] ; then
      echo "Failed : jcert.jar not found in classes"
      exit 4
    fi
    if [ ! -f "classes/glue/servlet.jar" ] ; then
      echo "Failed : servlet.jar not found in classes"
      exit 4
    fi
    echo "Ok"
      printf " Checking Axis ........"
      if [ ! -f "classes/axis/jaxrpc.jar" ] ; then
        echo "Failed : jaxrpc.jar  not found in classes/axis"
        exit 4
      fi
      if [ ! -f "classes/axis/saaj.jar" ] ; then
        echo "Failed : saaj.jar not found in classes/axis"
        exit 4
      fi
      if [ ! -f "classes/axis/axis.jar" ]; then
        echo "Failed : axis.jar not found in classes/axis"
        exit 4
      fi
      echo "Ok"
 fi
#
java_sources_all=`find . -name "*.java" | grep -v dcache | grep -v v2_1 | grep -vi tomcat | grep -v movers`
#
# cells needed for everything
export CLASSPATH=../..
    CLASSPATH=$CLASSPATH:classes/cells.jar
    CLASSPATH=$CLASSPATH:classes/globus/puretls.jar
    CLASSPATH=$CLASSPATH:classes/globus/cryptix.jar    
    CLASSPATH=$CLASSPATH:classes/globus/log4j-1.2.8.jar
    CLASSPATH=$CLASSPATH:classes/globus/cog-jglobus.jar  
    CLASSPATH=$CLASSPATH:classes/globus/cog-axis.jar  
    CLASSPATH=$CLASSPATH:classes/globus/cryptix-asn1.jar  
    CLASSPATH=$CLASSPATH:classes/globus/cryptix32.jar  
    CLASSPATH=$CLASSPATH:classes/globus/jce-jdk13-120.jar
    CLASSPATH=$CLASSPATH:classes/glue/GLUE-STD.jar:classes/glue/dom.jar
    CLASSPATH=$CLASSPATH:classes/glue/jnet.jar:classes/glue/jsse.jar
    CLASSPATH=$CLASSPATH:classes/glue/jcert.jar:classes/glue/servlet.jar
    CLASSPATH=$CLASSPATH:classes/axis/jaxrpc.jar
    CLASSPATH=$CLASSPATH:classes/axis/saaj.jar
    CLASSPATH=$CLASSPATH:classes/axis/axis.jar
    CLASSPATH=$CLASSPATH:classes/globus/cog-axis.jar
#
printf " Compiling SRM .... "
javac $java_sources_all
#    
if [ $? -ne 0 ] ; then
  echo ""
  echo "  !!! Error in compiling dCache " 1>&2
  exit 5
fi
rm -rf classes/dcache.jar 2>/dev/null
echo "Ok"
#
  printf " Making srm.jar for srm client ..."
  ( goUp ;
    goUp ;
    rm -rf diskCacheV111/srm/classes/srm.jar
    jar -cf diskCacheV111/srm/classes/srm.jar \
        `find diskCacheV111 -name "*.class"`
  )
  echo "Done"

  printf " Making dcache-srm.jar for srm client ..."
  ( goUp ;
    goUp ;
    rm -rf diskCacheV111/srm/classes/dcache-srm.jar
    jar -cf diskCacheV111/srm/classes/dcache-srm.jar \
        `find diskCacheV111/srm -name "*.class" | grep -v "srm/movers"`
  )
  echo "Done, this jar file goes to srm/lib/dcache directory of srmcp product"
#
