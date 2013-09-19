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
printf " Checking JVM ...... "
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
if [ "$x" -ge 5 ] ; then
   extra_5opt="" #"-source 1.4"
else
   extra_5opt=""
fi
echo "Ok"
#
#
if [ "$build_gsiftp" -o "$build_srm" -o "$build_srmv2" ]; then
    printf " Checking cells ...... "
    if [ ! -f "lib/cells.jar" ] ; then
      echo "Failed : cells.jar not found in lib"
      exit 4
    fi
    printf " Checking Globus ...... "
    if [ ! -f "lib/globus/cog-jglobus.jar" ] ; then
      echo "Failed : cog-jglobus.jar not found in lib/globus"
      exit 4
    fi
    if [ ! -f "lib/globus/cog-axis.jar" ] ; then
      echo "Failed : cog-jglobus.jar not found in lib/globus"
      exit 4
    fi
    if [ ! -f "lib/globus/cryptix-asn1.jar" ] ; then
      echo "Failed : cryptix-asn1.jar not found in lib/globus"
      exit 4
    fi
    if [ ! -f "lib/globus/cryptix32.jar" ] ; then
      echo "Failed : cryptix32.jar  not found in lib/globus"
      exit 4
    fi
    if [ ! -f "lib/globus/puretls.jar" ] ; then
      echo "Failed : .puretls.jar not found in lib/globus"
      exit 4
    fi
    if [ ! -f "lib/globus/cryptix.jar" ] ; then
      echo "Failed : cryptix.jar not found in lib/globus"
      exit 4
    fi
    if [ ! -f "lib/globus/log4j-1.2.8.jar" ] ; then
      echo "Failed : log4j-1.2.8.jar not found in lib/globus"
      exit 4
    fi
    if [ ! -f "lib/globus/jce-jdk13-120.jar" ] ; then
      echo "Failed : jce-jdk13-120.jar not found in lib/globus"
      exit 4
    fi
    echo "Ok"
fi
#    
if [ "$build_srm" -o "$build_srmv2" ]; then
    printf " Checking GLUE ...... "
    if [ ! -f "lib/glue/GLUE-STD.jar" ] ; then
      echo "Failed : GLUE-STD.jar not found in lib"
      exit 4
    fi
    if [ ! -f "lib/glue/dom.jar" ] ; then
      echo "Failed : dom.jar not found in lib"
      exit 4
    fi
    if [ ! -f "lib/glue/jnet.jar" ] ; then
      echo "Failed : jnet.jar not found in lib"
      exit 4
    fi
    if [ ! -f "lib/glue/jsse.jar" ] ; then
      echo "Failed : jsse.jar not found in lib"
      exit 4
    fi
    if [ ! -f "lib/glue/jcert.jar" ] ; then
      echo "Failed : jcert.jar not found in lib"
      exit 4
    fi
    if [ ! -f "lib/glue/servlet.jar" ] ; then
      echo "Failed : servlet.jar not found in lib"
      exit 4
    fi
    echo "Ok"
      printf " Checking Axis ........"
      if [ ! -f "lib/axis/jaxrpc.jar" ] ; then
        echo "Failed : jaxrpc.jar  not found in lib/axis"
        exit 4
      fi
      if [ ! -f "lib/axis/saaj.jar" ] ; then
        echo "Failed : saaj.jar not found in lib/axis"
        exit 4
      fi
      if [ ! -f "lib/axis/axis.jar" ]; then
        echo "Failed : axis.jar not found in lib/axis"
        exit 4
      fi
      echo "Ok"
 fi
#
java_sources_all=`find src -name "*.java" | grep -v "Tomcat"`
java_sources_tomcat_plugin=`find src -name "*.java" | grep "Tomcat"`
#
# cells needed for everything
export CLASSPATH=.
    CLASSPATH=$CLASSPATH:lib/cells.jar
    CLASSPATH=$CLASSPATH:lib/jdom.jar

for i in lib/globus/*.jar
  do
  CLASSPATH=$CLASSPATH:$i
done


for i in lib/axis/*.jar
  do
  CLASSPATH=$CLASSPATH:$i
done
    CLASSPATH=$CLASSPATH:lib/glue/GLUE-STD.jar:lib/glue/dom.jar
    CLASSPATH=$CLASSPATH:lib/glue/jnet.jar:lib/glue/jsse.jar
    CLASSPATH=$CLASSPATH:lib/glue/jcert.jar:lib/glue/servlet.jar
#
printf " Compiling SRM ..... "
javac -classpath src:$CLASSPATH $extra_5opt $java_sources_all
#    
if [ $? -ne 0 ] ; then
  echo ""
  echo "  !!! Error in compiling SRM " 1>&2
  exit 5
fi
rm -rf lib/srm.jar 2>/dev/null
echo "Ok"
#
  printf " Making srm.jar .... "
  ( 
    
    rm -rf lib/srm.jar
    cd src
    jar -cf ../lib/srm.jar \
        `find . -name "*.class" | grep -v "Tomcat"`
  )
  echo "Done"
CLASSPATH=$CLASSPATH:lib/globus/cog-tomcat.jar
for tomcatlib in lib/tomcat/*jar ; do
	CLASSPATH=$CLASSPATH:${tomcatlib}
done
printf " Compiling Tomcat Plugin "
javac -classpath src:$CLASSPATH -deprecation $java_sources_tomcat_plugin
#    
if [ $? -ne 0 ] ; then
  echo ""
  echo "  !!! Error in compiling Tomcat Plugin " 1>&2
  exit 5
fi
rm -rf lib/srm-tomcat.jar 2>/dev/null
echo "Ok"
#
  printf " Making srm-tomcat.jar .... "
  ( 
    
    rm -rf lib/srm-tomcat.jar
    cd src
    jar -cf ../lib/srm-tomcat.jar \
        `find . -name "*.class" | grep  "Tomcat"`
  )
  echo "Done"
 printf " Generating Javadoc  "
 bin/generatedocs.sh 2>&1 >/dev/null 2>&1
echo "Done, javadocs are in doc/javadoc"

#
