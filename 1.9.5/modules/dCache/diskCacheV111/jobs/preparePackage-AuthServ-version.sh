#!/bin/sh
#
# by defining these variable you can enable optional dcache components building
#
build_srm=true
#build_srmv2=true
build_gsiftp=true
#build_tomcat=true
#build_authserv=true
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
if [ "$x" -lt 3 ] ; then
   echo "Failed : Insufficient Java Version : $version"
   exit 4
fi
echo "Ok"
#
printf " Checking Cells ...... "
if [ ! -f "../classes/cells.jar" ] ; then
  echo "Failed : cells.jar not found in ../classes"
  exit 4
fi
echo "Ok"
#

if [ "$build_gsiftp" -o "$build_srm" -o "$build_srmv2" ]; then
    rm -rf ../classes/globus
    cp -r ../srm/classes/globus ../classes
    rm -rf ../classes/globus/CVS
    printf " Checking Globus ..... "
    if [ ! -f "../classes/globus/cog-jglobus.jar" ] ; then
      echo "Failed : cog-jglobus.jar not found in ../classes/globus"
      exit 4
    fi
    if [ ! -f "../classes/globus/cog-url.jar" ] ; then
      echo "Failed : cog-url.jar not found in ../classes/globus"
      exit 4
    fi
    if [ ! -f "../classes/globus/cryptix32.jar" ] ; then
      echo "Failed : cryptix32.jar  not found in ../classes/globus"
      exit 4
    fi
    if [ ! -f "../classes/globus/cryptix-asn1.jar" ] ; then
      echo "Failed : cryptix-asn1.jar not found in ../classes/globus"
      exit 4
    fi
    if [ ! -f "../classes/globus/cryptix.jar" ] ; then
      echo "Failed : cryptix.jar not found in ../classes/globus"
      exit 4
    fi
    if [ ! -f "../classes/globus/jce-jdk13-125.jar" ] ; then
      echo "Failed : jce-jdk13-125.jar not found in ../classes/globus"
      exit 4
    fi
    if [ ! -f "../classes/globus/jgss.jar" ] ; then
      echo "Failed : jgss.jar not found in ../classes/globus"
      exit 4
    fi
    if [ ! -f "../classes/globus/junit.jar" ] ; then
      echo "Failed : junit.jar not found in ../classes/globus"
      exit 4
    fi
    if [ ! -f "../classes/globus/log4j-1.2.8.jar" ] ; then
      echo "Failed : log4j-1.2.8.jar not found in ../classes/globus"
      exit 4
    fi
    if [ ! -f "../classes/globus/puretls.jar" ] ; then
      echo "Failed : .puretls.jar not found in ../classes/globus"
      exit 4
    fi
    echo "Ok"
fi
#    
if [ "$build_srm" -o "$build_srmv2" ]; then
    printf " Checking SRM ........"
    if [ ! -f "../classes/srm.jar" ]; then
      echo "Failed : srm.jar  not found in ../classes"
      exit 4
    fi
    echo "Ok"
#    rm -rf ../classes/glue
#    cp -r ../srm/classes/glue ../classes
#    rm -rf ../classes/glue/CVS
    printf " Checking GLUE ....... "
    if [ ! -f "../classes/glue/GLUE-STD.jar" ] ; then
      echo "Failed : GLUE-STD.jar not found in ../classes"
      exit 4
    fi
    if [ ! -f "../classes/glue/dom.jar" ] ; then
      echo "Failed : dom.jar not found in ../classes"
      exit 4
    fi
    if [ ! -f "../classes/glue/jnet.jar" ] ; then
      echo "Failed : jnet.jar not found in ../classes"
      exit 4
    fi
    if [ ! -f "../classes/glue/jsse.jar" ] ; then
      echo "Failed : jsse.jar not found in ../classes"
      exit 4
    fi
    if [ ! -f "../classes/glue/jcert.jar" ] ; then
      echo "Failed : jcert.jar not found in ../classes"
      exit 4
    fi
    if [ ! -f "../classes/glue/servlet.jar" ] ; then
      echo "Failed : servlet.jar not found in ../classes"
      exit 4
    fi
    echo "Ok"
#    rm -rf ../classes/axis
#    cp -r ../srm/classes/axis ../classes
#    rm -rf ../classes/axis/CVS
    printf " Checking Axis ........"
    if [ ! -f "../classes/axis/jaxrpc.jar" ] ; then
      echo "Failed : jaxrpc.jar  not found in ../classes/axis"
      exit 4
    fi
    if [ ! -f "../classes/axis/saaj.jar" ] ; then
      echo "Failed : saaj.jar not found in ../classes/axis"
      exit 4
    fi
    if [ ! -f "../classes/axis/axis.jar" ]; then
      echo "Failed : axis.jar not found in ../classes/axis"
      exit 4
    fi
    if [ ! -f "../classes/globus/cog-axis.jar" ]; then
      echo "Failed : cog-axis.jar not found in ../classes/globus"
      exit 4
    fi
    echo "Ok"
 fi
#
 if [ "$build_srmv2" -o "${build_tomcat}" ]; then
#      rm -rf ../classes/tomcat
#      cp -r ../srm/classes/tomcat ../classes
#      rm -rf ../classes/tomcat/CVS
      printf " Checking Tomcat ........"
      if [ ! -f "../classes/tomcat/catalina.jar" ]; then
        echo "Failed : catalina.jar  not found in ../classes/tomcat"
        exit 4
      fi
      echo "Ok"
 fi
 
#
 if [ "$build_srmv2" ]; then
      rm -rf ../classes/axis
      cp -r ../srm/classes/axis ../classes
      rm -rf ../classes/axis/CVS
      printf " Checking Axis ........"
      if [ ! -f "../classes/axis/jaxrpc.jar" ] ; then
        echo "Failed : jaxrpc.jar  not found in ../classes/axis"
        exit 4
      fi
      if [ ! -f "../classes/axis/saaj.jar" ] ; then
        echo "Failed : saaj.jar not found in ../classes/axis"
        exit 4
      fi
      if [ ! -f "../classes/axis/axis.jar" ]; then
        echo "Failed : axis.jar not found in ../classes/axis"
        exit 4
      fi
      echo "Ok"
 fi
#
 if [ "$build_authserv" ]; then
      rm -rf ../classes/OSG-privilege
      cp -r ../services/authorization/classes/OSG-privilege ../classes
      rm -rf ../classes/OSG-privilege/CVS
      rm -rf ../classes/OSG-privilege/target/CVS
      rm -rf ../classes/OSG-privilege/target/privilege/CVS
      rm -rf ../classes/OSG-privilege/target/privilege/WEB-INF/CVS
      rm -rf ../classes/OSG-privilege/target/privilege/WEB-INF/lib/CVS
      rm -rf ../classes/OSG-privilege/endorsed/CVS
      rm -rf ../classes/OSG-privilege/etc/CVS
	  
      printf " Checking OSG-Privilege"
      if [ ! -f "../classes/OSG-privilege/etc/log4j.cfg" ] ; then
        echo "Failed : ../classes/OSG-privilege/etc/log4j.cfg not found"
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/target/privilege/WEB-INF/lib/glite-security-trustmanager-1.6.1.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/target/privilege/WEB-INF/lib/glite-security-trustmanager-1.6.1.jar not found"
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/target/privilege/WEB-INF/lib/axis-saaj-1.2-RC2.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/target/privilege/WEB-INF/lib/axis-saaj-1.2-RC2.jarnot found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/target/privilege/WEB-INF/lib/log4j-1.2.8.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/target/privilege/WEB-INF/lib/log4j-1.2.8.jar not found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/target/privilege/WEB-INF/lib/bcprov-jdk14-122.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/target/privilege/WEB-INF/lib/bcprov-jdk14-122.jar not found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/target/privilege/WEB-INF/lib/opensaml-1.0.1.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/target/privilege/WEB-INF/lib/opensaml-1.0.1.jar not found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/target/privilege/WEB-INF/lib/axis-1.2-RC2.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/target/privilege/WEB-INF/lib/axis-1.2-RC2.jar not found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/target/privilege/WEB-INF/lib/axis-jaxrpc-1.2-RC2.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/target/privilege/WEB-INF/lib/axis-jaxrpc-1.2-RC2.jar not found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/target/privilege/WEB-INF/lib/axis-wsdl4j-1.2-RC2.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/target/privilege/WEB-INF/lib/axis-wsdl4j-1.2-RC2.jar not found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/target/privilege/WEB-INF/lib/xmlsec-1.1.0.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/target/privilege/WEB-INF/lib/xmlsec-1.1.0.jar not found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/target/privilege/WEB-INF/lib/commons-discovery-0.2.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/target/privilege/WEB-INF/lib/commons-discovery-0.2.jar not found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/target/privilege/WEB-INF/lib/commons-logging-1.0.3.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/target/privilege/WEB-INF/lib/commons-logging-1.0.3.jar not found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/target/privilege/WEB-INF/lib/glite-security-util-java-0.2.0.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/target/privilege/WEB-INF/lib/glite-security-util-java-0.2.0.jar not found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/endorsed/dom3-xml-apis-2.5.0.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/endorsed/dom3-xml-apis-2.5.0.jar not found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/endorsed/dom3-xercesImpl-2.5.0.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/endorsed/dom3-xercesImpl-2.5.0.jar not found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/endorsed/xalan-2.4.1.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/endorsed/xalan-2.4.1.jar not found."
        exit 4
      fi
      if [ ! -f "../classes/OSG-privilege/privilege-0.1.jar" ] ; then
        echo "Failed : ../classes/OSG-privilege/privilege-0.1.jar not found."
        exit 4
      fi
     echo "Ok"
 fi
#
java_sources_all=`find .. -name "*.java" | grep -v web-dcache`
#java_sources_base1=`echo $java_sources_all     | tr " " "\n" | grep -v srm | grep -v tomcat | grep -v GsiFtpDoorV1` 
java_sources_base1=`echo $java_sources_all     | tr " " "\n" | grep -v srm | grep -v tomcat | grep -v GsiFtpDoorV1 | grep -v "services/authorization"`
java_sources_base2=`echo $java_sources_all | tr " " "\n" | grep "vehicles/srm"`
java_sources_base="${java_sources_base1} ${java_sources_base2}"
java_sources_srm_base=`echo $java_sources_all | tr " " "\n" | grep srm | grep -v "vehicles/srm" | grep -v v2_1 |  grep -v "security/Tomcat"`
java_sources_srm_v2_1=`echo $java_sources_all | tr " " "\n" | grep srm | grep -v "vehicles/srm" `
java_sources_tomcat=`echo $java_sources_all   | tr " " "\n" | grep tomcat`
java_sources_gsiftp=`echo $java_sources_all   | tr " " "\n" | grep GsiFtpDoorV1`
java_sources_authserv=`echo $java_sources_all   | tr " " "\n" | grep "services/authorization"`
java_sources=$java_sources_base
#
if [ "${build_srmv2}" ] ; then
	java_sources="${java_sources} ${java_sources_srm_v2_1}"
elif [ "${build_srm}" ] ; then
	java_sources="${java_sources} ${java_sources_srm_base}"
fi
#
if [ "${build_gsiftp}" ] ; then
	java_sources="${java_sources} ${java_sources_gsiftp}"
fi
if [ "${build_tomcat}" ] ; then
	java_sources="${java_sources} ${java_sources_tomcat}"
fi
if [ "$build_authserv" ]; then
        java_sources="${java_sources} ${java_sources_authserv}"
fi

# cells needed for everything
export CLASSPATH=../classes/cells.jar
# globus needed for gsiftp door, and both srms
if [ "${build_gsiftp}" -o "${build_srm}" -o "${build_srmv2}" ]; then
    CLASSPATH=$CLASSPATH:../classes/globus/cog-jglobus.jar  
    CLASSPATH=$CLASSPATH:../classes/globus/cog-url.jar  
    CLASSPATH=$CLASSPATH:../classes/globus/cryptix32.jar  
    CLASSPATH=$CLASSPATH:../classes/globus/cryptix-asn1.jar  
    CLASSPATH=$CLASSPATH:../classes/globus/cryptix.jar    
    CLASSPATH=$CLASSPATH:../classes/globus/jce-jdk13-125.jar
    CLASSPATH=$CLASSPATH:../classes/globus/jgss.jar
    CLASSPATH=$CLASSPATH:../classes/globus/junit.jar
    CLASSPATH=$CLASSPATH:../classes/globus/log4j-1.2.8.jar
    CLASSPATH=$CLASSPATH:../classes/globus/puretls.jar
fi
# glue classes needed by both srms
if [ "${build_srm}" -o "${build_srmv2}" ]; then
    CLASSPATH=$CLASSPATH:../classes/srm.jar
    CLASSPATH=$CLASSPATH:../classes/glue/GLUE-STD.jar:../classes/glue/dom.jar
    CLASSPATH=$CLASSPATH:../classes/glue/jnet.jar:../classes/glue/jsse.jar
    CLASSPATH=$CLASSPATH:../classes/glue/jcert.jar:../classes/glue/servlet.jar
    CLASSPATH=$CLASSPATH:../classes/axis/jaxrpc.jar
    CLASSPATH=$CLASSPATH:../classes/axis/saaj.jar
    CLASSPATH=$CLASSPATH:../classes/axis/axis.jar
    CLASSPATH=$CLASSPATH:../classes/globus/cog-axis.jar
fi
# axis classes needed by srmv2   
if [ "${build_srmv2}" ]; then
      CLASSPATH=$CLASSPATH:../classes/axis/jaxrpc.jar
      CLASSPATH=$CLASSPATH:../classes/axis/saaj.jar
      CLASSPATH=$CLASSPATH:../classes/axis/axis.jar
fi
# tomcat classes needed by srmv2 and tomcat
if [ "$build_tomcat" -o "${build_srmv2}" ] ; then
      CLASSPATH=$CLASSPATH:../classes/tomcat/servlet.jar
      CLASSPATH=$CLASSPATH:../classes/tomcat/catalina.jar
fi
# OSG-privilege classes needed with VO Plugin of Authorization Service 
if [ "$build_authserv" ]; then
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/target/privilege/WEB-INF/lib/glite-security-trustmanager-1.6.1.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/target/privilege/WEB-INF/lib/axis-saaj-1.2-RC2.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/target/privilege/WEB-INF/lib/log4j-1.2.8.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/target/privilege/WEB-INF/lib/bcprov-jdk14-122.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/target/privilege/WEB-INF/lib/opensaml-1.0.1.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/target/privilege/WEB-INF/lib/axis-1.2-RC2.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/target/privilege/WEB-INF/lib/axis-jaxrpc-1.2-RC2.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/target/privilege/WEB-INF/lib/axis-wsdl4j-1.2-RC2.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/target/privilege/WEB-INF/lib/xmlsec-1.1.0.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/target/privilege/WEB-INF/lib/commons-discovery-0.2.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/target/privilege/WEB-INF/lib/commons-logging-1.0.3.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/target/privilege/WEB-INF/lib/glite-security-util-java-0.2.0.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/endorsed/dom3-xml-apis-2.5.0.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/endorsed/dom3-xercesImpl-2.5.0.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/endorsed/xalan-2.4.1.jar
      CLASSPATH=$CLASSPATH:../classes/OSG-privilege/privilege-0.1.jar
fi
#
printf " Compiling dCache .... "
javac $java_sources
#    
if [ $? -ne 0 ] ; then
  echo ""
  echo "  !!! Error in compiling dCache " 1>&2
  exit 5
fi
rm -rf ../classes/dcache.jar 2>/dev/null
echo "Ok"
#
printf " Making dcache.jar ... "
(  goUp ;
   goUp ; 
  rm -rf diskCacheV111/classes/dcache.jar
  java_classes_all=`find . -name "*.class" `
  java_classes_base1=`echo $java_classes_all     | tr " " "\n" | grep -v srm ` 
  java_classes_base2=`echo $java_classes_all | tr " " "\n" | grep -e "vehicles/srm" -e "srm/movers"`
  java_classes_base="${java_classes_base1} ${java_classes_base2}"
  jar cf diskCacheV111/classes/dcache.jar $java_classes_base
 )
echo "Done"
if [ "$build_srm" ]; then
  printf " Making dcache-srm.jar for srm client ..."
  ( goUp ;
    goUp ;
    rm -rf diskCacheV111/classes/dcache-srm.jar
    jar -cf diskCacheV111/classes/dcache-srm.jar \
        `find diskCacheV111/srm -name "*.class" | grep -v "srm/movers"`
  )
  echo "Done, this jar file goes to srm/lib/dcache directory"
fi
if [ "$build_srmv2" ]; then
  printf " Making  dcache-tomcat-user.jar and dcache-tomcat.jar for srmv2 ... "
  ( goUp ;
    goUp ; 
    rm -rf diskCacheV111/classes/dcache-tomcat-user.jar
    jar cf diskCacheV111/classes/dcache-tomcat-user.jar \
    	`find diskCacheV111/util -name "User*.class"` \
	`find diskCacheV111/util -name "KAu*.class"` \
	`find diskCacheV111/srm -name "SRMUser*.class"` \
	`find diskCacheV111/srm/security -name "DCacheU*.class"`
    rm -rf diskCacheV111/classes/dcache-tomcat.jar
    jar cf diskCacheV111/classes/dcache-tomcat.jar \
    	`find diskCacheV111/util -name "User*.class"` \
    	`find diskCacheV111/util -name "KAu*.class"`  \
	`find diskCacheV111/srm/security -name "Tom*.class"` \
	`find diskCacheV111/srm -name "SRMUser*.class"` \
	`find diskCacheV111/srm/security -name "DCac*.class"` \
	`find diskCacheV111/srm/security -name "SRMA*.class"` 
  
   )
  echo "Done, these jars are going into tomcat server/lib directory"
fi

#
printf " dCacheVersion ....... "
CLASSPATH=../classes/cells.jar:../classes/dcache.jar
version=`java diskCacheV111.util.Version 2>/dev/null`
if [ $? -ne 0 ] ; then
  echo "Failed"
else
  echo "Version $version"
fi
goUp
cd config

if [ "otto" = "karl" ] ;then
if [ -f dCacheSetup ] ;then
   echo " Warning : we found a used dCacheSetup file"
   echo " Is it still correct ?"
else
   cp dCacheSetup.temp dCacheSetup
   echo "   A new dCache setup master file has been created."
   echo "   It needs to be custumized"
fi
fi
printf " Preparing config .... "
#
cat >./extract-dcache <<!
./docs/images/eagle_logo.gif
./docs/images/eagle-main.gif
./docs/images/eagle-grey.gif
./docs/images/eagleredtrans.gif
./docs/images/trudey.gif
./docs/images/sorry.gif
./docs/images/redbox.gif
./docs/images/greenbox.gif
./docs/images/yellowbox.gif
./docs/images/bg.jpg
./jobs/needFulThings.sh
./jobs/generic.lib.sh
./jobs/wrapper.sh
./jobs/wrapper2.sh
./jobs/initPackage.sh
./jobs/hsmcp.sh
./jobs/encp.sh
./jobs/osm-hsmcp.sh
./jobs/remote-osmcp.sh
./jobs/rosm-hsmcp.sh
./config/dCacheSetup.temp
./config/PoolManager.conf.temp
./config/defaultPools.poollist.temp
./config/lm.config.temp
./config/setup.temp
!
#
for batchName in *.batch  ; do
  echo ./config/$batchName >>./extract-dcache
done

if [ -f ./extract.local ] ; then cat extract.local >>./extract-dcach ;fi
#
# may contain patch-*.jar as well
#
( cd .. ; find ./classes -name "*.jar" -print >>./config/extract-dcache )
#
# currently only needed to support pam's
#
( cd .. ; 
  if [ -d ./lib ]
  then  
    find ./lib -type f -print >>./config/extract-dcache
  fi
)
#
echo "Done"
#
#
goUp
#
if [ -d ./scripts ] 
  then

    find ./scripts -type f -print >> config/extract-dcache

fi
#
tar cf dcache.tar -T config/extract-dcache
echo ""
echo " Distribution on `pwd`/dcache.tar"
echo ""
exit 0 
