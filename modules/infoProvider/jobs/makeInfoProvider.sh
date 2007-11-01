#!/bin/sh
#

printf "Checking for java ..... "
which java >/dev/null 2>/dev/null
if [ $? -ne 0 ] ; then
   echo "Not found"
else
   java -version 2>&1 | head -1
fi

printf "Checking for cells .... "
export CLASSPATH
if [ -f ../classes/cells.jar ]
  then
    java -jar ../classes/cells.jar | grep SpecificationVersion | awk '{ print $NF }'
    CLASSPATH=../classes/cells.jar
else
    printf "Not found\n"
    exit 3
fi

printf "Checking for dCache .... "
if [ -f ../classes/dcache.jar ]
  then
    CLASSPATH=$CLASSPATH:../classes/dcache.jar
    java diskCacheV111.util.Version
else
    printf "Not found\n"
    exit 3
fi

printf "Checking for glite security ... \n"
if [ -f ../classes/gplazma/vo-mapping/lib/glite-security-trustmanager-1.6.3dev.jar ]
  then
    CLASSPATH=$CLASSPATH:../classes/gplazma/vo-mapping/lib/glite-security-trustmanager-1.6.3dev.jar
else
    printf "Not found\n"
    exit 3
fi
if [ -f ../classes/gplazma/vo-mapping/lib/glite-security-util-java-1.0.0dev.jar ]
  then
    CLASSPATH=$CLASSPATH:../classes/gplazma/vo-mapping/lib/glite-security-util-java-1.0.0dev.jar
else
    printf "Not found\n"
    exit 3
fi

printf "Checking for axis ... \n"
if [ -f ../classes/axis/axis.jar ]
  then
    CLASSPATH=$CLASSPATH:../classes/axis/axis.jar
else
    printf "Not found\n"
    exit 3
fi

printf "Checking for jaxrpc ... \n"
if [ -f ../classes/axis/jaxrpc.jar ]
  then
    CLASSPATH=$CLASSPATH:../classes/axis/jaxrpc.jar
else
    printf "Not found\n"
    exit 3
fi


printf "Checking for clarens-discovery ... \n"
if [ -f ../classes/jclarens/clarens-discovery.jar ]
  then
    CLASSPATH=$CLASSPATH:../classes/jclarens/clarens-discovery.jar
else
    printf "Not found\n"
    exit 3
fi

#
cd ../src
printf "Compiling ... "
javac `find . -name "*.java"`
[ $? -ne 0 ] || echo "Done"
printf "making jar ... "
echo "Main-Class: infoDynamicSE.InfoProvider" > mc.mf
jar -cvfm ../classes/infoDynamicSE.jar mc.mf  `find . -name "*.class"`
[ $? -ne 0 ] || echo "Done"
rm -rf mc.mf
#
exit 0
