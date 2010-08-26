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
#
if [ -f ../docs/javadoc  ] ; then
 mkdir -p ../docs/javadoc
fi 
#
printf "Generating ..... "
javadoc  -private  -d ../docs/javadoc  -sourcepath ../src  org.dcache.services.infoCollector  infoDynamicSE

exit 0		     	
