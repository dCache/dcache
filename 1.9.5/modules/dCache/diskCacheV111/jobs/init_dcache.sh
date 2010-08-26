#!/bin/sh
#
#  check for java
#
#
echo "   Checking java"
changed=0
#
if ! which javac >/dev/null 2>/dev/null ; then
   echo " * Please add java to the PATH variable" 1>&2 
   exit 4 
fi
#
if [ ! -f ../util/RealPath.java ] ; then
  echo " * The current working directory is NOT the dcache jobs dir." 1>&2
  exit 5 
fi
  
if [ ! -f ./needFulThings.sh ] ; then 
   echo " * NeedFulThings not found" 1>&2 ||
   exit  5
fi
. ./needFulThings.sh
  
if ! javac ../util/RealPath.java >/dev/null 2>/dev/null ; then
  echo " * Fatal Problem : can't compile basic module" 1>&2
  exit 5 
fi
#
classpath=${CLASSPATH}
CLASSPATH=../util
realpath=`java RealPath 2>/dev/null`
if [ ! -d "${realpath}" ] ; then
  echo "* Fatal Problem : can't determine real path" 1>&2
  exit 7 
fi

echo "   Trying to get the Cells"
CLASSPATH=${classpath}
if [ -f ./cells.jar ] ; then
   echo "   Cells have already been available"
   echo "      In case, a new version is required,"
   echo "      remove ./cells.jar and restart this script"
else
   cellpath=`findCellsInPath 2>/dev/null`
   if [ -z "${cellpath}" ] ; then 
     echo " * Please add the full cells.jar path to the CLASSPATH" 1>&2
     exit 8
   fi
   echo " ! Got the cells"
   changed=1
   cp ${cellpath} cells.jar
fi
echo "   Trying to make dCache.jar"
if [ -f ./dCache.jar ] ; then
   echo "   dCache has already been available"
   echo "      In case, a new version is required,"
   echo "      remove ./dCache.jar and restart this script"
else
   cd ${realpath}/../..
   echo " ! Compiling dCache ... "
   if ! javac `find . -name "*.java"` ; then
      echo ""
      echo " * Processing stopped due to Compiler errors" 1>&2
      exit 4
   fi
   changed=1
   echo "   dCache succefully compiled"
   echo " ! Creating dCache.jar"
   if ! jar -c0f ./diskCacheV111/jobs/dCache.jar `find . -name "*.class"` ; then
      echo ""
      echo " * Processing stopped due to Jar errors" 1>&2
      exit 4
   fi
fi
jarDir=${realpath}
CLASSPATH=${jarDir}/cells.jar:${jarDir}/dCache.jar
version=`java diskCacheV111.util.Version `
if [ -z "${version}" ] ; then
   echo " * Fatal Problem : couldn't determine dCache Version" 1>&2
   exit 7
fi
#
#
echo "   Checking 'this and that'"
#
cd ${realpath}

if [ \( ! -h ./dcache \) -a \( -f ../dcache \) ] ;then
   echo " * The dcache startup script needs to be a link to "
   echo "   the wrapper. This seems not to be the case."
   echo "   Please Check.... "
   exit 6
else
   rm -rf ./dcache
   ln -s ./wrapper.sh ./dcache
   echo " ! dcachelink created"
fi
if [ \( "$changed" = "1" \) -o \( ! -f ../dCache.tar \) ] ; then
   cd ../..
   dest=./diskCacheV111/jobs
   tar cf ./diskCacheV111/dCache.tar \
        ${dest}/dcacheSetup.temp \
        ${dest}/master.batch ${dest}/pools.batch \
        ${dest}/dCache.jar   ${dest}/cells.jar  \
        ${dest}/wrapper.sh   ${dest}/needFulThings.sh \
        ${dest}/dcache       ${dest}/encp.sh \
        ${dest}/hsmcp.sh     ${dest}/dcache.lib.sh
   echo "   Packaging done ! : ../dCache.tar "
else
   echo "   Package didn't change : ../dCache.tar"
fi
echo ""
echo "          Package  dCache  Version ${version}"
echo ""
echo "   ======>>>>  ../dCache.tar  <<<<======"
echo ""
echo ""
exit 0 
