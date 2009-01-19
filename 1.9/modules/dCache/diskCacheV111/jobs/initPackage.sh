#!/bin/sh
#
# $Id: initPackage.sh,v 1.7 2007-10-02 16:51:00 tigran Exp $
#


DCACHE_HOME=$1
if [ "x${DCACHE_HOME}" = "x" ];
then
	# path is not defined are we in $DCACHE_HOME/jobs ?
	pwd=`pwd`
	DCACHE_HOME=`dirname $pwd`
fi

goUp() {
  m=`pwd`
  m=`basename $m`
  cd ..
  if [ -L $m ] ; then
     m=`( ls -l $m | awk '{ print $11 }')`
     m=`echo $m | awk -F/ '{ for(i=1;i<NF;i++)printf "%s/",$i }'`
     cd $m
  fi
}

targetOfLink() {
   ls -l $1 | awk '{
      for(i=1;i<=NF;i++){
        if( $i == "->" ){
           print $(i+1)
           break
        }
      }
   }'
}
#

cd ${DCACHE_HOME}
if [ ! -d ./config ] ; then
  echo "Can't find 'config' directory" >&2
  exit 5
fi
cd ./config
#
if [ ! -f dCacheSetup ] ; then
  echo "" >&2
  echo " Panic : Master Setup File  './config/dCacheSetup' not found" >&2
  echo "" >&2
  echo " You may use ./config/dCacheSetup.temp as template" >&2
  echo "" >&2
  exit 4
else
  echo ""
  echo " Checking MasterSetup  ./config/dCacheSetup O.k."
fi
#
echo ""
echo "   Sanning dCache batch files"
echo ""
for batchName in *.batch  ; do
   domainName=`echo $batchName | awk -F. '{print $1}'`
   #
   echo "    Processing ${domainName}"
   #
   #
   domainSetup=${domainName}Setup
   if [ -L $domainSetup ] ;  then
      r=`targetOfLink $domainSetup`
      if [ "$r" != "dCacheSetup" ] ; then
         echo "    $domainSetup is link to $r, but should point to dCacheSetup"
      fi
   elif [ -f $domainSetup ] ; then
      if [ "$domainSetup" != "dCacheSetup" ]  ; then
         echo "        !!! $domainSetup is a file, but should point to dCacheSetup"
      fi
   else
      ln -s dCacheSetup ${domainSetup}
   fi
   #
   goUp 
   cd jobs
   #
   if [ -L $domainName ] ;  then
      r=`targetOfLink $domainName`
      if [ "$r" != "wrapper2.sh" ] ; then
         echo "    $domainName is link to $r, but should point to wrapper2.sh"
      fi
   elif [ -f $domainName ] ; then
      echo "        !!! $domainName is a file, but should point to wrapper2.sh"
   else
      ln -s wrapper2.sh $domainName
   fi
   #
   #
   domainLib=${domainName}.lib.sh
   if [ -L $domainLib ] ;  then
      r=`targetOfLink ${domainLib}`
      if [ "$r" != "generic.lib.sh" ] ; then
         echo "    $domainLib is link to $r, but should point to generic.lib.sh"
      fi
   elif [ -f $domainLib ] ; then
      echo "        !!! $domainLib is a file, but should point to generic.lib.sh"
   else
      ln -s generic.lib.sh ${domainLib}
   fi
   #
   goUp
   cd config
done
#
echo ""
echo ""
printf " Checking Users database .... "
   if [ ! -d users ]  ; then
      mkdir users >/dev/null 2>&1 
      if [ $? -ne 0 ] ; then
         echo " Failed : can't create ..../config/users"
      else
         mkdir  users/acls users/meta users/relations >/dev/null 2>&1
         if [ $? -ne 0 ] ; then
            echo " Failed : can't create ..../config/users/acls|meta|relations"
         else
            echo " Created"
         fi 
      fi 
   else
      isok=0
      for c in acls relations meta ; do
         if [ ! -d $c ] ; then
            mkdir $c >/dev/null 2>/dev/null 
            if [ $? -ne 0 ] ; then
               echo " Failed : can't create .../config/users/$c"
               isok=1
               break
            fi
         fi
      done
      if [ $isok -eq 0 ] ; then echo "Ok" ; fi
   fi

#
#
. ./dCacheSetup
#
printf " Checking Security       .... "
if [ \( ! -f ./server_key \) -o \( ! -f ./host_key \) ] ; then
  echo " server_key and/or host_key are missing "
  echo ""
  echo " Use following commands to generate them: "
  echo "  cd ../config"
  echo "  ssh-keygen -t rsa1 -b  768 -f ./server_key -N \"\" "
  echo "  ssh-keygen -t rsa1 -b 1024 -f ./host_key   -N \"\" "
  echo ""
else
  echo "Ok"
fi
printf " Checking JVM ........ "
version=`$java -version 2>&1 | grep version | awk '{print $3}'`
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
printf " dCacheVersion ....... "
export CLASSPATH
CLASSPATH=../classes/cells.jar:../classes/dcache.jar
version=`$java diskCacheV111.util.Version 2>/dev/null`
if [ $? -ne 0 ] ; then
  echo "Failed"
else
  echo "Version $version"
fi
echo ""
exit 0
