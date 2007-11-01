#!/bin/sh
#
#set -x
binDir=/usr/people/patrick/cvs-cells/cells/dmg/apps/ampexCopy
acopyHome=/usr/people/patrick/aclCopy
flagDir=${acopyHome}/flags
requestDir=${acopyHome}/requests
toOsmDir=${acopyHome}/toOsm
doneDir=${acopyHome}/doneRequests
dataDir=/bigDisk/ampexCopy
bigDisk=${dataDir}
device=/dev/rmt/tps21d4nrns
bsize=4096000
CLASSPATH=/usr/people/patrick/cvs-classes
export acopyHome bigDisk device bsize CLASSPATH flagDir
#
if [ $# -lt 1 ] ; then
   echo "Usage : ... <tapeName>"
   exit 4
fi
tape=$1
data=${bigDisk}/${tape}
if [ ! -d ${data} ] ; then
  echo "Tape directory not found : ${data}" 1>&2
  exit 4
fi
if [ ! -f ${data}/.allDone ] ; then
  echo "Tape copy 'from ampex' not yet finished" 1>&2
  exit 5
fi
cat ${data}/.[0-9]* | \
while read pname fsize xtape sblock fsn d1 
do
   sourcepath=${data}/${fsn}
   osmpath=`echo $pname | \
   awk -F/ '{ printf "/pnfs/io/usr/zeus" ;
              for(i=4;i<=NF;i++)printf "/%s",$i
              printf "\n"
            }'`
   echo "Processing : ${sourcepath} -> ${osmpath}" 
   if [ -f ${data}/.done-${fsn} ] ; then
      echo "Already Done : ${pname}"
      continue ;
   fi
   if [ -f ${osmpath} ] ; then
      echo "Needs correction : ${pname}" 1>&2
      exit 55 ;
   fi
   
   dirPath=`dirname ${osmpath}`
   if [ ! -d ${dirPath} ] ; then
      mkdir -p ${dirPath} 1>/dev/null 2>/dev/null
      if [ $? -ne 0 ] ; then
         echo "Can't create ${dirPath}" 1>&2
         exit 5
      fi
   fi
   osmcp -d -h localhost $sourcepath $osmpath
   rc=$?
   if [ $rc -ne 0 ] ; then
      echo "osmcp failed : $rc"
   else
      rm $sourcepath
      touch ${data}/.done-${fsn}
   fi
done
exit 0
