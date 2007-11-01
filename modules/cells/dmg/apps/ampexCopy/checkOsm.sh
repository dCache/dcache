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
sizeOf() {
   s=`ls -l $1 | awk '{ print $5 }' 2>/dev/null`
   if [ -z "$s" ] ; then return 1 ; fi
   echo $s
   return 0
}
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
#   echo "Processing : ${sourcepath} -> ${osmpath}" 
   if [ ! -f ${data}/.done-${fsn} ] ; then
      echo "Not Done : ${fsn} ${pname}"
      continue ;
   fi
   if [ ! -f ${osmpath} ] ; then
      echo "missing : ${fsn} ${pname}"
      continue
   fi
   s1=`sizeOf $sourcepath` 
   s2=`sizeOf $osmpath`
   if [ "$s1" -ne "$s2" ] ; then
     echo "Size not matching $s1 <-> $s2"
     continue ;
   fi
   echo "OK ${fsn} ${s1} ${osmpath}"

done
exit 0
