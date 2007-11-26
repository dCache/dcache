#!/bin/sh
#
binDir=/usr/people/patrick/cvs-cells/cells/dmg/apps/ampexCopy
acopyHome=/usr/people/patrick/aclCopy
flagDir=${acopyHome}/flags
requestDir=${acopyHome}/requests
dataDir=/bigDisk/ampexCopy
bigDisk=${dataDir}
device=/dev/rmt/tps21d4nrns
bsize=4096000
CLASSPATH=/usr/people/patrick/cvs-classes
#
#
checkVar() {
  while [ $# -ne 0 ] ; do
    z="echo \${$1}"
    x=`( eval  $z )`
    if [ -z "$x" ] ; then
      echo "Fatal : Variable not defined in $setupFile : $1" 2>1
      return 1
    fi
    shift
  done
}
updateProgress() {
   echo "x=$1" > ${flagDir}/progress.${tapeName}
   touch ${flagDir}/progress
}
#
checkVar bigDisk device bsize acopyHome CLASSPATH flagDir || exit 5
#
export CLASSPATH
#
if [ $# -eq 0 ] ; then
  echo "Usage : fromAmpex <tapeName>"
  exit 4
fi
tapeName=$1
desc=${acopyHome}/zeus/tapes/${tapeName}
if [ ! -f $desc ] ; then
  echo "Desciption file not found : $desc"
  exit 4
fi
data=${bigDisk}/${tapeName}
mkdir ${data} 1>/dev/null 2>/dev/null 
if [ ! -d ${data} ] ; then
  echo "Couldn't create ${data}"
  exit 7
fi
doneFlag=${data}/.allDone
if [ -f ${doneFlag} ]  ; then
   echo "Tape ${tapeName} already done"
   exit 0
fi
#
#
echo "Rewinding ... ${tapeName}"
mt -f $device rewind
if [ $? -ne 0 ]
then
   echo "Initial rewind failed" 1>&2
   exit 9
fi
echo "Rewinding Done"
#
#
totalCount=`wc $desc | awk '{ print $1 }' 2>/dev/null`
if [ -z "$totalCount" ] ; then
  echo "Can't determine total file count" 1>&2
  exit 2
fi
echo "Total files to dump : $totalCount"
#
current=0
cfsn=1
cat $desc | while read pname fsize tape sblock fsn d1 ; 
do
#
#     get the already used space ( may decide to wait .. )
#
   usedSpace=`du -ks ${bigDisk} 2>/dev/null | awk '{ print $1 }' 2>/dev/null`
   if [ $? -ne 0 ]; then
	echo "Can't determine used space"
	exit 5 ;
   fi
   echo "Used space : $usedSpace KBytes"
   echo "Working on : $pname "
   echo $pname $fsize $tape $sblock $fsn $d1 >>$data/.workingOn
   fname=$fsn
   if [ -f ${data}/.${fname} ]  
   then

      echo "Already done : $fname"
   
   else
      skip=`expr $fsn - $cfsn`
      if [ "$skip" -lt 0 ] ; then
         echo "Description file not in sequence : $desc"
         exit 5
      fi
      echo "Skipping $skip ... "
      mt -f $device fsf $skip
      if [ $? -ne 0 ]; then
	   echo "skip to file $fsn ($skip) failed"
	   exit 5 ;
      fi
      echo "Skipping Done"
      cfsn=$fsn
      echo "Reading .. $fname"
      dd if=$device ibs=$bsize of=${data}/${fname}-x
      if [ $? -ne 0 ]; then
          echo "reading file $fname at $fsn failed"
          exit 6 ;
      fi
      cfsn=`expr $cfsn + 1`
      echo "Reading Done"
      echo "Cutting to $fsize bytes"
      java dmg.apps.ampexCopy.cut ${data}/${fname}-x ${data}/${fname} $fsize
      if [ $? -ne 0 ] ; then 
         echo "Problem while cutting file" 1>&2
         exit 4 ; 
      fi
      rm ${data}/${fname}-x
      echo "Cutting Done"
      echo $pname $fsize $tape $sblock $fsn $d1 > ${data}/.${fname}
      ls -l ${data}/$fname
   fi
   # increment cfsn - cause we read this file
   current=`expr $current + 1`
   rel=`expr $current \* 100`
   rel=`expr $rel \/ $totalCount`
   updateProgress $rel
done
touch ${doneFlag}
exit 0
