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
#
#################################################################
#
#   the 'from ampex process'
# 
copyToOsm() { 
   if [ ! -f ${data}/.allDone ] ; then
     echo "Tape copy 'from ampex' not yet finished" 1>&2
     exit 5
   fi
   tmpFile=${data}/.tmp-$$
   pcounter=0
   current=0
   cat ${data}/.[0-9]* >$tmpFile
   maxCount=`wc $tmpFile | awk '{ print $1 }' 2>/dev/null`
   if [ -z "$maxCount" ] ; then
      echo "Can't determine maxCount" 1>&2
      return 4
   fi
   echo "Max Count $maxCount"
   while read pname fsize xtape sblock fsn d1 
   do
      current=`expr $current + 1`
      echo "$current $maxCount" >${data}/.progressToOsm
      sourcepath=${data}/${fsn}
      osmpath=`echo $pname | \
         awk -F/ '{ printf "/pnfs/io/usr/zeus" ;
                    for(i=4;i<=NF;i++)printf "/%s",$i
                    printf "\n"
                  }'`
      echo "`date` Processing : ${sourcepath} -> ${osmpath}" 
      if [ -f ${data}/.done-${fsn} ] ; then
         echo "Already Done : ${pname}"
         continue ;
      fi
      if [ -f ${osmpath} ] ; then
         echo "Needs correction : ${pname}" 1>&2
         continue ;
      fi

      dirPath=`dirname ${osmpath}`
      if [ ! -d ${dirPath} ] ; then
         mkdir -p ${dirPath} 1>/dev/null 2>/dev/null
         if [ $? -ne 0 ] ; then
            echo "Can't create ${dirPath}" 1>&2
            continue
         fi
      fi
      
      ( osmcp -d -h localhost $sourcepath $osmpath
        rc=$?
        if [ $rc -ne 0 ] ; then
           echo "osmcp failed : $rc" 1>&2
        else        
          rm $sourcepath
          touch ${data}/.done-${fsn}
        fi
      )  >${data}/.log-${fsn}.log 2>&1 &
      pcounter=`expr $pcounter + 1`
      if [ $pcounter -gt 3 ] ; then
         echo "`date` going to wait for osmcp n=$pcounter"
         jobs
         wait
         echo "`date` all osmcp done"
         pcounter=0
      fi
   done <${tmpFile}
#
   if [ $pcounter -gt 0 ] ; then
      echo "`date` going to wait for osmcp n=$pcounter"
      jobs
      wait
      echo "`date` all osmcp done"
      pcounter=0
   fi
   rm -rf ${tmpFile} >/dev/null 2>/dev/null
   return 0
}
#------------------------------------------------------------------
#
currentStatus=${bigDisk}/.statusToOsm
currentTape=${bigDisk}/.currentToOsm
while : 
do
   echo "`date` Waiting for new request"
   echo "Waiting for new request" >$currentStatus
   tape=`ls $toOsmDir 2>/dev/null | head -n 1 2>/dev/null`
   while [ -z "${tape}" ]
   do 
       sleep 5 
       tape=`ls $toOsmDir 2>/dev/null | head -n 1 2>/dev/null`      
   done
   echo "`date` Processing tape $tape"
   echo ${tape} >$currentTape
   
   data=${bigDisk}/${tape}   
   if [ ! -d ${data} ] ; then
     echo "`date` Couldn't find ${data}"
     exit 7
   fi
   
   doneFlag=${data}/.allCopiedToOsm
   if [ -f ${doneFlag} ]  
   then
      echo "`date` Tape ${tape} already done"
   else
      echo "copy" >$currentStatus
      copyToOsm >>${data}/.toOsm.log 2>&1
      if [ $? -ne 0 ] ; then
         echo "`date` Processing tape $tape failed" 
         exit 6 ; 
      fi
      touch ${doneFlag}
   fi
   echo "cp $requestDir/$tape $requestDir/.X-$tape"
   echo "mv $requestDir/$tape $toOsmDir/$tape"
   cp $toOsmDir/$tape $toOsmDir/.X-$tape >/dev/null 2>&1
   mv $toOsmDir/$tape $doneDir/$tape >/dev/null 2>&1
   
done
echo "`date` Done"
exit 0
