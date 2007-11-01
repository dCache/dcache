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
##########################################################
#
#       THE DISPLAY CONTROL PART
#
#
getDisplayLock() {
#   echo "Trying to get the Display LOCK"
   return 
   while : 
   do
      rm "$flagDir/displayLock" >/dev/null 2>&1
      if [ $? -eq 0 ]  
      then
         break
      fi
      sleep 2
   done
#   echo "Got the Display LOCK"
}
releaseDisplayLock() {
   touch "$flagDir/displayLock"
}
waitForOkButton() {
   while [ -f "$flagDir/OkButton" ]
   do
      sleep 2
   done
   return 0
}
askForTape() {
   getDisplayLock
   touch $flagDir/OkButton
   cat >$flagDir/messages <<!
Tape Is Ready
We need a new tape
150,0,0=Please insert tape
250,0,0=$1
!
   waitForOkButton
   rm $flagDir/messages
   releaseDisplayLock
}
doDisplay() {
   getDisplayLock
   cat >$flagDir/messages <<!
*
$1
150,0,0=$1
!
}
#
#
##########################################################
panic() {
#------------ <#> <line1> <line2>
   getDisplayLock
   touch $flagDir/OkButton
   cat >$flagDir/messages <<!
*
Fuhrmann anrufen 4474 ( oder 01713383076 )
255,0,0=Panic $1
255,0,0=$2
255,0,0=$3
!
   waitForOkButton
   rm $flagDir/messages
   releaseDisplayLock
}
####################################################
#
#
deviceReady() {
   msg=`mt -f $device 2>&1 | grep "Media : Not READY"`
   if [ -z "$msg" ] ; then return 1 ; else return 0 ; fi
}
waitForDeviceReady() {

   while :
   do
      deviceReady || break
      sleep 5
   done
   return 0
}
testTest() {
   if [ $# -ne 2 ] 
   then
      echo "Usage : <tape> <drive>"
      exit 4
   fi
   while :
   do
      askForTape $1 $2
      #
      # now we can work
      #
      n=0
      while [ $n -lt 20 ]
      do
         l=`expr $n \* 5`
         echo "$2=$l" >"$flagDir/progress.$2"
         date > "$flagDir/progress"
         n=`expr $n + 1`
         echo $n
         sleep 1
      done
   done
}
processTape() {
   echo "Processing tape $1"
   dt=$1
   tapeFile=$2
   n=0
   flagFile="$flagDir/progress.$dt"
   while [ $n -lt 20 ]
   do
      l=`expr $n \* 5`
#      echo "$dt=$l" >${flagFile}
      date > "$flagDir/progress"
      n=`expr $n + 1`
      echo $n
      sleep 1
   done
   rm ${flagFile}
   return 0
}
updateProgress() {
   if [ -z "$1" ] ;  then filled=0 ; else filled=$1 ; fi
   echo "x=$filled" >${flagDir}/progress.tape
   touch ${flagDir}/progress
   echo "$current $totalCount" >$currentProgress
}
unloadDevice() {
   mt -f $device unload
   return 0
}
#
#################################################################
#
#   the 'from ampex process'
#
copyFromAmpex() {
   tapeName=$1
   echo "----------------------------------"
   date 
   echo ""
   banner "  ${tapeName}"
   desc=${acopyHome}/zeus/tapes/${tapeName}
   if [ ! -f $desc ] ; then
     echo "Desciption file not found : $desc" 2>&1
     exit 4
   fi
   #
   #
   echo "Rewinding ... ${tapeName}"
   mt -f $device rewind
   if [ $? -ne 0 ]
   then
      echo "Initial rewind failed" 1>&2
      return 9
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
   updateProgress
   cfsn=1
   cat $desc | while read pname fsize xtape sblock fsn d1 ; 
   do
   #
   #     get the already used space ( may decide to wait .. )
   #
      echo "Working on : $pname "
      echo $pname $fsize $xtape $sblock $fsn $d1 >>$data/.workingOn
      fname=$fsn
      if [ -f ${data}/.${fname} ]  
      then

         echo "Already done : $fname"

      else
         skip=`expr $fsn - $cfsn`
         if [ "$skip" -lt 0 ] ; then
            echo "Description file not in sequence : $desc"
            unloadDevice
            exit 5
         fi
         echo "Skipping $skip ... "
         mt -f $device fsf $skip
         if [ $? -ne 0 ]; then
	      echo "skip to file $fsn ($skip) failed"
              unloadDevice
	      return 5 ;
         fi
         echo "Skipping Done"
         cfsn=$fsn
         echo "Reading .. $fname"
         dd if=$device ibs=$bsize of=${data}/${fname}-x
         if [ $? -ne 0 ]; then
             echo "reading file $fname at $fsn failed"
             unloadDevice
             return 6 ;
         fi
         cfsn=`expr $cfsn + 1`
         echo "Reading Done"
         echo "Cutting to $fsize bytes"
#         java dmg.apps.ampexCopy.cut ${data}/${fname}-x ${data}/${fname} $fsize
         ${binDir}/truncate ${data}/${fname}-x $fsize
         if [ $? -ne 0 ] ; then 
            echo "Problem while cutting file" 1>&2
            unloadDevice
            return 4 ; 
         fi
         mv ${data}/${fname}-x ${data}/${fname}
         echo "Cutting Done"
         echo $pname $fsize $xtape $sblock $fsn $d1 > ${data}/.${fname}
         ls -l ${data}/$fname
      fi
      # increment cfsn - cause we read this file
      current=`expr $current + 1`
      rel=`expr $current \* 100`
      rel=`expr $rel \/ $totalCount`
      updateProgress $rel
   done
   rm ${flagDir}/progress* >/dev/null 2>/dev/null
   unloadDevice
   return 0
}
#------------------------------------------------------------------
#
status=${bigDisk}/.statusFromAmpex
currentTape=${bigDisk}/.currentFromAmpex
currentProgress=/dev/null
while : 
do
   echo "`date` Waiting for new request"
   echo "-" >$currentTape 
   echo "Waiting for new Request" >$status
   tape=`ls $requestDir 2>/dev/null | head -n 1 2>/dev/null`
   while [ -z "${tape}" ]
   do 
       sleep 5 
       tape=`ls $requestDir 2>/dev/null | head -n 1 2>/dev/null`      
   done
   echo $tape >$currentTape
   echo "`date` Processing tape $tape"
   data=${bigDisk}/${tape}
   currentProgress=${data}/.progressFromAmpex
   echo "0 0" >$currentProgress
   mkdir ${data} 1>/dev/null 2>/dev/null 
   if [ ! -d ${data} ] ; then
     echo "`date` Couldn't create ${data}"
     panic 41 "Couldn't create" "${data}"
     exit 7
   fi
   doneFlag=${data}/.allDone
   if [ -f ${doneFlag} ]  
   then
      echo "`date` Tape ${tape} already done"
   else
      echo "Waiting for tape" >$status
      echo "`date` Waiting for tape"
      askForTape $tape Drive
      echo "Waiting for device Ready" >$status
      echo "`date` Waiting for device Ready"
      waitForDeviceReady
      echo "`date` Ok, device is ready"
      echo "`date` Waiting for space"
      echo "Waiting for space" >$status
      while : 
      do
         usedSpace=`du -ks ${bigDisk} 2>/dev/null | \
                    awk '{ printf "%d\n",$1/1024/1024 }' 2>/dev/null`
         if [ "$usedSpace" -lt 75 ] ; then break ; fi
      done
      echo "`date` Used space : $usedSpace KBytes"
      echo "Go" >$status
      copyFromAmpex ${tape} >>${data}/.fromAmpex.log 2>&1
      if [ $? -ne 0 ] ; then
         echo "Processing tape $tape failed" 1>&2
         continue ; 
      fi
      touch ${doneFlag}
   fi
   echo "cp $requestDir/$tape $requestDir/.X-$tape"
   echo "mv $requestDir/$tape $toOsmDir/$tape"
   cp $requestDir/$tape $requestDir/.X-$tape >/dev/null 2>&1
   mv $requestDir/$tape $toOsmDir/$tape >/dev/null 2>&1
   
done
echo "Done"
doDisplay "Done"
#panic 44 "widi widi wum" "wam"
