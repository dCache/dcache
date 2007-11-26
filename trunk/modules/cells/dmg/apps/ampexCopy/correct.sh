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
##################################################################
#
#   the 'from ampex process'
#
sizeOf() {
   s=`ls -l $1 | awk '{ print $5 }' 2>/dev/null`
   if [ -z "$s" ] ; then return 1 ; fi
   echo $s
   return 0
}
checkAmpex() {
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
   #
   totalCount=`wc $desc | awk '{ print $1 }' 2>/dev/null`
   if [ -z "$totalCount" ] ; then
     echo "Can't determine total file count" 1>&2
     exit 2
   fi
   echo "Total files to check : $totalCount"
   #
   current=0
   cfsn=1
   cat $desc | while read pname fsize xtape sblock fsn d1 ; 
   do
   #
   #     get the already used space ( may decide to wait .. )
   #
#      echo "Working on : $pname "
      echo $pname $fsize $xtape $sblock $fsn $d1 >>$data/.workingOn
      fname=$fsn
         sourcepath=${data}/${fsn}
         osmpath=`echo $pname | \
         awk -F/ '{ printf "/pnfs/io/usr/zeus" ;
                    for(i=4;i<=NF;i++)printf "/%s",$i
                    printf "\n"
                  }'`
#         echo "Processing : ${sourcepath} -> ${osmpath}" 
         if [ -f ${osmpath} ] ; then
            s2=`sizeOf $osmpath`
            
            if [ "$s2" -eq 0 ] ; then
               echo "NULL ${fsn} ${osmpath}"
            elif [ "$s2" -ne "$fsize" ] ; then
               echo "BAD  ${fsn} ${fname}"
            else
               echo "OK   ${fsn} ${s1} ${osmpath}"
               if [ ! -f ${data}/.${fname} ] ; then
                   echo $pname $fsize $xtape $sblock $fsn $d1 > ${data}/.${fname}
               fi
               if [ ! -f ${data}/.done-${fsn} ] ; then
                 touch ${data}/.done-${fsn}
               fi
            fi
         elif [ -f ${sourcepath} ] ; then
            s2=`sizeOf $sourcepath`
            if [ "$s2" -eq 0 ] ; then
               echo "DISK NULL ${fsn} ${fname}"
            elif [ "$s2" -ne "$fsize" ] ; then
               echo "DISK BAD  ${fsn} ${fname}"
            else
               echo "DISK OK   ${fsn} ${s1} ${osmpath}"
               if [ ! -f ${data}/.${fname} ] ; then
                   echo $pname $fsize $xtape $sblock $fsn $d1 > ${data}/.${fname}
               fi
            fi
         else
            echo "MIS  ${fsn} ${osmpath}"
         fi
      # increment cfsn - cause we read this file
      current=`expr $current + 1`
   done
   return 0
}
#------------------------------------------------------------------
#
if [ $# -ne 1 ] ; then
  echo "Usage : ... <tapeId>"
  exit 5
fi
tape=$1
echo "`date` Processing tape $tape"
data=${bigDisk}/${tape}
mkdir ${data} 1>/dev/null 2>/dev/null 
if [ ! -d ${data} ] ; then
  echo "`date` Couldn't create ${data}"
  panic 41 "Couldn't create" "${data}"
  exit 7
fi
checkAmpex $tape
