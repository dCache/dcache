#!/bin/sh
#
#set -x
LOGFILE=/tmp/hsmcp.log
usage() {
   echo "Usage : put|get <pnfsId> <filePath> [-si=<storageInfo>] [-key[=value] ...]" 1>&2
}
if [ $# -lt 3 ] ; then 
   usage
   exit 4
fi
problem() {
   echo "$1 ($2)" >>${LOGFILE}
   echo "$1 ($2)" >&2
   exit $2
}
echo "$* `date`" >>${LOGFILE}
echo "dCache as HSM version" >>${LOGFILE}
#
#########################################################
#
# split the arguments into the options -<key>=<value> and the 
# positional arguments.
#
args=""
while [ $# -gt 0 ] ; do
  if expr "$1" : "-.*" >/dev/null ; then
     a=`expr "$1" : "-\(.*\)" 2>/dev/null`
     key=`echo "$a" | awk -F= '{print $1}' 2>/dev/null`
     value=`echo "$a" | awk -F= '{for(i=2;i<NF;i++)x=x $i "=" ; x=x $NF ; print x }' 2>/dev/null`
     if [ -z "$value" ] ; then a="${key}=" ; fi
     eval "${key}=\"${value}\""
     a="export ${key}"
     eval "$a"
  else
     args="${args} $1"
  fi
  shift 1
done
if [ ! -z "$args" ] ; then
   set `echo "$args" | awk '{ for(i=1;i<=NF;i++)print $i }'`
fi
if [ $# -lt 3 ] ; then
   usage
   exit 4
fi
#
#########################################################
#
#         get our location
#
PRG=`type -p $0` >/dev/null 2>&1
while [ -L "$PRG" ]
do
    newprg=`expr "\`/bin/ls -l "$PRG"\`" : ".*$PRG -> \(.*\)"`
    expr "$newprg" : / >/dev/null || newprg="`dirname $PRG`/$newprg"
    PRG="$newprg"
done
#
thisDir=`dirname $PRG`
weAre=`basename $0`
#
#
#########################################################
#
#    find dccp
#
if [ ! -z "${dccp}" ] ; then
   DCCP=${dccp}
else
   DCCP=dccp
   PATH=${thisDir}:${thisDir}/../dcap/bin:$PATH
fi
#
which ${DCCP} >/dev/null 2>/dev/null
[ $? -ne 0 ] && problem "DCCP not found or not operational : ${DCCP}" 4
#
#
command=$1
pnfsid=$2
filename=$3
#
#########################################################
#
#    for testing error codes only. 
#
if [ ! -z "$errorNumber" ] ; then
   case "${errorNumber}" in
     41) echo "No space left on device" 1>&2
         exit 41 ;;
     42) echo "OSM disk read IOError" 1>&2
         exit 42 ;;
     43) echo "OSM disk write IOError" 1>&2
         exit 43 ;;
   esac
   problem "ErrorNumber-${errorNumber}"  ${errorNumber}
fi
#
#     end of init
#
#  extract the options
#    -si=<storageInfos>
#    -hsmBase=<hsmBase>
#    -waitTime=<queueingTime>
#    -pnfs=<mountpoint>
#    -hsmDoor=<hsm server door address> #   dcache.desy.de:22125
#
#######################################################################
#
#  do we know where pnfs is and is it really a pnfs filesystem ?
#
[ -z "${pnfs}" ] && problem "Variable 'pnfs' not defined but needed" 7
#
cat "${pnfs}/.(const)(xyzOtto9813724)" >/dev/null 2>/dev/null 
[ $? -ne 0 ] &&  problem "Seems not to be a pnfsFilesystem : ${pnfs}"  8
#
#######################################################################
#
#           get get the server door
#
[ -z "${serverDoor}" ] && problem "variable 'serverDoor' not specified" 12
#
#######################################################################
#
#           get storage info
#
if [ \( -z "${si}" \) -a \( "${command}" = "put" \) ] ; then
   problem "StorageInfo (-si=...) needed for 'put'"  9
fi
storageInfo=${si}
#
#
pnfsfile=$BASE/$pnfsid
if [ $command = "get" ] ; then
   #
   [ ! -z "${waitTime}" ] && sleep ${waitTime}
   #
   ${DCCP} -X-hsm  pnfs://${serverDoor}/${pnfsid} ${filename} >>${LOGFILE} >&1 
   [ $? -ne 0 ] && problem "Command failed : ${DCCP} -X-hsm  pnfs://${serverDoor}/${pnfsid} ${filename}" 14
#
   echo "Finished ${pnfsid}" >>${LOGFILE}
   exit 0
#
elif [ $command = "put" ] ; then
#
   [ ! -z "${waitTime}" ] && sleep ${waitTime}
#
   ${DCCP} -X-hsm  ${filename} pnfs://${serverDoor}/${pnfsid} >>${LOGFILE} >&1 
   [ $? -ne 0 ] && problem "Command failed : ${DCCP} -X-hsm  ${filename} pnfs://${serverDoor}/${pnfsid}" 15
#
   parentId=`cat "${pnfs}/.(parent)($pnfsid)"`
   osmtId=`awk '{ if( $2 == "OSMTemplate" )print $1 }' "${pnfs}/.(ptags)($parentId)"`
   store=`awk '{ if( $1 == "StoreName" )print $2 }' "${pnfs}/.(access)($osmtId)"`
   osmgroupId=`awk '{ if( $2 == "sGroup" )print $1 }' "${pnfs}/.(ptags)($parentId)"`
   group=`cat "${pnfs}/.(access)($osmgroupId)"`
   echo "${store} ${group} bfid.${pnfsid}" >"${pnfs}/.(access)($pnfsid)(1)" 2>/dev/null ;
   if [ $? -ne 0 ] ; then
      rm -rf $BASE/$pnfsid >/dev/null 2>/dev/null
      problem "Couldn't write storageInfo back into pnfs"  16
   fi
#
   echo "Finished ${pnfsid}" >>${LOGFILE}
   exit 0
elif [ $command = "next" ] ; then
   echo 0
else 
   echo "Illegal command $command" 1>&2
   exit 4
fi
exit 0
