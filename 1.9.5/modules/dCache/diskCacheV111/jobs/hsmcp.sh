#!/bin/sh
#
#set -x
usage() {
   echo "Usage : put|get <pnfsId> <filePath> [-si=<storageInfo>] [-key[=value] ...]" 1>&2
}
if [ $# -lt 3 ] ; then 
   usage
   exit 4
fi
echo "$* `date`" >>/tmp/hsm.log
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
command=$1
pnfsid=$2
filename=$3
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
   echo "ErrorNumber-${errorNumber}" 1>&2
   exit $errorNumber 
fi
#
#     end of init
#
#  extract the options
#    -si=<storageInfos>
#    -hsmBase=<hsmBase>
#    -waitTime=<queueingTime>
#    -pnfs=<mountpoint>
#
if [ -z "${hsmBase}" ] ; then
   echo "Need 'hsmBase' ... " 1>&2
   exit 2
fi
if [ -z "${pnfs}" ] ; then
   echo "Need 'pnfs' , the pnfsMountpoint " 1>&2
   exit 5
fi
cat "${pnfs}/.(const)(xyzOtto9813724)" >/dev/null 2>/dev/null 
if [ $? -ne 0 ] ; then
   echo "Seems not to be a pnfsFilesystem : ${pnfs}" 1>&2
   exit 6
fi
if [ \( -z "${si}" \) -a \( "${command}" = "put" \) ] ; then
   echo "StorageInfo (-si=...) needed for 'put'" 1>&2
   exit 1
fi
storageInfo=${si}
#
if [ -z ${waitTime} ] ; then waitTime=1 ; fi
#
BASE=${hsmBase}/data
if [ ! -d "$BASE" ] ; then
   echo "Not a directory : ${BASE}" 1>&2
   exit 5
fi
#
#
pnfsfile=$BASE/$pnfsid
if [ $command = "get" ] ; then
   if [ ! -f $pnfsfile ] ; then
       echo "pnfsid not found : $2" 1>&2
       exit 4
   fi
   if [ ! -z "${hsmError}" ] ; then exit ${hsmError} ; fi
   sleep ${waitTime} 
   cp $BASE/$pnfsid $filename 1>/dev/null 2>/dev/null
   if [ $? -ne 0 ] ; then
      echo "Failed : cp $BASE/$pnfsid $filename"
      exit 5
   fi
   exit 0
elif [ $command = "put" ] ; then
   if [ -f "${pnfsfile}" ] ; then
       echo "pnfsid already exists : ${pnfsid}" 1>&2
       exit 4
   fi
   touch ${BASE}/${pnfsid}
   sleep ${waitTime}
   cp  $filename $BASE/$pnfsid 1>/dev/null 2>/dev/null
   if [ $? -ne 0 ] ; then
      echo "Failed : cp $filename $BASE/$pnfsid"
      exit 5
   fi
   parentId=`cat "${pnfs}/.(parent)($pnfsid)"`
   osmtId=`awk '{ if( $2 == "OSMTemplate" )print $1 }' "${pnfs}/.(ptags)($parentId)"`
   store=`awk '{ if( $1 == "StoreName" )print $2 }' "${pnfs}/.(access)($osmtId)"`
   osmgroupId=`awk '{ if( $2 == "sGroup" )print $1 }' "${pnfs}/.(ptags)($parentId)"`
   group=`cat "${pnfs}/.(access)($osmgroupId)"`
   echo "${store} ${group} bfid.${pnfsid}" >"${pnfs}/.(access)($pnfsid)(1)" 2>/dev/null ;
   if [ $? -ne 0 ] ; then
      echo "Couldn't write storageInfo back into pnfs" 1>&2
      rm -rf $BASE/$pnfsid >/dev/null 2>/dev/null
      exit 7
   fi
   exit 0
elif [ $command = "next" ] ; then
   echo 0
else 
   echo "Illegal command $command" 1>&2
   exit 4
fi
exit 0
