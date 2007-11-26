#!/bin/sh
#
#set -x
this=`dirname $0`
#
#OSMCP=/usr/sbin/osmcp
#ROSMCP=$this/remote-osmcp.sh
OSMCP=$this/remote-osmcp.sh
RTMP=/copyDisk/dCache/transfer
LOG=/tmp/rosmcp.log
RHOST=oho-test.desy.de
#
usage() {
   echo "Usage : put|get <pnfsId> <filePath> [-si=<storageInfo>] [-key[=value] ...]" 1>&2
}
if [ $# -lt 3 ] ; then 
   usage
   exit 4
fi
echo "$* `date`" >>${LOG}
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
rfilename=$RTMP/$pnfsid
#
#     end of init
#
#  extract the options
#    -si=<storageInfos>
#    -pnfs=<mountpoint>
#    -waitTime=<queueingTime>
#
if [ -z "${pnfs}" ] ; then
   echo "Need 'pnfs' , the pnfsMountpoint " 1>&2
   exit 5
fi
cat "${pnfs}/.(const)(`date`)" >/dev/null 2>/dev/null 
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
if [ -z "${waitTime}" ] ; then waitTime=1 ; fi
#
#
#   access pnfsfile through Pnfsid
#
pnfsfile="${pnfs}/.(access)(${pnfsid})"
#
if [ $command = "get" ] ; then
   if [ ! -f ${pnfsfile} ] ; then
       echo "pnfsid not found : $pnfsid" 1>&2
       exit 4
   fi
   level1=`cat "${pnfsfile}(1)" 2>/dev/null`
   store=`echo "${level1}" | awk '{ print $1 }'`
   group=`echo "${level1}" 2>/dev/null | awk '{ print $2 }'`
   bfid=`echo "${level1}" 2>/dev/null | awk '{ print $3 }'`
   if [ "$group" = "${testFailedGroup}" ] ; then
     echo "Test group triggered ${testFailedGroup}" 1>&2
     if [ -z "${testFailedErrno}" ] ; then testFailedErrno=4 ; fi
     exit ${testFailedErrno}
   fi
#
   if [ \( -z "${store}" \) -o \( -z "${group}" \) -o \( -z "${bfid}" \) ] ; then
      echo "Invalid level-1 content : >${level1}<" 1>&2
      exit 14 
   fi 
   echo "$OSMCP -v -S ${store} -B ${bfid} $rfilename" >>${LOG}
   $OSMCP -v  -S ${store} -B ${bfid} $rfilename 1>>${LOG} 2>&1
   if [ $? -ne 0 ] ; then
      echo "Failed : $OSMCP -d -S ${store} -B ${bfid} $rfilename" 1>&2
      echo "Failed : $OSMCP -d -S ${store} -B ${bfid} $rfilename" >>${LOG}
      exit 5
   fi
   echo "Remove osm successful ... "
   echo "/usr/bin/scp $RHOST:$rfilename  $filename" >>$LOG
   /usr/bin/rcp $RHOST:$rfilename  $filename >>$LOG 2>&1
   if [ $? -ne 0 ] ; then
      echo "Failed : /usr/bin/rcp $RHOST:$rfilename  $filename" 1>&2
      echo "Failed : /usr/bin/rcp $RHOST:$rfilename  $filename" >>$LOG
      /usr/bin/ssh $RHOST "rm $rfilename" >>$LOG
      exit 5
   fi
   /usr/bin/ssh $RHOST "rm $rfilename" >>$LOG
   exit 0
elif [ $command = "put" ] ; then
#
    echo "remove put not supported" >&2
    exit 3
#   and the put
#
   filesize=`ls -l ${filename} 2>/dev/null | awk '{ print $5 }'`
   if [ -z "${filesize}" ] ; then
     echo "File not found : ${filename}" 1>&2
     echo "File not found : ${filename}" >>$LOG
     exit 5
   elif [ "${filesize}" = "0" ] ; then
     echo "Filesize is zero (${filename})" 1>&2
     echo "Filesize is zero (${filename})" >>$LOG
     exit 6
   fi
   parentId=`cat "${pnfs}/.(parent)($pnfsid)"`
   osmtId=`awk '{ if( $2 == "OSMTemplate" )print $1 }' "${pnfs}/.(ptags)($parentId)"`
   store=`awk '{ if( $1 == "StoreName" )print $2 }' "${pnfs}/.(access)($osmtId)"`
   osmgroupId=`awk '{ if( $2 == "sGroup" )print $1 }' "${pnfs}/.(ptags)($parentId)"`
   group=`cat "${pnfs}/.(access)($osmgroupId)"`
   echo "$OSMCP -v  -S ${store} -G ${group} ${filename}" >>${LOG}
   bfid=`$OSMCP -v  -S ${store} -G ${group} ${filename} 2>>${LOG}`
   if [ $? -ne 0 ] ; then
      echo "Failed : $OSMCP -v -S ${store} -G ${group} ${filename}" 1>&2
      exit 6
   fi
   echo "${store} ${group} ${bfid}" >"${pnfsfile}(1)" 2>/dev/null ;
   if [ $? -ne 0 ] ; then
      echo "Couldn't write storageInfo back into pnfs '${store} ${group} ${bfid}'" 1>&2
      echo "Couldn't write storageInfo back into pnfs '${store} ${group} ${bfid}'" >>${LOG}
      exit 7
   fi
   setfilesize="${pnfs}/.(pset)($pnfsid)(size)($filesize)"
   touch ${setfilesize} 2>>$LOG
   if [ $? -ne 0 ] ; then
      echo "Couldn't set filesize of ${setfilesize}" 2>>$LOG
   fi
   exit 0
elif [ $command = "next" ] ; then
   echo "'next' operation not supported by this HSM" 1>&2
   exit 10 
else 
   echo "Illegal command $command" 1>&2
   exit 4
fi
exit 0
