#!/bin/sh
#
#set -x
#
#	
#
#   wrapper script to wrap eurogate client using the standard dCache HSM shellscript interface
#   
#
#    dCache configuration
#
#   hsm set osm -command=<fullPathToThisScript>	(e.g. -command=/opt/d-cache/jobs/euwrapper.sh )
#   hsm set osm -eudir=<fullPathToEurogateHome>     (e.g -eudir=/opt/d-cache/eurogate )
#   hsm set osm -eudoor=<EurogateDoor:Port>	(e.g. -eudoor=reagan.desy.de:28000 )
#   hsm set osm -pnfs=<fullPathToPnfsMountpoint>  (e.g. -pnfs=/pnfs/desy.de )
#
#   prerequisits
#
LOG=/tmp/hsmio.log
DEVTTY=$LOG
AWK=gawk
OSMCP=cp
#
#   get the dcache base directory 
#   (assumes that we are called with full path)
#
dir=`dirname $0`
DCACHE_HOME=`dirname ${dir}`
#
#  some help functions
#
usage() {
   echo "Usage : put|get <pnfsId> <filePath> [-si=<storageInfo>] [-key[=value] ...]" | tee -a $LOG >&2
}
problem() {
   echo "($$) $2 ($1)" | tee -a $LOG >&2
   exit $1
}
errorReport() {
   echo "($$) $1" | tee -a ${LOG} >&2
   return 0
}
#
#  say hallo to people
#
echo "$* `date`" >>${LOG}
#
#   have we been called correctly ?
#
if [ $# -lt 3 ] ; then 
   usage
   exit 4
fi
#
#   Something wrong, we didn't find our base directory
#
[ ! -d "${DCACHE_HOME}" ] && problem 1 "DCACHE root not found ${DCACHE_HOME}"
#
#
##################################################################################
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
##################################################################################
#
# assign the manditory arguments
#
command=$1
pnfsid=$2
filename=$3
#
#
########################################################
#
# check for some basic variables
#
#[ -z "${hsmBase}" ] && problem 3 "Variable 'hsmBase' not defined"
#
#[ ! -d "${hsmBase}" ]  && problem 4 "hsmBase=${hsmBase} : not a directory"
#
# make the storage info variables available
#
[ -z "${si}" ] && problem 1 "StorageInfo (-si=...) not available" 
#
#
getStorageInfoKey() {

   echo $si | 
   $AWK -v key=$1 -F';' '{
             for(i=1;i<=NF;i++){
                split($i,a,"=") ;
                if(a[1]==key)print a[2]
             }
   }'
}
#
#  construct the actual file path from 'hsmBase, store ,group and bfid'
#
#  createFilePath 'hsmBase' 'store' 'group' 'pnfsId'
#
createBfid() {

    xstore=$1
    xgroup=$2
    xpnfsId=$3
    
    xhelp=`echo ${xpnfsId} | awk '{ print substr( $1 , 23 ) }'`
    
    echo ${xstore}/${xgroup}/${xhelp}/${xpnfsId}
    
    return 0;

}
createFilePath() {

    zhsmBase=$1
    zbfid=$2
        
    zfilebase=`dirname ${zhsmBase}/${zbfid}`
    
    [ ! -d "${fzilebase}" ] && mkdir -p ${zfilebase} 2>/dev/null
    errno=$?
    if [ ${errno} -ne 0 ] ; then
       errorReport "(mkdir) : Couldn't create ${zfilebase}" 
       return ${errno}
    fi    
    echo ${zfilebase}
    return 0;

}
#     end of init
#
########################################################
#
#                    check for pnfs
#
needpnfs=false
#
#
#   make sure we get pnfs for 'put'
#
[ ${command} = "put" ] && needpnfs=true
#
# if pnfs is required, we need to make some checks
# and we have to calculate the parent 'pnfsid' and the
# so called pnfsFile.
#
if [ "${needpnfs}" = "true" ] ; then

   [ -z "${pnfs}" ]  && problem 5 "Need 'pnfs' , the pnfsMountpoint "
   
   cat "${pnfs}/.(const)(452kl345)" >/dev/null 2>/dev/null 
   [ $? -ne 0 ] && problem 6 "Seems not to be a pnfsFilesystem : ${pnfs}"
   
   parentId=`cat "${pnfs}/.(parent)($pnfsid)"`
   [ $? -ne 0 ] && problem 32 "PnfsFile disappeared $pnfsid"
   #
   #   access pnfsfile through Pnfsid
   #
   pnfsfile="${pnfs}/.(access)(${pnfsid})"
   
fi
#
readTag() {
   tagid=`awk -v key=$1 '{ if( $2 == key )print $1 }' "${pnfs}/.(ptags)($parentId)" 2>/dev/null`
   [ -z "$tagid" ] && return 1
   cat "${pnfs}/.(access)(tagid)"
   return 0
}
#
#########################################################
#
#   osm specific variables
#
store=`getStorageInfoKey store 2>/dev/null`
group=`getStorageInfoKey group 2>/dev/null`
bfid=`getStorageInfoKey bfid 2>/dev/null`
#
#########################################################
#
datasetPut() {
 
ygroup=${1}
yfilename=${2}

#    echo "execute put : ${euclient} write ${yfilename} ${ygroup}" 1>&2
#    ybfid=`${euclient} write ${yfilename} ${ygroup}`

#    echo "hallo:ballo execute put : ${euclient} write ${yfilename} all" 1>&2
    ybfid=`${euclient} write ${yfilename} all`


   if [ $? -ne 0 ] ; then
      errorReport "Problem in writing ${filename} ${group}"
      #
      #  this seems to be a serious problem, so we return 41
      #  which will disable to the pool.
      #
      return 41
   fi   

   echo ${ybfid}
   return 0 
}
setFilesize() {
   localFilesizeFile="${pnfs}/.(pset)($pnfsid)(size)($1)"
   touch ${localFilesizeFile} 2>>$LOG
   if [ $? -ne 0 ] ; then
      errorReport "Couldn't set filesize (${$1}) of ${pnfsid}" 
      return 35
   fi
   return 0
}
#
#
# storeStorageInfo group bfid [level]
#
storeStorageInfo() {
   #
   #
   wstore=$1
   wgroup=$2
   wbfid=$3

   echo "${wstore} ${wgroup} ${wbfid}" >"${pnfsfile}(1)" 2>/dev/null
   if [ $? -ne 0 ] ; then
      errorReport "Couldn't write storageInfo back into pnfs ${pnfsid} '${wstore} ${wgroup} ${wbfid}'"
      return 34
   fi
   
   return 0
}
#
#
#
#
# test for optional key '-eudir'
[ -z "${eudir}" ] && problem 3 "key 'eudir' not set"


#
#
# test for optional key '-eudoor'
[ -z "${eudoor}" ] && problem 4 "key 'eudoor' not set"


# test for java
java -version 2> /dev/null
[ $? -ne 0 ] &&  problem 5 "no java in classpath"


# test for the eurogate client classes
euclassesdir=$eudir/classes
[ ! -f $euclassesdir/cells.jar -a ! -f $euclassesdir/eurogate.jar ] && problem 6 "cells.jar and/or eurogate.jar not in$euclassesdir"

# the client execution command
euclient="java -cp ${euclassesdir}/cells.jar:${euclassesdir}/eurogate.jar eurogate.gate.EuroSyncClient"

# this variable must be accessable by the eurogate-application
export DOORADDR=${eudoor}
#
#
if [ -z "${waitTime}" ] ; then waitTime=1 ; fi
#
#
#
if [ $command = "get" ] ; then
#
    [ \( "${needpnfs}" = "true" \) -a \( ! -f "${pnfsfile}" \) ] && \
      problem 34 "pnfsid not found : $pnfsid" 
#
#   we don't need pnfs, all parameters are coming
#   with '-si=...'
#  
   [ \( -z "${store}" \) -o \( -z "${group}" \) -o \( -z "${bfid}" \) ] && \
      problem 22 "couldn't get sufficient info for 'copy' : store=>${store}< group=>${group}< bfid=>${bfid}<" 
   
   #
   if [ "$group" = "${testFailedGroup}" ] ; then
     errorReport "Test group triggered ${testFailedGroup}"
     [ -z "${testFailedErrno}" ] && testFailedErrno=4
     exit ${testFailedErrno}
   fi


   echo "execute get: ${euclient} read ${bfid} ${filename}" 1>&2
   ${euclient} read ${bfid} ${filename} 1>&2
   if [ $? -ne 0 ] ; then
      errorReport "Copy back failed : ${euclient} read ${bfid} ${filename}"
      rm -rf ${filename} 2>/dev/null
   fi
   exit 0

#   cp ${hsmBase}/${bfid} ${filename}
#   if [ $? -ne 0 ] ; then
#      errorReport "Copy back failed : ${hsmBase}/${bfid} ${filename}"
#      rm -rf ${filename} 2>/dev/null
#   fi
#   exit 0
   
elif [ $command = "put" ] ; then
#
#   and the put
#
   filesize=`ls -l ${filename} 2>/dev/null | awk '{ print $5 }'`
   #
   #  check for existence of file
   #  NOTE : if the filesize is zero, we are expected to return 31, so that
   #         dcache can react accordingly.
   #
   [ -z "${filesize}" ] && problem 31 "File not found : ${filename}"
   [ "${filesize}" = "0" ] && problem 31 "Filesize is zero (${filename})" 
   #
   #
   #  now, finally copy the file to the HSM
   #  (we assume the bfid to be returned)
   #
   bfid=`datasetPut ${group} ${filename}` || exit $?
   #
   #  and now store 'store group' and bfid into level I
   #
   
   storeStorageInfo ${store} ${group} ${bfid} 
   errno=$?
   #
   if [ $errno -ne 0 ] ; then
      
      # get rid of the hsm file if we can't register the copy into pnfs
      #
      echo "could not store storageInfo, removing file from eurogate: ${euclient} remove ${bfid}" 1>&2
      ${euclient} remove ${bfid} 1>&2

      exit ${errno}
  
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
