#!/bin/sh
#
#set -x
#
OSMCP=/usr/d-cache/jobs/osmcp
LOG=/tmp/osmcp.log
DEVTTY=$LOG
#DEVTTY=/dev/tty
#
AWK=/opt/sfw/bin/gawk
#
# LOG=${DEVTTY}
#   localBfid=`$OSMCP -v  -S ${store} -G ${localGroup} -P $pnfsid ${filename} 2>>${LOG}`
# OSMCP=ourosmcp

ourosmcp() {
  lstore=$3
  lgroup=$5
  lpnfsid=$7
  lfile=$8
  
  ourdir=/tmp/hsm/${lstore}/${lgroup}
  mkdir -p ${ourdir} 2>/dev/null
  cp $lfile ${ourdir}/${pnfsid}
  
  sleep 10 

#  [ ${lgroup} != "users-p-dup" ] && return 4
  echo "${lstore}.${lgroup}.${lpnfsid}"
  return 0
}
#
# osm error definitions for local disk IO errors
#
OSM_EIOSPACE=120
OSM_EIOREAD=242
OSM_EIOWRITE=243
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
########################################################
#
# assign the manditory arguments
#
if [ $# -lt 3 ] ; then
   usage
   exit 4
fi
command=$1
pnfsid=$2
filename=$3
#
x=`dirname ${filename}`
x=`dirname ${x}`
LOCK_DIR=${x}/locks
[ ! -d $LOCK_DIR ]  && mkdir $LOCK_DIR 2>/dev/null
CURRENT_LOCK=""
#
#
########################################################
#
# make the storage info variables available
#
if [ \-z "${si}" ] ; then
   echo "StorageInfo (-si=...) not available" 1>&2
   exit 1
fi
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
#     end of init
#
########################################################
#
# check for pnfs
#
needpnfs=false
#
#
#   make sure we get pnfs for 'put'
#
[ ${command} = "put" ] && needpnfs=true
#
# if pnfs is required, we need to make some checks
# and get have to calculate the parent 'pnfsid' and the
# so called pnfsFile.
#
if [ "${needpnfs}" = "true" ] ; then

   if [ -z "${pnfs}" ] ; then
      echo "Need 'pnfs' , the pnfsMountpoint " 1>&2
      exit 5
   fi
   
   cat "${pnfs}/.(const)(`date`)" >/dev/null 2>/dev/null 
   if [ $? -ne 0 ] ; then
      echo "Seems not to be a pnfsFilesystem : ${pnfs}" 1>&2
      exit 6
   fi
   
   parentId=`cat "${pnfs}/.(parent)($pnfsid)"`
   if [ $? -ne 0 ] ; then
      echo "PnfsFile disappeared $pnfsid" | tee -a $LOG >&2
      exit 32
   fi
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
   localGroup=$1
   echo "DEBUG $$ : $OSMCP -v  -S ${store} -G ${localGroup} -P $pnfsid ${filename}" >>${LOG}
   localBfid=`$OSMCP -v  -S ${store} -G ${localGroup} -P $pnfsid ${filename} 2>>${LOG}`
   osm_error=$?
   ourRc=0
   if [ ${osm_error} -ne 0 ] ; then
      case "${osm_error}" in
        ${OSM_EIOSPACE}) 
            echo "OSM : no space left on device" | tee -a $LOG >&2
            ourRc=41 ;;
        ${OSM_EIOREAD}) 
            echo "OSM : disk read IOError" | tee -a $LOG >&2
            ourRc=42 ;;
        ${OSM_EIOWRITE})
            echo "OSM : disk write IOError" | tee -a $LOG >&2
            ourRc=43 ;;
        *)
           echo "Failed : $OSMCP -v -S ${store} -G ${localGroup} ${filename} = ${osm_error}" | tee -a $LOG >&2
           ourRc=${osm_error}
        ;;
      esac
   fi
   echo "DEBUG $$ : Finished (rc=${ourRc}) : $OSMCP -v  -S ${store} -G ${localGroup} -P $pnfsid ${filename}" >>${LOG}
   [ $ourRc -eq 0 ] && echo "${localBfid}"
   return $ourRc 
}
setFilesize() {
   localFilesizeFile="${pnfs}/.(pset)($pnfsid)(size)($1)"
   touch ${localFilesizeFile} 2>>$LOG
   if [ $? -ne 0 ] ; then
      echo "Couldn't set filesize (${$1}) of ${pnfsid}" | tee -a $LOG >&2
      return 35
   fi
   return 0
}
getLock() {

   localPnfsid=$1
   [ "${NEED_LOCK}" != "true" ] && return 0
   echo "DEBUG $$ : Trying to get lock for ${localPnfsid}" >>${DEVTTY}
   if [ ! -d ${LOCK_DIR} ] ; then
      echo "Lock directory not found : ${LOCK_DIR}" >&2
      return 1
   fi
   CURRENT_LOCK=${LOCK_DIR}/${localPnfsid}
   while : ; do
      mkdir ${CURRENT_LOCK} 2>/dev/null
      [ $? -eq 0 ] && break  
      sleep 4 
   done
   echo "DEBUG $$ : got lock ${localPnfsid}" >>${DEVTTY}
   return 0

}
releaseLock() {

   [ "${NEED_LOCK}" != "true" ] && return 0

   [ -z "${CURRENT_LOCK}" ] && return 0
   
   echo "DEBUG $$ : releasing lock ${CURRENT_LOCK}" >>${DEVTTY}
   rmdir ${CURRENT_LOCK} 2>/dev/null
   return 0
}
setExit() {
   echo $1 >$LOCK_DIR/${pnfsid}.rc 2>/dev/null 
   echo "DEBUG $$ : SET EXIT : $1" >>${DEVTTY}
   exit $1
}
getExit() {
   [ ! -f $LOCK_DIR/${pnfsid}.rc ] && return 1
   cat $LOCK_DIR/${pnfsid}.rc
   rm -rf $LOCK_DIR/${pnfsid}.rc 2>/dev/null
   return 0
}
#
trap releaseLock 0
#
checkLevel() {
  $AWK '{ 
           a[NR]=$0 
       }END{ 
           if( NR == 0 ){
              print 3
           }else if( NR == 1 ){
              split(a[1],b) ;
              if( ( b[1] != "" ) &&
                  ( b[2] != "" ) &&
                  ( b[3] != "" ) &&
                  ( b[3] != "*" )    ){
                  
                  print 2   
              }else{
                  print 3
              }             
           }else{
              for( i = 1 ; i < 3 ; i++ ){
                 line[i]=0
                 split(a[i],b) ;
                 if( ( b[1] != "" ) &&
                     ( b[2] != "" ) &&
                     ( b[3] != "" ) &&
                     ( b[3] != "*" )   )line[i]=1   
              }
              if( ( line[1] == 0 ) &&
                  ( line[2] == 0 )    ){
                  
                 print 3
              }else if( line[1] == 0 ){
                 print 1
              }else if( line[2] == 0 ){
                 print 2
              }else{
                 print 0
              }            
           }
        }' "${pnfsfile}(1)"
  
  return $?
}
#
# storeStorageInfo group bfid [level]
#
storeStorageInfo() {
   #
   #
   localGroup=$1
   localBfid=$2
   localLevel=$3
   echo "DEBUG $$ : storeStorageInfo count=$# $0 >$localGroup< >$localBfid< >$localLevel<"
   if [ -z "${localLevel}" ] ; then
      echo "${store} ${localGroup} ${localBfid}" >"${pnfsfile}(1)" 2>/dev/null ;
      if [ $? -ne 0 ] ; then
         echo "Couldn't write storageInfo back into pnfs \
              ${pnfsid} '${store} ${localGroup} ${localBfid}'" >&2
          return 34
      fi
      return 0
   fi
   if [ \( "${localLevel}" = "2" \) -o  \( "${localLevel}" = "1" \) ] ; then
   
      getLock ${pnfsid} || return $?
      
      result=`$AWK '{ 
                       a[NR]=$0 
                   }END{ 
                       if( NR == 0 ){
                          if( line == "1" ){
                             print store,group,bfid
                          }else{
                             printf "%s * *\n%s %s %s\n",store,store,group,bfid
                          }
                       }else if( NR == 1 ){
                          if( line == "1" ){
                             print store,group,bfid
                          }else{
                             printf "%s\n%s %s %s\n",a[1],store,group,bfid
                          }                       
                       }else{
                          if( line == "1" ){
                             printf "%s %s %s\n%s\n",store,group,bfid,a[2]
                          }else{
                             printf "%s\n%s %s %s\n",a[1],store,group,bfid
                          }                       
                      }
                   }' "${pnfsfile}(1)" \
              line=${localLevel} store=${store} group=${localGroup} bfid=${localBfid}`

      if [ -z "${result}" ] ; then
         storeRc=1
      else
         storeRc=0
         echo "${result}" >"${pnfsfile}(1)"
         storeRc=$?
      fi
      
      releaseLock ${pnfsid}

      
      if [ $storeRc -ne 0 ] ; then
         echo "Problem storing storageinfo ${pnfsid}" >&2
         return 1
      fi

   else
      echo "ASSERTION : illegel level number for \
           ${pnfsid} : ${localLevel}"  >&2
      return 1
   fi
   
   return 0
}
#
#
#
if [ -z "${waitTime}" ] ; then waitTime=1 ; fi
#
#
#
if [ $command = "get" ] ; then
   if [ \( "${needpnfs}" = "true" \) -a \( ! -f "${pnfsfile}" \) ] ; then
       echo "pnfsid not found : $pnfsid" 1>&2
       exit 4
   fi
#
#   we don't need pnfs, all parameters are coming
#   with '-si=...'
#  
#   level1=`cat "${pnfsfile}(1)" 2>/dev/null`
#   store=`echo "${level1}" | awk '{ print $1 }'`
#   group=`echo "${level1}" 2>/dev/null | awk '{ print $2 }'`
#   bfid=`echo "${level1}" 2>/dev/null | awk '{ print $3 }'`

   if [ "$group" = "DUMMY" ] ; then
     echo "Dummy group used" >&2
     exit 0
   fi
   if [ "$group" = "${testFailedGroup}" ] ; then
     echo "Test group triggered ${testFailedGroup}" 1>&2
     if [ -z "${testFailedErrno}" ] ; then testFailedErrno=4 ; fi
     exit ${testFailedErrno}
   fi
#
   if [ \( -z "${store}" \) -o \( -z "${group}" \) -o \( -z "${bfid}" \) ] ; then
      echo "Invalid level-1 content : store=>${store}< group=>${group}< bfid=>${bfid}<" 1>&2
      exit 14 
   fi 
   echo "$OSMCP -v -S ${store} -B ${bfid} $filename" >>${LOG}
   $OSMCP -v  -S ${store} -B ${bfid} $filename 1>>${LOG} 2>&1
   osm_error=$?
   if [ ${osm_error} -ne 0 ] ; then
      case "${osm_error}" in
        ${OSM_EIOSPACE}) 
            echo "OSM : no space left on device" 1>&2
            exit 41 ;;
        ${OSM_EIOREAD}) 
            echo "OSM : disk read IOError" 1>&2
            exit 42 ;;
        ${OSM_EIOWRITE})
            echo "OSM : disk write IOError" 1>&2
            exit 43 ;;
        *)
           echo "Failed : $OSMCP -d -S ${store} -B ${bfid} $filename" | tee -a $LOG >&2
           exit 5
        ;;
      esac
   fi
   exit 0
elif [ $command = "put" ] ; then
#
#   and the put
#
   filesize=`ls -l ${filename} 2>/dev/null | awk '{ print $5 }'`
   if [ -z "${filesize}" ] ; then
     echo "File not found : ${filename}" | tee -a $LOG >&2
     exit 31
   elif [ "${filesize}" = "0" ] ; then
     echo "Filesize is zero (${filename})" | tee -a $LOG >&2
     exit 31
   fi
   osmtId=`awk '{ if( $2 == "OSMTemplate" )print $1 }' "${pnfs}/.(ptags)($parentId)"`
   store=`awk '{ if( $1 == "StoreName" )print $2 }' "${pnfs}/.(access)($osmtId)"`
   osmgroupId=`awk '{ if( $2 == "sGroup" )print $1 }' "${pnfs}/.(ptags)($parentId)"`
   group=`cat "${pnfs}/.(access)($osmgroupId)"`
   if [ "$group" = "DUMMY" ] ; then
     echo "Dummy group used" >&2
     exit 0
   fi
   #
   #  check for duplicate copy 
   #
   groupDup=""
   osmgroupDupId=`awk '{ if( $2 == "sGroupDup" )print $1 }' "${pnfs}/.(ptags)($parentId)" 2>/dev/null`
   echo "DEBUG $$ : dup osmgroupDupId : ${osmgroupDupId}" >>${DEVTTY}
   [ ! -z "${osmgroupDupId}" ] && groupDup=`cat "${pnfs}/.(access)($osmgroupDupId)"`
   echo "DEBUG $$ : dup osmgroup : ${groupDup}" >>${DEVTTY}
   if [ -z "$groupDup" ] ; then
      #
      echo "DEBUG $$ : running single instance hsm copy" >>${DEVTTY}
      NEED_LOCK=false
      #
      # this is the standard 'single copy part'
      # .........................................
      #
      # at this point we assume that the following variables
      # have a reasonable 'value'
      #
      #  from arguments    : $filename, $pnfsid
      #  from pnfs, geneic : $pnfsfile
      #  from pnfs, osm    : $store, $group
      #
      bfid=`datasetPut ${group}` || exit $?

      storeStorageInfo "${group}" "${bfid}" || exit $?
            
      setFilesize $filesize
      
   else
   
      echo "DEBUG $$ : running double instance hsm copy" >>${DEVTTY}
      NEED_LOCK=true

      check=`checkLevel 2>>${LOG}` || exit $?
         
      echo "DEBUG $$ : check got ${check}" >>${DEVTTY}
      
      [ "${check}" = "0" ] && exit 0
      
      if [ "${check}" = "3" ] ; then

         echo "DEBUG $$ : running full version" >>${DEVTTY}
         
         (

            echo "DEBUG $$ : starting dataset put background " >>${DEVTTY}
            bfid=`datasetPut ${groupDup}` || setExit $?
            echo "DEBUG $$ : dataset put background done" >>${DEVTTY}

            storeStorageInfo ${groupDup} ${bfid} 2 || setExit $?

            echo "DEBUG $$ : getstorageinfo background done" >>${DEVTTY}

            setExit 0

         ) >>$LOG 2>&1 &

         BACKGROUND_ID=$!

         bfid=`datasetPut ${group}` && storeStorageInfo ${group} ${bfid} 1
         fgrc=$?
         echo "DEBUG $$ : forground returned : $fgrc" >>${DEVTTY}
         echo "DEBUG $$ : waiting for background $BACKGROUND_ID" >>${DEVTTY}
         wait $BACKGROUND_ID
         bgrc=`getExit`
         echo "DEBUG  $$: background return : $bgrc" >>${DEVTTY}
         if [ $fgrc -ne 0 ] ; then
            exit $fgrc
         elif [ $bgrc -ne 0  ] ; then
            exit $bgrc
         else
            exit 0
         fi
      else
      
         thisgroup=${group}
         [ "${check}" = "2" ] && thisgroup=${groupDup}
         
         echo "DEBUG $$ : running with group ${thisgroup}"
         
         bfid=`datasetPut ${thisgroup}` || exit $?

         storeStorageInfo ${thisgroup} ${bfid} ${check} || exit $?
      
      fi
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
