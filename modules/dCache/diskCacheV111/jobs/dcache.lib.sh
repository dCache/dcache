#
#
#----------------------------------------------------------------------
#
#          We need the full path for the diskCache domain
#
fullSetup=${setupFilePath}
#
#  make the keyBase available, if not set at this point, we
#  follow the standard and put the keyBase into the ${jobs}
#
if [ -z "${keyBase}" ] ; then  keyBase=${jobs} ; fi
#
hostBase=`hostname | awk -F. '{ print $1 }'`
#
#
#
#
##################################################################
#
#       dcache.stop      
#
dcacheStop() {
#
   checkForSsh  || exit $?
#
   echo "Trying to halt the dCache" 
   $SSH $SSH_OPTIONS -p $sshPort -o "FallBackToRsh no" localhost <<!  1>/dev/null 2>&1
       kill System
!
  if [ $? -ne 0 ] ; then
     echo "System already stopped" 1>&2
     exit 3
  fi
  return 0
}
##################################################################
#
#       dcache.login      
#
dcacheLogin() {
#............
#
   checkForSsh  || exit $?
#
  $SSH $SSH_OPTIONS -p $sshPort  -o "FallBackToRsh no" localhost
  return $?
}
#
##################################################################
#
dcacheCheck() {
   $SSH $SSH_OPTIONS -p $sshPort -o "FallBackToRsh no" localhost <<! >/dev/null 2>&1
      exit
      exit
!
   return $?
}
##################################################################
#
#       dcache.spy     
#
dcacheSpy() {

   checkVar spyPort || exit 4
   checkJava || exit $?
  
   $JAVA dmg.cells.applets.spy.Spy localhost $spyPort 1>/dev/null 2>&1 &
   spyId=$!
   echo "Spy has been forked (Id=$spyId)"
   sleep 3
   kill -0 $spyId 2>/dev/null
   if [ $? -ne 0 ] ; then
       echo "Forking Spy seems to have failed"
       exit 4
   fi
   exit 0
   
}
##################################################################
#
#       dcache.listen    
#
dcacheListen() {

   checkJava || exit $?
  
   $JAVA diskCacheV111.clients.vsp.VspListener
   exit $?
   
}
##################################################################
#
#       dcache.start     
#
startUsage() {
    echo "   Usage : ... start  {-type=master} } { -type=pool -pools=<poolFile> }" 2>&1
    exit 4
}
checkLogfile() { 
      rm -rf $LOGFILE >/dev/null 2>/dev/null
      touch $LOGFILE >/dev/null 2>/dev/null
      if [ $? -ne 0 ] ; then
	 echo " - Not allowed to write logfile : ${LOGFILE}. Using dumpster"
	 LOGFILE=/dev/null
      fi
      echo "   Logfile written to ${LOGFILE}"
      return 0
}
createPoolCellBatch() {
   if [ ! -z ${pools} ] ; then
       pools=`getFull ${pools}`
       echo " - reading poolinformation from ${pools}"
       if [ -f ${pools} ] ; then
          poolBatch=${jobs}/context-$$.batch
          rm -rf ${poolBatch}
          while read poolName poolBase ; do
             echo "   pool ${poolName} on ${poolBatch}"
             echo -n "create diskCacheV111.pools.MultiProtocolPool ${poolName}" >>${poolBatch}
             echo    "   \"${poolBase}\"" >>${poolBatch}
          done <${pools}
       else
          echo " * poolinfo file not found : ${pools}" 1>&2
          exit 54
       fi
   fi
   return 0
}
dcacheStartMaster() {
    echo " - Master diskCache instance requested"
    domainName=dc-admin-${hostBase}
    if [ ! -z "${batch}" ] ; then
       BATCHFILE=`getFull ${batch}`
    else
       BATCHFILE=${jobs}/master.batch
    fi
    LOGFILE=/tmp/${domainName}.log
    #
    #   need some variables
    #
    checkVar sshPort ftpPort ftpBase vspPort || exit $?
    #
    #   get pnfs to work 
    #
    if [ \( -z "$pnfsMount"        \) -o \
         \( "$pnfsMount" = "auto"  \)      ] ; then

       echo " - Trying to autodetect PnfsFilesystem" 
       pnfsMount=`autodetectPnfs` 
       xx=$?
       if [ $xx -ne 0 ] ; then
          echo " * Couldn't detect Pnfs Filesystem $pnfsMount" 1>&2
          exit 5
       fi
       echo "   PnfsFilesystem found on $pnfsMount"
    else      
       if [ ! -f "$pnfsMount/.(const)(magic)" ] ; then
          echo " * $pnfsMount seems not to be a P Filesystem" 1>&2
          exit 4
       fi
       echo "   PnfsFilesystem defined to $pnfsMount"
    fi
    #
    if [ ! -z "$spyPort" ] ; then
       SPY_IF_REQUESTED="-spy $spyPort"
    fi
    if [ ! -z "${masterPort}" ] ; then
       TUNNEL_IF_REQUESTED="-tunnel ${masterPort} -routed"
    fi
    #
    # we might want to have a pool running within the admin
    # domain.
    #
    createPoolCellBatch || exit $?
    #
    #
    # check for java and ssh
    #
    checkLogfile
    checkJava || exit $?
    checkForSsh  || exit $?
    #
    echo " - Trying to start DiskCache (Master)"
    #
    export LD_LIBRARY_PATH
    LD_LIBRARY_PATH=${libraryPath}
    echo " - Using LibraryPath : $LD_LIBRARY_PATH"
    printf "   Please wait ... "
    #
    # are we still/already running
    #
    dcacheCheck 
    if [ $? -eq 0 ] ; then
       echo "System already running" 
       exit 4 
    fi
    #
    $JAVA  dmg.cells.services.Domain ${domainName} \
             -param setupFile=${fullSetup} \
                    keyBase=${keyBase} \
                    pnfsMount=${pnfsMount} \
                    poolList=${poolBatch}  \
             -batch $BATCHFILE  \
             $TELNET_IF_REQUESTED $SPY_IF_REQUESTED $TUNNEL_IF_REQUESTED >$LOGFILE 2>&1 &
    printf "  "
    for c in 6 5 4 3 2 1 0 ; do 
       sleep 1
       printf "$c "
    done
    echo " ."
    $SSH $SSH_OPTIONS -e none -p $sshPort \
         -o "FallBackToRsh no" localhost <<! 2>/dev/null | grep " A "
       ps -f
       exit
       exit
!
    if [ $? -ne 0 ] ; then 
       echo "dCache didn't start"
       echo ""
       echo " ------ Infos from Logfile ($LOGFILE) ---------"
       echo ""
       tail -5 $LOGFILE
       exit 4
    else
       exit 0
    fi
    return 0 
}
dcacheStartPool() {
   #
   domainName=dc-pool-${hostBase}-$$
   if [ ! -z ${batch} ] ; then
      BATCHFILE=`getFull ${batch}`
   else
      BATCHFILE=${jobs}/pools.batch
   fi
   LOGFILE=/tmp/${domainName}.log
   #
   if [ -z "${pools}" ] ; then
      echo " * need pools setup '-pools=<poolFile>'" 1>&2
      startUsage
      exit 5
   fi
   createPoolCellBatch || exit $?
   if [ ! -f "${pools}" ] ; then
      echo " * pool definition file not found ${pools}" 1>&2
      exit 4
   fi
   echo " - Trying to start DiskCache (${domainName})"
    printf "   Please wait ... "
    if [ \( ! -z "${masterPort}" \) -a \( ! -z "${masterHost}" \) ] ; then
       TUNNEL_IF_REQUESTED="-connect ${masterHost} ${masterPort} -routed"
    else
       echo " * masterHost or masterPort not defined (can't run without admin Domain)" 1>&2
       exit 4
    fi
    $JAVA  dmg.cells.services.Domain ${domainName} \
             -param setupFile=${fullSetup} \
                    poolList=${poolBatch}  \
             -batch $BATCHFILE  \
             $TELNET_IF_REQUESTED $TUNNEL_IF_REQUESTED >$LOGFILE 2>&1 &
    lastId=$!
    printf "  "
    for c in 6 5 4 3 2 1 0 ; do 
       sleep 1
       printf "$c "
    done
    echo " ."
    if !  kill -0 ${lastId} >/dev/null 2>/dev/null ; then
       echo " * Pool Domain ${domainName} couldn't be started" 1>&2
       tail -10 ${LOGFILE}
       exit 45
    fi
    return 0
}
dcacheStart() {
   # 
   if [ ! -z "$spyPort" ] ; then
      SPY_IF_REQUESTED="-spy $spyPort"
   fi
   if [ ! -z "$telnetPort" ] ; then
      TELNET_IF_REQUESTED="-telnet $telnetPort"
   fi
   if [ "${type}" = "master" ] ; then
       dcacheStartMaster
   elif [ "${type}" = "pool" ] ; then
       dcacheStartPool
   else
       echo "   Needs : -type=master|pool" 1>&2
       startUsage
       exit 4
   fi
   rm -rf ${poolBatch}
}
#
#-----------------------------------------------------------------
#
dcacheHelp() {
   echo "Usage : dcache start|stop|login "
   exit 4
}
dcacheSwitch() {
   case "$1" in
      *start)       shift 1 ; dcacheStart $* ;;
      *stop)        dcacheStop  ;;
      *login)       dcacheLogin  ;;
      *spy)         dcacheSpy $*  ;;
      *listen)         dcacheListen $*  ;;
      *) dcacheHelp $*
      ;;
   esac
}
#
# ---------------------------------------------------------------------
#
