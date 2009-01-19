#!/bin/sh
#
# $Id: admin-install.sh,v 1.1 2004-05-05 13:10:44 ernst Exp $
#
ADMIN_CONFIG=/opt/d-cache/etc/admin_config
#ADMIN_SETUP_TEMPLATE=`cat $ADMIN_CONFIG | grep ADMIN_SETUP_TEMPLATE |awk '{print $3}'`



DCACHE_HOME=`cat $ADMIN_CONFIG | grep DCACHE_HOME |awk '{print $3}'`

if [ "x${DCACHE_HOME}" = "x" ] ; then
  echo "WARNING: the variable DCACHE_HOME is not set."
  DCACHE_HOME=`cat $ADMIN_CONFIG | grep DCACHE_BASE_DIR |awk '{print $3}'`
  if [ "x${ourHome}" != "x" ] ; then
     echo "WARNING: Using deprecated value of DCACHE_BASE_DIR as DCACHE_HOME"
  else
     echo "ERROR: Failed getting the value of DCACHE_HOME"
  fi
fi




ADMIN_SETUP_TEMPLATE=$DCACHE_HOME/etc/dCacheSetup.template
PNFS_ROOT=`cat $ADMIN_CONFIG | grep PNFS_ROOT |awk '{print $3}'`
PNFS_INSTALL_DIR=`cat $ADMIN_CONFIG | grep PNFS_INSTALL_DIR |awk '{print $3}'`
RETVAL=0
      rpcinfo -u localhost 100003 >/dev/null 2>&1
      RETVAL=$?
    if [ $RETVAL -eq 1 ]; then
      echo ""
      printf " PNFS is not running (needed to prepare dCache) "
#read yesno < /dev/tty
      yesno=`cat $ADMIN_CONFIG | grep PNFS_START |awk '{print $3}'`
      if [ \( "$yesno" = "n" \) -o \( "$yesno" = "no" \) ] ; then
      exit 0
      elif [ \( "$yesno" = "y" \) -o \( "$yesno" = "yes" \) ] ; then
      echo ""
      fi
      echo "Starting the PNFS server"
      $PNFS_INSTALL_DIR/tools/pnfs.server start
   fi  
echo "Checking on a possibly existing dCache/PNFS configuration"
if [ -d $PNFS_ROOT/fs ]; then
   cp=`df $PNFS_ROOT/fs 2>/dev/null | tail -1 | awk '{print $0}' |grep $PNFS_ROOT | awk '{print $2}'`
    if [ -z $cp ]; then
       echo "$PNFS_ROOT/fs mount point exists, but is not mounted - going to mount it now"
       mount -o intr,rw,noac,hard localhost:/fs $PNFS_ROOT/fs
    fi
 if [ -f $PNFS_ROOT/fs/admin/etc/config/serverRoot ]; then
   echo "Found an existing dCache/PNFS configuration!"
# echo "Do you want to overwrite it? [y/n] : "
   echo""
#read yesno < /dev/tty
      yesno=`cat $ADMIN_CONFIG | grep PNFS_OVERWRITE |awk '{print $3}'`
      if [ \( "$yesno" = "n" \) -o \( "$yesno" = "no" \) ] ; then
      echo ""
      echo "Not allowed to overwrite existing configuration - Exiting"
      echo""
      echo "Installing dCacheSetup.template in dCache installation directory"
      echo "Preserving eventual existing dCacheSetup in dCacheSetup.old" 
      if [ -f $DCACHE_HOME/config/dCacheSetup ]; then
         cp $DCACHE_HOME/config/dCacheSetup $DCACHE_HOME/config/dCacheSetup.old
      fi
      cp $ADMIN_SETUP_TEMPLATE $DCACHE_HOME/config/dCacheSetup 
#      echo "Installing dcache.kpwd (SRM/GridFTP authentication file)"
#      echo " Preserving eventual existing dcache.kpwd in dcache.kpwd.old"
#      if [ -f $DCACHE_HOME/etc/dcache.kpwd ]; then
#         cp $DCACHE_HOME/etc/dcache.kpwd $DCACHE_HOME/etc/dcache.kpwd.old
#      fi
#      cp $DCACHE_HOME/etc/dcache.kpwd.template $DCACHE_HOME/etc/dcache.kpwd
#      cd $PNFS_INSTALL_DIR/tools
#     umount $PNFS_ROOT/fs
#     ./pnfs.server stop
      exit 0
      elif [ \( "$yesno" = "y" \) -o \( "$yesno" = "yes" \) ] ; then
      echo ""
      echo "Overwriting existing dCache/PNFS configuration"
      fi
 fi
fi
echo "Creating PNFS mount point ($PNFS_ROOT/fs) and mounting PNFS"
if [ ! -d $PNFS_ROOT/fs ]; then
        mkdir $PNFS_ROOT
        mkdir $PNFS_ROOT/fs
        mount -o intr,rw,noac,hard localhost:/fs $PNFS_ROOT/fs
fi
# mount -o intr,rw,noac,hard localhost:/fs $PNFS_ROOT/fs
sleep 5
cd $PNFS_ROOT/fs
sr=`cat ".(id)(usr)"`
cd ./admin/etc/config
echo `hostname` > ./serverName
# cat /etc/resolv.conf| grep search | awk '{ print($2) }' >./serverId
SID="`cat /etc/resolv.conf| grep search | awk '{ print($2) }'`"
if [ -z $SID ]; then
SID="`cat /etc/resolv.conf| grep domain | awk '{ print($2) }'`"
fi
echo $SID >./serverId
# Map /pnfs/fs/usr to /pnfs/domain (e.g. /pnfs/fnal.gov) for GridFTP/SRM
cd $PNFS_ROOT
if [ -h $SID ]; then
        echo "$PNFS_ROOT/$SID - link exists"
else
        ln -s fs/usr $SID
fi
cd $PNFS_ROOT/fs/admin/etc/config
echo "$sr ." > ./serverRoot
touch ".(fset)(serverName)(io)(on)"
touch ".(fset)(serverId)(io)(on)"
touch ".(fset)(serverRoot)(io)(on)"
mkdir -p dCache
cd dCache
echo `hostname`:22125 > ./dcache.conf
touch ".(fset)(dcache.conf)(io)(on)"
#
# Install ssh keys for secure communication
cd $DCACHE_HOME/config
if [ -f ./server_key ]; then
           rm ./server_key; rm ./host_key
   fi
   ssh-keygen -b 768 -t rsa1 -f ./server_key -N ""
   ln -s /etc/ssh/ssh_host_key ./host_key
# Configure misc. stuff for large file store
cd $PNFS_ROOT/fs/usr/data
echo "StoreName myStore" > ".(tag)(OSMTemplate)"
echo STRING > ".(tag)(sGroup)"
# Configuration completed, stop & unmount PNFS
#echo "Configuration completed - stop and unmount PNFS"
#cd $PNFS_INSTALL_DIR/tools
#umount $PNFS_ROOT/fs
#./pnfs.server stop
exit 0
fi
echo "Creating PNFS mount point ($PNFS_ROOT/fs) and mounting PNFS"
if [ ! -d $PNFS_ROOT/fs ]; then
        mkdir $PNFS_ROOT
        mkdir $PNFS_ROOT/fs
fi
mount -o intr,rw,noac,hard localhost:/fs $PNFS_ROOT/fs
sleep 5
cd $PNFS_ROOT/fs
sr=`cat ".(id)(usr)"`
cd ./admin/etc/config
echo `hostname` > ./serverName
# cat /etc/resolv.conf| grep search | awk '{ print($2) }' >./serverId
SID="`cat /etc/resolv.conf| grep search | awk '{ print($2) }'`"
if [ -z $SID ]; then
SID="`cat /etc/resolv.conf| grep domain | awk '{ print($2) }'`"
fi
echo $SID >./serverId
# Map /pnfs/fs/usr to /pnfs/domain (e.g. /pnfs/fnal.gov) for GridFTP/SRM
cd $PNFS_ROOT
if [ -h $SID ]; then
        echo "$SID exists"
else
        ln -s fs/usr $SID
fi
cd $PNFS_ROOT/fs/admin/etc/config
echo "$sr ." > ./serverRoot
touch ".(fset)(serverName)(io)(on)"
touch ".(fset)(serverId)(io)(on)"
touch ".(fset)(serverRoot)(io)(on)"
mkdir dCache
cd dCache
echo `hostname`:22125 > ./dcache.conf
touch ".(fset)(dcache.conf)(io)(on)"
#
# Install ssh keys for secure communication
cd $DCACHE_HOME/config
if [ -f ./server_key ]; then
           rm ./server_key; rm ./host_key
   fi
   ssh-keygen -b 768 -t rsa1 -f ./server_key -N ""
   ln -s /etc/ssh/ssh_host_key ./host_key
# Configure misc. stuff for large file store
cd $PNFS_ROOT/fs/usr/data
echo "StoreName myStore" > ".(tag)(OSMTemplate)"
echo STRING > ".(tag)(sGroup)"
# Configuration completed, stop & unmount PNFS
#echo "Configuration completed - stop and unmount PNFS"
#cd $PNFS_INSTALL_DIR/tools
#umount $PNFS_ROOT/fs
#./pnfs.server stop
      echo "Installing dCacheSetup.template in dCache installation directory"
      echo "Preserving eventual existing dCacheSetup in dCacheSetup.old" 
      if [ -f $DCACHE_HOME/config/dCacheSetup ]; then
         cp $DCACHE_HOME/config/dCacheSetup $DCACHE_HOME/config/dCacheSetup.old
      fi
      cp $ADMIN_SETUP_TEMPLATE $DCACHE_HOME/config/dCacheSetup
#      echo "Installing dcache.kpwd (SRM/GridFTP authentication file)"
#      echo " Preserving eventual existing dcache.kpwd in dcache.kpwd.old"
#      if [ -f $DCACHE_HOME/etc/dcache.kpwd ]; then
#         cp $DCACHE_HOME/etc/dcache.kpwd $DCACHE_HOME/etc/dcache.kpwd.old
#      fi
#      cp $DCACHE_HOME/etc/dcache.kpwd.template $DCACHE_HOME/etc/dcache.kpwd
exit 0




# Make Final Changes to config Files
#dserv=`/bin/hostname`
#cd /opt/d-cache/config
#JN="java=\"/usr/java/`ls /usr/java|grep j2`/bin/java \""
#echo "s|^java=\".*\"$|$JN|" > edit.lst
#sed -f edit.lst /opt/d-cache/config/dCacheSetup > dCacheSetup.mod
# sed 's|^java=\".*\"$|'"$JN"' |' < /usr/d-cache/config/dCacheSetup > dCacheSetup.mod
#sed -e s:dcache-serv:$dserv:g dCacheSetup.mod > dCacheSetup
# PSP="-pnfs-srm-path=/pnfs/`cat /etc/resolv.conf| grep search | awk '{ print($2) }'`/data \\\\"
#PSP="-pnfs-srm-path=/pnfs/`cat /pnfs/fs/admin/etc/config/serverId`/data \\\\"
#SID="`cat /etc/resolv.conf| grep search | awk '{ print($2) }'`"
#if [ -z $SID ]; then
#SID="`cat /etc/resolv.conf| grep domain | awk '{ print($2) }'`"
#fi
#PSP="-pnfs-srm-path=/pnfs/$SID/data \\\\"
#echo "s|^-pnfs-srm-path=\/.*\\\\$|$PSP|" > edit.lst
#sed -f edit.lst /opt/d-cache/config/srm.batch > edit.mod
#cp edit.mod /opt/d-cache/config/srm.batch
# sed 's|^-pnfs-srm-path=^-pnfs-srm-path=\/.*\\$|'"$PSP"' |' < /usr/d-cache/config/srm.batch > srm.batch.mod
#
#if [ -f /tmp/globus_location ]; then
#if [ -z $GLOBUS_LOCATION ]; then
#   echo "Warning: GLOBUS_LOCATION not set, using /opt/globus"
#   GLOBUS_LOCATION=/opt/globus
#   else
#   GLOBUS_LOCATION=$GLOBUS_LOCATION
#GL="GLOBUS_LOCATION=$GLOBUS_LOCATION"
#echo "s|^GLOBUS_LOCATION=\".*\$|$GL|" > edit.lst
#sed -f edit.lst /opt/grid/gsint/gsint.sh > edit.mod
#cp edit.mod /opt/grid/gsint/gsint.sh
#GL="GLOBUS_LOCATION = `cat /opt/d-cache/etc/globus_location`"
#echo "s|^GLOBUS_LOCATION = .*\$|$GL|" > edit.lst
#sed -f edit.lst /opt/grid/gsint/Makefile > edit.mod
#cp edit.mod /opt/grid/gsint/Makefile
#fi
