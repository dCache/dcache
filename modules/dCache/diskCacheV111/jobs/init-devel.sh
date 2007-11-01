#!/bin/sh
#
problem() {
   echo ""
   echo "Problem : $2" >&2
   echo ""
   exit $1
}
goUp() {
  m=`pwd`
  m=`basename $m`
  cd ..
  if [ -L $m ] ; then
     m=`( ls -l $m | awk '{ print $11 }')`
     m=`echo $m | awk -F/ '{ for(i=1;i<NF;i++)printf "%s/",$i }'`
     cd $m
  fi
}
targetOfLink() {
   ls -l $1 | awk '{
      for(i=1;i<=NF;i++){
        if( $i == "->" ){
           print $(i+1)
           break
        }
      }
   }'
}
#
goUp
dcacheHome=`pwd`
printf "Checking for config directory ... "
#
[ ! -d ./config ] && problem 5 "Can't find 'config' directory" 
#
echo "Ok"
cd ./config
configdir=`pwd`
#
#
###################################################################
#
#   checking pnfs
#
###################################################################
#
#    create list of pnfs mountpoints
#      Result : pnfsMountpoint , defaultPnfsServer
#
###################################################################
#
printf "Preparing pnfs filesystem ... "
#
tmpfile=".$$-pnfsmountpoints"
rm -rf ${tmpfile} 2>/dev/null
#
df -k | grep -v Filesystem | awk '{ print $1,$NF }' |
  while read rem loc rest 
    do
        echo $rem $loc $rest
        serverId=`cat "${loc}/.(config)(serverId)" 2>/dev/null `
#        echo "${loc}/.(config)(serverName)"
        serverName=`cat "${loc}/.(config)(serverName)" 2>/dev/null `
	[ $? -ne 0 ] && continue
	echo "${rem} ${loc} ${serverId} ${serverName}" >>${tmpfile}
	echo "found ${rem} ${loc} ${serverId} ${serverName}" 
    done
#cat ${tmpfile}
#
# if there is none, we can't continue
#
n=`wc ${tmpfile} | awk '{ print $1 }'`
[  -z "$n" ] && problem 6 "No pnfs filesystem found"
echo "Ok"
echo ""
echo " Please choose one of the following"
echo ""
echo "e : <exit>"
i=1
while read rem loc serverId serverName rest 
  do
     echo "$i : ${loc}   (server=${rem},serverId=${serverId},serverName=${serverName})"
     i=`expr $i + 1`
done <${tmpfile}

i=`expr $i - 1`

echo "" 

quest=" e,1 ... ${i} "
[ $i -eq 1 ] && quest="e,1"
while : ; do
   printf " ${quest} ? "
   read answer
   [ -z ${answer} ] && continue 
   [ "${answer}" = "e" ] && exit 4
   n=`expr ${answer} + 0 2>/dev/null`
   [ $? -ne 0 ] && n=0
   if [ \( "$n" -lt 1 \) -o \( "$n" -gt $i \) ] ; then
      echo " Out of range "
      continue ;
   fi
   break ;
done
echo ""
#
pnfsMountpoint=`head -${i} ${tmpfile} | tail -1 | awk '{ print $2}'`
defaultPnfsServer=`head -${i} ${tmpfile} | tail -1 | awk '{ print $4}'`
rm -rf ${tmpfile}
#
#   update pnfs=<mountpoint> in dCacheSetup
#
#z="s%^pnfs=.*%pnfs=$mp%"
#sed "$z" <dCacheSetup >dCacheSetup.update
#mv dCacheSetup.update dCacheSetup
#
printf "Creating billing and statistics ... "
logdir=${configdir}/../logs
mkdir ${logdir} 2>/dev/null
[ ! -d ${logdir} ] && problem 3 "Can't create ${logdir}"
mkdir ${logdir}/billing 2>/dev/null
[ ! -d ${logdir}/billing ] && problem 3 "Can't create ${logdir}/billing"
mkdir ${logdir}/statistics 2>/dev/null
[ ! -d ${logdir}/statistics ] && problem 3 "Can't create ${logdir}/statistics"
#
echo "Ok"
#
unset ourTrash
#
if [ -f /usr/etc/pnfsSetup ]
 then
     . /usr/etc/pnfsSetup
     if [ -z "${trash}" ] 
       then
          echo ""
	  echo "Warning : \${trash} not defined in /usr/etc/pnfsSetup"
	  echo "          Could be, there is no pnfs server running on this host"
	  echo ""
     elif [ ! -d ${trash} ] 
       then
           echo ""
           echo "Warning : \${trash}=${trash} doesn't exist"
	   echo "          Could be, there is no pnfs server running on this host"
	   echo ""
     else
         mkdir ${trash}/2 2>/dev/null
	 if [ ! -d ${ourTrash} ] 
	    then
              echo ""
   	      echo "Warning : can't create ${ourTrash}/2"
              echo ""
	 else
              ourTrash=${trash}/2
	 fi 
     fi 
     
 else
    echo ""
    echo "Warning : no /usr/etc/pnfsSetup found" >&2
    echo "          There is no pnfs server running on this host"
    echo ""
 fi 
#
echo ""
echo ""
#
#   
#################################################################
#
#       create the developers config/devel
#
#################################################################
mkdir ${configdir}/devel 2>/dev/null
cd ${configdir}/devel
#
#
printf " Checking Users database .... "
isok=0
mkdir -p users/acls users/meta users/relations
for c in users/acls users/meta users/relations
  do
     if [ ! -d ${c} ]
       then
           echo "Failed"
           isok=1
           break 
       fi
  done
  [ $isok -eq 0 ] && echo "Ok"

poollistfile=${configdir}/devel/basic.poollist
#
if [ ! -f "${poollistfile}" ] ; then
   echo ""
   echo "No poollist find found, creating empty one."
   echo "Please customize : ${poollistfile}"
   touch ${poollistfile}
   echo ""
fi
#
#################################################################
#
#      preparing keys 
#
#################################################################
keydir=${configdir}/devel
#
printf "Preparing keys ... " 
which ssh-keygen 1>/dev/null 2>&1
if [ $? -ne 0 ] 
   then
       echo "ssh-keygen not found, running without 'ssh server'" >&2
   else
       serverkey=${keydir}/server_key
       [ ! -f "${serverkey}" ] && ssh-keygen -t rsa1 -b  768 -f ${serverkey} -N "" 1>/dev/null 2>&1
       hostkey=${keydir}/host_key
       if [ ! -f ${hostkey} ] 
          then
              if [ -f /etc/ssh/ssh_host_key ]
                   then
                   ln -s /etc/ssh/ssh_host_key ${hostkey}
              else
                   ssh-keygen -t rsa1 -b  1024 -f ${hostkey} -N "" 1>/dev/null 2>&1
              fi
          fi
   fi
#
if [ \( ! -f ${serverkey} \) -o \( ! -f ${hostkey} \) ] 
   then
       echo "Some ssh keys couldn't be created , running without 'ssh server'" >&2
   fi
#
#
echo "Ok"
#
#
#################################################################
#
#       create the developer's skin
#
#################################################################
#
# preparing fancy images
#
printf "Preparing skin ... "
imageDirectory=${configdir}/../docs/images
#
[ ! -d "${imageDirectory}" ] && problem 6 "Couldn't find images directory @ ${imageDirectory}"
#
if [ -f ${imageDirectory}/dc-devel-2.png ]
  then
     if [ -L ${imageDirectory}/bg.jpg ] 
       then
          rm ${imageDirectory}/bg.jpg
       else
          mv ${imageDirectory}/bg.jpg ${imageDirectory}/bg.jpg.old 2>/dev/null
       fi
       ln -s ${imageDirectory}/dc-devel-2.png ${imageDirectory}/bg.jpg
  fi
(
echo "<html>"
echo "<head><title>dCache server for Developer</title></head>"
echo "<body background=\"/images/bg.jpg\" text=\"#000000\" link=\"#000000\" vlink=\"#000000\" alink=\"#000000\">"
echo "<center>"
echo "<h1 style=\"font-size:300%\"><font color=red><em>dCache server for Developers</em></font></h1>"
echo "</center>"
echo "<center>"
echo "<table border=\"1\" width=\"70%\">"
echo "<tr>"
echo "<td align=\"center\" valign=\"middle\" >"
echo "<br><br>"
echo "<img src=\"/images/eagle-main.gif\"><br><br>"
echo "<table width=\"90%\" border=1 cellspacing=0 cellpadding=10 bgcolor=\"#5555ff\">"
echo "<tr>"
echo "<td align=center><a href=\"/cellInfo\"><h3><font color=white>Cell Services</font></h3></a></td>"
echo "<td align=center><a href=\"/usageInfo\"><h3><font color=white>Pool (Space) Usage</font></h3></a></td>"
echo "<td align=center><a href=\"/queueInfo\"><h3><font color=white>Pool Request Queues</font></h3></a></td>"
echo "</tr>"
echo "<tr>"
echo "<td align=center><a href=\"/billing/\"><h3><font color=white>Action Log</font></h3></a></td>"
echo "<td align=center><a href=\"/context/transfers.html\"><h3><font color=white>Active Transfers</font></h3></a></td>"
echo "<td align=center><a href=\"/poolInfo\"><h3><font color=white>Pool Setup</font></h3></a></td>"
echo "</tr>"
echo "</table>"
echo "<br>"
echo "</td></tr>"
echo "</table>"
echo "</center>"
echo "<br><br><br>"
echo "<hr>"
echo "<address><a href="http://www.dcache.org">&copy; dCache.ORG</a> Version HEAD</address>"
echo "</body>"
echo "</html>"
) >home-skin1.html
#
echo "Ok"
#################################################################
#
#       finally create the dCacheSetupfile
#
#################################################################
#
   setupfile=dCacheSetup
   [ -f ${setupfile} ] && mv ${setupfile} ${setupfile}-$$
   (
       echo "#################################################################"
       echo "#"
       echo "# Created `date`  by init-devel"
       echo "#"
       echo "#################################################################"
       echo "java=/usr/java/bin"
       echo "java_options=\"\""
       echo "#classpath=\${dcacheHome}/classes"
       echo "#"
       echo "librarypath=\${dcacheHome}/lib"
       echo "images=\${dcacheHome}/docs/images"
       echo "keyBase=\${dcacheHome}/config/devel"
       echo "billingDb=\${dcacheHome}/logs/billing"
       echo "statisticsDb=\${dcacheHome}/logs/statistics"
       echo "trashDb=${ourTrash}"
       echo "trash=${ourTrash}"
       echo "#"
       echo "portBase=22"
       echo "adminPort=\${portBase}223"
       echo "httpdPort=\${portBase}88"
       echo "dCapPort=\${portBase}125"
       echo "pnfs=${pnfsMountpoint}"
#       echo "#defaultPnfsServer=${defaultPnfsServer}"

    ) >${setupfile}
#################################################################
#
#       finally create the PoolManager.conf
#
#################################################################
    poolconf=PoolManager.conf
    (
      echo "psu create unit -net    0.0.0.0/0.0.0.0"
      echo "psu create unit -store  *@*"
      echo "#"
      echo "# The unit Groups ..."
      echo "#"
      echo "psu create ugroup any-store"
      echo "psu addto ugroup any-store *@*"
      echo "psu create ugroup world-net"
      echo "psu addto ugroup world-net 0.0.0.0/0.0.0.0"
      echo "#"
      echo "# The pool groups ..."
      echo "#"
      echo "psu create pgroup default"
      echo "#"
      echo "# The links ..."
      echo "#"
      echo "psu create link default-link any-store world-net"
      echo "psu set link default-link -readpref=10 -writepref=10 -cachepref=10"
      echo "psu add link default-link default"
      echo "#"
      echo "rc onerror suspend"
      echo "rc set max retries 3"
      echo "rc set retry 900"
      echo "rc set warning path billing"
      echo "rc set poolpingtimer 600"
      echo "rc set slope 0.0"
      echo "rc set p2p oncost"
      echo "rc set stage oncost off"
      echo "rc set stage off"
      echo "set costcuts -idle=1.0 -p2p=2.0 -alert=0.0 -halt=0.0 -fallback=0.0"
      echo "rc set max copies 500"
      echo "rc set max restore unlimited"
      echo "rc set sameHostCopy besteffort"
      echo "rc set max threads 0"

    ) >${poolconf}
exit 0
