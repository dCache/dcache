#!/bin/sh
#
# $Id: install.sh,v 1.17 2007-10-19 09:27:29 radicke Exp $
#
#       dCache Installation script
#
# This script reads all neccessary information from ${ourHomeDir}/etc/node_config
#
ourHomeDir=/opt/d-cache



domainname_os() {
    case `uname` in
        Linux)
            echo `hostname -d`
            ;;
        SunOS)
            echo `/usr/lib/mail//sh/check-hostname |cut -d" " -f7 | awk -F. '{ for( i=2; i <= NF; i++){ printf("%s",$i); if( i  <NF) printf("."); } } '`
            ;;
    esac
}


fqdn_os() {
    case `uname` in
        Linux)
            echo `hostname --fqdn`
            ;;
        SunOS)
            echo `/usr/lib/mail/sh/check-hostname |cut -d" " -f7`
            ;;
    esac
}

shortname_os() {
    case `uname` in
        Linux)
            echo `hostname -s`
            ;;
        SunOS)
            echo `uname -n`
            ;;
    esac
}

printConfig() {
    key=$1
    cat ${ourHomeDir}/etc/node_config \
        ${ourHomeDir}/etc/door_config 2>/dev/null |
    perl -e "
      while (<STDIN>) { 
         s/\#.*$// ;                        # Remove comments
         s/\s*$// ;                         # Remove trailing space
         if ( s/^\s*${key}\s*=*\s*//i ) {   # Remove key and equals
            print;                          # Print if key found
            last;                           # Only use first appearance
         }
      }
    "
}

absfpath () {
  CURDIR=`pwd`
  cd `dirname $1`
  ABSPATH=`pwd`/`basename $1`
  cd "$CURDIR"
  echo ${ABSPATH}
}


os_absolutePathOf() {
    case `uname` in
        Linux)
            readlink -f $1
            ;;
        *)
            path=`absfpath $1`
            while true
            do
                /bin/test -L "${path}"
                if [ $? -ne 0 ]
                then
                   break;
                fi

                newpath=`ls -ld  ${path} | awk '{ print $11 }'`
                echo ${newpath} | egrep "^/"  > /dev/null 2>&1
                if [ $? -eq 0 ]
                then
                    # absolute path
                    fullpath=${newpath}
                else
                    linkpath=`dirname ${path}`/${newpath}
                    fullpath=`absfpath ${linkpath}`
                fi
                path=${fullpath}
            done
            echo ${fullpath}
            ;;
    esac
}



if [ ! -r ${ourHomeDir}/etc/node_config ]; then
    echo "[ERROR] ${ourHomeDir}/etc/node_config missing."
    echo "[HINT]  Copy ${ourHomeDir}/etc/node_config.template to ${ourHomeDir}/etc/node_config and customize it "
    echo "        before running the install script. Exiting."
    exit 4
fi

# What are we going to install - an admin, pool or custom node?

#
# valid valued for NODE_TYPE:
#     admin  : head node
#     custom : custom service definition
#     door   : door only
#     pool   : pool only
#
nodeType=`printConfig NODE_TYPE`
case ${nodeType} in
	
	admin|door|pool)
		# OK
		;;
	custom)
		#Custom can be admin door or pool for this scripts perspective
		adminDoor=`printConfig adminDoor`
		httpDomain=`printConfig httpDomain`
		lmDomain=`printConfig lmDomain`
		poolManager=`printConfig poolManager`
		utilityDomain=`printConfig utilityDomain`
		if [ "${adminDoor}" == "yes" -a "${httpDomain}" == "yes" -a "${lmDomain}" == "yes" -a "${poolManager}" == "yes" -a "${utilityDomain}" == "yes" ] ; then
			nodeType="admin"
		fi		
		;;
	dummy)
		# not specified
		echo "[ERROR] ${ourHomeDir}/etc/node_config not configured. Exiting."
		exit 1		
		;;
	*)
		# bad falue
		echo "[ERROR] ${ourHomeDir}/etc/node_config not useful. Exiting."		
	    echo "[HINT]  Copy node_config.template to node_config and customize it "
	    echo "        before running the install script. Exiting."		
		exit 2
		;;

esac


DCACHE_HOME=`printConfig DCACHE_HOME`
ADMIN_SETUP_TEMPLATE=${DCACHE_HOME}/etc/dCacheSetup

# pnfs or chimera?
NAMESPACE=`printConfig NAMESPACE`

PNFS_ROOT=`printConfig PNFS_ROOT`
PNFS_INSTALL_DIR=`printConfig PNFS_INSTALL_DIR`
NUMBER_OF_MOVERS=`printConfig NUMBER_OF_MOVERS`
pnfsServer="`printConfig pnfsServer`"
ADMIN_NODE="`printConfig ADMIN_NODE`"
if [ "${ADMIN_NODE}" = "myAdminNode" ] ; then
    unset ADMIN_NODE
fi
if [ -z "${pnfsServer}" ] ; then
    if [ -n "${ADMIN_NODE}" ] ; then
		pnfsServer="${ADMIN_NODE}"
    else
		pnfsServer='localhost'
    fi
fi

startPnfsManager="`printConfig pnfsManager`"

fqHostname=`fqdn_os`
shortHostname=`shortname_os`

SERVER_ID=`printConfig SERVER_ID`
if [ -z "${SERVER_ID}" ] ; then
    SERVER_ID=`domainname_os`
    if [ $? -ne 0 -o -z "${SERVER_ID}" ] ; then
        SERVER_ID="`cat /etc/resolv.conf | sed -e 's/#.*$//' | grep 'search' | awk '{ print($2) }'`"
        if [ -z "${SERVER_ID}" ]; then
            SERVER_ID="`cat /etc/resolv.conf | sed -e 's/#.*$//' | grep 'domain' | awk '{ print($2) }'`"
        fi
    fi
    echo ""
    echo "[INFO]  No 'SERVER_ID' set in 'node_config'. Using SERVER_ID=${SERVER_ID}."
fi


if [ ! -f ${DCACHE_HOME}/config/dCacheSetup ] ; then
    echo ""
    echo "[ERROR] There is no dCacheSetup file."
    echo "[HINT]  Copy ${ourHomeDir}/etc/dCacheSetup.template to ${DCACHE_HOME}/config/dCacheSetup and customize it "
	echo "        before running the install script. Exiting."    
    exit 1
fi

ourHomeDir=${DCACHE_HOME}
. ${DCACHE_HOME}/config/dCacheSetup


#
# check java:
#     jdk >= 1.5 , ( javac needed by tomcat/SRM )
#

if [ -z "${java}" ]; then
	echo "[ERROR] java variable in ${DCACHE_HOME}/config/dCacheSetup not defined"
	exit 6
fi

#
# resove java path eg. /usr/bin/java = /usr/j2se_1.4.2/bin/java
#
java=`os_absolutePathOf ${java}`
if [ -z "${java}" ]; then
	echo "[ERROR] java variable in ${DCACHE_HOME}/config/dCacheSetup do not point to existing binary"
	exit 7
fi

${java} -version 2>&1 | grep version | egrep "1\.[56]\." >/dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "[ERROR] java variable in ${DCACHE_HOME}/config/dCacheSetup do not point to java version 1.5.x or 1.6.x"
	exit 6
fi

# standard javac location $JAVA_HOME/bin/java
# check for javac
JAVA_HOME=${java%/bin/*}
if [ ! -x ${JAVA_HOME}/bin/javac ]; then
    # on some system (e.g. Debian), $JAVA_HOME/bin/java points
    # to $JAVA_HOME/jre/bin/java. Try to go up another level.
    JAVA_HOME=${java%/jre/bin/*}
    if [ ! -x ${JAVA_HOME}/bin/javac ]; then
	    echo "[ERROR] java installation looks like JRE, while JDK is needed."	
	    exit 7
    fi
fi

#
# init package ( create wrappers in jobs directory )
#
${DCACHE_HOME}/jobs/initPackage.sh ${DCACHE_HOME}
if [ $? != 0 ]; then
	echo "Failed to initalize dCache installation, exiting."
	exit 2
fi


if [ -e ${DCACHE_HOME}/bin/dcache-opt ] ; then
    echo ""
    echo "[INFO]  Moving ${DCACHE_HOME}/bin/dcache-opt out of the way, because it is obsolete."
    mv ${DCACHE_HOME}/bin/dcache-opt ${DCACHE_HOME}/bin/dcache-opt-obsolete
fi


#
# install SRM 
#

# put correct JAVA_HOME into srm_setup.env
( 
  grep -v JAVA_HOME ${DCACHE_HOME}/etc/srm_setup.env
  echo "JAVA_HOME=${JAVA_HOME}"
) > ${DCACHE_HOME}/etc/srm_setup.env.$$
mv ${DCACHE_HOME}/etc/srm_setup.env.$$ ${DCACHE_HOME}/etc/srm_setup.env

if [ "`printConfig SRM`" = yes ] ; then
  ${DCACHE_HOME}/install/deploy_srmv2.sh ${DCACHE_HOME}/etc/srm_setup.env
fi


# this needed by PNFS only . ignode if Chimera is used


if [ "x${NAMESPACE}" != "xchimera" ]
then

	# Creating /pnfs/fs and Symbolic Link /pnfs/fs/usr to /pnfs/<domain> (e.g. /pnfs/fnal.gov) for GridFTP

	if [ \( "`printConfig GRIDFTP`" = yes -o "`printConfig GRIDFTP`" = y \) -a ! "${nodeType}" = admin ] ; then

	pnfsMountPoint=${PNFS_ROOT}/${SERVER_ID}

	echo ""
	echo -n "[INFO]  Checking if ${pnfsMountPoint} mounted to the right export. ..."
	exportPoint=`mount | grep ${pnfsMountPoint} | awk '{print $1}' | awk -F':' '{print $2}'`
	if [ "${exportPoint}" = '/pnfsdoors' ] ; then
	   echo "OK"
	else
	   echo ""
	   if [ "${exportPoint}" ] ; then
	       echo "[WARN]  ${pnfsMountPoint} mounted, however not to ${ADMIN_NODE}:/pnfsdoors."
	       echo "        Unmounting it now:"
	       umount ${pnfsMountPoint}
	       echo ""
	   fi

	   if [ -L "${pnfsMountPoint}" ] ; then
	       echo "[INFO]  Trying to remove symbolic link ${pnfsMountPoint} :"
	       rm -f ${pnfsMountPoint}
	       if [ $? -eq 0 ] ; then
        	   echo "[INFO]  'rm -f ${pnfsMountPoint}' went fine."
	       else
        	   echo "[ERROR] 'rm -f ${pnfsMountPoint}' failed. Please move it out of the way manually"
        	   echo "        and run me again. Exiting."
        	   exit 1
	       fi
	   fi

	   if [ ! -d "${pnfsMountPoint}" ]; then
	       if [ -e "${pnfsMountPoint}" ] ; then
        	   echo "[ERROR] The file ${pnfsMountPoint} is in the way. Please move it out of the way"
        	   echo "        and call me again. Exiting."
        	   exit 1
	       else
        	   echo "[INFO]  Creating pnfs mount point (${pnfsMountPoint})"
        	   mkdir -p ${pnfsMountPoint}
	       fi
	   fi
	   echo "[INFO]  Will be mounted to ${ADMIN_NODE}:/pnfsdoors by dcache-core start-up script."
	fi

	echo ""
	if [ ! -L "${PNFS_ROOT}/ftpBase" -a ! -e "${PNFS_ROOT}/ftpBase" ] ; then
	   echo "[INFO]  Creating link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} which is used by the GridFTP door."
	   ln -s ${pnfsMountPoint} ${PNFS_ROOT}/ftpBase
	else
	   ftpBaseLinkedTo=`find ${PNFS_ROOT}/ftpBase -type l -printf '%l'`
	   if [ "${ftpBaseLinkedTo}" = "${pnfsMountPoint}" ] ; then
	       echo "[INFO]  Link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} already there."
	   else
	       echo "[ERROR] Link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} cannot be created. Needed by the GridFTP door."
	       echo "        Please move ${PNFS_ROOT}/ftpBase and run me again. Exiting."
	       exit 1
	   fi
	fi

	if ! grep "^ftpBase=${PNFS_ROOT}/ftpBase" ${DCACHE_HOME}/config/dCacheSetup 2>/dev/null >/dev/null ; then
	   echo ""
	   echo "[WARN]  The file ${DCACHE_HOME}/config/dCacheSetup does not contain:"
	   echo "           ftpBase=${PNFS_ROOT}/ftpBase"
	   echo "        Make shure it is set correctly before you start dCache."
	fi

	if mount | grep pnfs 2>/dev/null >/dev/null ; then
	   echo ""
	   echo "[WARN]  A pnfs export is already mounted. The GridFTP door will only use the"
	   echo "        mount at ${pnfsMountPoint} which will be mounted by the start-up script."
	   echo "        You might want to remove any mounts not needed anymore."
	fi
	echo ""
	fi


	if [ \( "${nodeType}" = admin -a ! "${startPnfsManager}" = "no" \) -o "${startPnfsManager}" = "yes" ] ; then

	    echo ""
	    #    Checking and creating mountpoint and link
	    #
	    pnfsMountPoint=${PNFS_ROOT}/fs
	    if [ ! -d "${pnfsMountPoint}" ]; then
			if [ -e "${pnfsMountPoint}" ] ; then
	            echo "[ERROR] The file ${pnfsMountPoint} is in the way. Please move it out of the way"
			    echo "        and call me again. Exiting."
			    exit 1
			else
			    echo "[INFO]  Creating pnfs mount point (${pnfsMountPoint})"
			    mkdir -p ${pnfsMountPoint}
			fi
	    fi
	    echo "[INFO]  Will be mounted to ${pnfsServer}:/fs by dcache-core start-up script."

	    cd ${PNFS_ROOT}
	    if [ ! -L "${SERVER_ID}" ]; then
			if [ -e "${SERVER_ID}" ] ; then
			    echo "[ERROR] The file/directory ${PNFS_ROOT}/${SERVER_ID} is in the way. Please move it out"
			    echo "        of the way and call me again. Exiting."
			else
			    echo "[INFO]  Creating link ${PNFS_ROOT}/${SERVER_ID} --> ${pnfsMountPoint}/usr/"
			    ln -s fs/usr ${SERVER_ID}
			fi
	    fi

	    cd ${PNFS_ROOT}
	    if [ ! -L "${SERVER_ID}" -a ! -e "${SERVER_ID}" ] ; then
        	echo "[INFO]  Creating link ${PNFS_ROOT}/${SERVER_ID} --> ${PNFS_ROOT}/fs/usr."
        	ln -s fs/usr ${SERVER_ID}
	    else
        	serverIdLinkedTo=`find ${PNFS_ROOT}/${SERVER_ID} -type l -printf '%l'`
        	if [ "${serverIdLinkedTo}" = "fs/usr" -o "${serverIdLinkedTo}" = "${PNFS_ROOT}/fs/usr" ] ; then
        	    echo "[INFO]  Link ${PNFS_ROOT}/${SERVER_ID} --> ${PNFS_ROOT}/fs/usr already there."
        	else
        	    echo "[ERROR] Link ${PNFS_ROOT}/${SERVER_ID} --> ${PNFS_ROOT}/fs/usr cannot be created."
        	    echo "        Please move ${PNFS_ROOT}/${SERVER_ID} and run me again. Exiting."
        	    exit 1
        	fi
	    fi

	    cd ${PNFS_ROOT}
	    if [ ! -L ${PNFS_ROOT}/ftpBase -a ! -e ${PNFS_ROOT}/ftpBase ] ; then
        	echo "[INFO]  Creating link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} which is used by the GridFTP door."
        	ln -s ${pnfsMountPoint} ${PNFS_ROOT}/ftpBase
	    else
        	ftpBaseLinkedTo=`find ${PNFS_ROOT}/ftpBase -type l -printf '%l'`
        	if [ "${ftpBaseLinkedTo}" = "${pnfsMountPoint}" ] ; then
        	    echo "[INFO]  Link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} already there."
        	else
        	    echo "[ERROR] Link ${PNFS_ROOT}/ftpBase --> ${pnfsMountPoint} cannot be created. Needed by the GridFTP door."
        	    echo "        Please move ${PNFS_ROOT}/ftpBase and run me again. Exiting."
        	    exit 1
        	fi
	    fi

	    # Checking for existance of rpcinfo
	    #

	    which rpcinfo 2>&1 > /dev/null
	    RC=$?
	    if [ ${RC} -ne 0 ] ; then 
        	echo "ERROR: rpcinfo is not on the path"
        	exit 1
	    fi

	    #    Checking and maybe starting pnfs server
	    #

	    RETVAL=0
	    rpcinfo -u localhost 100003 >/dev/null 2>&1
	    RETVAL=$?
	    if [ ${RETVAL} -eq 1 ]; then
        	echo ""
        	if [ ! -x ${PNFS_INSTALL_DIR}/tools/pnfs.server ]; then
        	    echo ""
        	    echo "[ERROR] PNFS not installed but needed for dCache admin node installation. Exiting."
        	    exit 1
        	fi
        	    echo ""
        	    echo "[INFO]  PNFS is not running. It is needed to prepare dCache. ... "
        	    yesno=`printConfig PNFS_START`
        	if [ \( "${yesno}" = "n" \) -o \( "${yesno}" = "no" \) ] ; then
        	    echo "[ERROR] Not allowed to start it. Set PNFS_START in etc/node_config to 'yes' or start by hand. Exiting."
        	    exit 1
        	elif [ \( "${yesno}" = "y" \) -o \( "${yesno}" = "yes" \) ] ; then
        	    echo "[INFO]  Trying to start it now:"
        	    ${PNFS_INSTALL_DIR}/tools/pnfs.server start
        	fi
	    fi

	    #    Checking pnfs mount and possibly mounting
	    #
	    echo ""
	    cp=`df ${pnfsMountPoint} 2>/dev/null | grep "${pnfsMountPoint}" | awk '{print $2}'`
	    if [ -z ${cp} ]; then
			echo "[INFO]  ${pnfsMountPoint} mount point exists, but is not mounted - going to mount it now ..."
			mount -o intr,rw,noac,hard,nfsvers=2 ${pnfsServer}:/fs ${pnfsMountPoint}
	    fi
	    cp=`df ${pnfsMountPoint} 2>/dev/null | grep "${pnfsMountPoint}" | awk '{print $2}'`
	    if [ -z $cp ]; then
			echo "[ERROR] Was not able to mount ${pnfsServer}:/fs to ${pnfsMountPoint}. Exiting."
			exit 1
	    fi

	    #     Checking if pnfs config exists
	    #
	    echo ""
	    echo "[INFO]  Checking on a possibly existing dCache/PNFS configuration ..."
	    if [ -f ${PNFS_ROOT}/fs/admin/etc/config/serverRoot ]; then
			WRITING_PNFS=no
	    else
			WRITING_PNFS=yes
	    fi

	    yesno=`printConfig PNFS_OVERWRITE`
	    if [ \( "${yesno}" = "n" -o "${yesno}" = "no" \) -a "${WRITING_PNFS}" = "no" ] ; then
			echo "[INFO]  Found an existing dCache/PNFS configuration!"
			echo "[INFO]  Not allowed to overwrite existing configuration."
			echo ""
	    elif [ \( "${yesno}" = "y" -o "${yesno}" = "yes" \) -a "${WRITING_PNFS}" = "no" ] ; then
			echo "[INFO]  Found an existing dCache/PNFS configuration!"
			echo ""
			echo "[WARN]  Overwriting existing dCache/PNFS configuration..."
			echo ""
			WRITING_PNFS=yes
			echo ""
			sleep 5
	    fi

	    #     Writing new pnfs configuration
	    #
	    if [ "${WRITING_PNFS}" = "yes" ] ; then

			cd ${PNFS_ROOT}/fs
			serverRoot=`cat ".(id)(usr)"`

			#     Writing Wormhole information
			#
			cd ${PNFS_ROOT}/fs/admin/etc/config
			echo "${fqHostname}" > ./serverName
			echo "${SERVER_ID}" >./serverId
			echo "$serverRoot ." > ./serverRoot

			touch ".(fset)(serverName)(io)(on)"
			touch ".(fset)(serverId)(io)(on)"
			touch ".(fset)(serverRoot)(io)(on)"

			echo "${fqHostname}" > ./serverName
			echo "${SERVER_ID}" >./serverId
			echo "$serverRoot ." > ./serverRoot

			mkdir -p dCache
			cd dCache
			echo "${fqHostname}:${dCapPort}" > ./dcache.conf
			touch ".(fset)(dcache.conf)(io)(on)"
			echo "${fqHostname}:${dCapPort}" > ./dcache.conf

        	#     Configure directory tags
        	#
			cd ${PNFS_ROOT}/fs/usr/data
			echo "StoreName myStore" > ".(tag)(OSMTemplate)"
			echo "STRING" > ".(tag)(sGroup)"
	    fi

	    #  ----  Mount point for doors
	    #    This is done, even if PNFS_OVERWRITE=no in order to
	    #    cleanly upgrade
	    #
	    echo ""
	    cd ${PNFS_ROOT}/fs/admin/etc/exports
	    if ! grep '^/pnfsdoors' * >/dev/null 2>/dev/null ; then
        	echo "[INFO]  Configuring pnfs export '/pnfsdoors' (needed from version 1.6.6 on)"
        	echo "        mountable by world."
        	echo '/pnfsdoors /0/root/fs/usr/ 30 nooptions' >> '0.0.0.0..0.0.0.0'
	    else
        	echo "[INFO]  There already are pnfs exports '/pnfsdoors' in"
        	echo "        /pnfs/fs/admin/etc/exports. The GridFTP doors need access to it."
			if grep '^/pnfsdoors' * | grep -v ':/pnfsdoors[^[:graph:]]\+/0/root/fs/usr/[^[:graph:]]\+' >/dev/null 2>/dev/null ; then
        	    echo "[WARN]  Make shure they all point to '/0/root/fs/usr/'! The GridFTP doors which"
        	    echo "        are not on the admin node will mount these from version 1.6.6 on."
        	fi
	    fi
	    echo "[INFO]  You may restrict access to this export to the GridFTP doors which"
	    echo "        are not on the admin node. See the documentation." 
	fi


fi # if chimera

if [ "${nodeType}" = "admin" ] ; then

    #     Install ssh keys for secure communication
    #
    echo ""
    echo "[INFO]  Generating ssh keys:"
    cd ${DCACHE_HOME}/config
    if [ -f ./server_key ]; then
		rm ./server_key; rm ./host_key
    fi
    ssh-keygen -b 768 -t rsa1 -f ./server_key -N ""
    ln -s /etc/ssh/ssh_host_key ./host_key
    echo ""
fi

if [ ! -r "${DCACHE_HOME}/config/${shortHostname}.poollist" ]; then
    rm -f "${DCACHE_HOME}/config/${shortHostname}.poollist"
    touch "${DCACHE_HOME}/config/${shortHostname}.poollist"
fi

#    Pool configuration
#
check_pool() {
    rt=`echo ${rrt} | awk '{print $1}'`
    size=`echo ${rrt} | awk '{print $2}'`
    
    if [ -d "${rt}/pool" ]; then
		WRITING_POOL=no
    else
		WRITING_POOL=yes
    fi
    
    if [ "${WRITING_POOL}" = "no" ] ; then
		yesno=`echo ${rrt} | awk '{print $3}'`
		if [ \( "${yesno}" = "n" \) -o \( "${yesno}" = "no" \) ] ; then
		    echo "[INFO]  Not overwriting pool at ${rt}."	
		elif [ \( "${yesno}" = "y" \) -o \( "${yesno}" = "yes" \) ] ; then
		    echo "[WARN]  Will overwrite pool at ${rt}."
		    WRITING_POOL=yes
		else
		    echo "[WARN]  Valid options for pool overwrite are y/yes or n/no. Assuming 'no'. "
		fi
    fi
    
    if [ "${WRITING_POOL}" = "yes" ] ; then	
		echo "[INFO]  Creating Pool" ${pn}
		rm -rf ${rt}/pool
		mkdir -p ${rt}/pool
		mkdir -p ${rt}/pool/data
		mkdir -p ${rt}/pool/control
		cp ${DCACHE_HOME}/config/setup.temp  ${rt}/pool/setup.orig
		cd ${rt}/pool
		sed -e "s:set max diskspace 100:set max diskspace ${size}:g" setup.orig > setup.temp
		sed -e "s:mover set max active 10:mover set max active ${NUMBER_OF_MOVERS}:g" setup.temp > setup        
    fi
    
    cd ${rt}/pool
    ds=`eval df -k . | grep -v Filesystem | awk '{ print int($2/1048576) }'`
    let val=${ds}-${size}
    if [ ${val} -lt 0 ]; then
		printf " Pool size exceeds partition size "
    else
		pnl=`grep "${pn}" ${DCACHE_HOME}/config/${shortHostname}.poollist | awk '{print $1}'`
		if [ -z "${pnl}" ]; then	   
			echo "${pn}  ${rt}/pool  sticky=allowed recover-space recover-control recover-anyway lfs=precious tag.hostname=${shortHostname}" \
			>> ${DCACHE_HOME}/config/${shortHostname}.poollist
		fi
    fi
}

echo ""

if [ ! -r ${DCACHE_HOME}/etc/pool_path ]; then
    echo "[WARN]  ${DCACHE_HOME}/etc/pool_path does not exist. No pools will be configured."
else
# For all the static areas under rt make a pool
    let x=0
    while read rrt; do
		let x=${x}+1 
	    pn=${shortHostname}"_"${x}
		check_pool
    done  < ${DCACHE_HOME}/etc/pool_path
fi

exit 0
