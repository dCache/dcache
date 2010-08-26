#!/bin/sh
UTILMODULES="central_certs userhandling yaimlog check_users_conf_format users_getspecialusers cron_job requires config_file run set_mysql_passwd split_quoted_variable users_getfirstpoolaccount users_getprduser users_getsgmuser users_getvogroup users_getvoprefix "
#UTILSDIR=`pwd`/utils
#MODULEDIR=`pwd`/modules/
CONFIGTARGETS="config_se_dcache"


help()
{
  echo "-s --site-info [site-info.def]      Siteinfo"
  echo "-c --configure [target]             select the target or 'all'"
  echo "-d --dcache-home [/opt/d-cache]     The location of dcache home"
  echo "-l --list                           List all the configuration targets."
  echo "-m --dirmodules                     Directory with configuration modules."
  echo "-u --dirutils                       Directory with Utility Modules."
  echo "-h --help                           show this help."
  
}


getDcacheInstallPrefix()
{
  if [ -n "${HOMEDCACHE}" ] ; then
    RET=${HOMEDCACHE}
    return
  fi
  if [ -z "${HOMEDCACHE}" ] ; then
    HOMEDCACHE="/opt/d-cache/"
  fi
  RET=${HOMEDCACHE}
}


getModulesDir()
{
  local testDir
  RET=""
  if [ -n "${MODULEDIR}" ] ; then
    
    RET=${MODULEDIR}
    return
  fi
  testDir=`pwd`/modules
  if [ -d "${testDir}" ] ; then
    RET=${testDir}
    return
  else
    getDcacheInstallPrefix
    testDir="${RET}/share/dCacheConfigure/modules/"
    if [ -d "${testDir}" ] ; then
      RET=${testDir}
      return
    fi
  fi
  return
}

getUtilsDir()
{
  local testDir
  RET=""
  if [ -n "${UTILSDIR}" ] ; then
    RET=${UTILSDIR}
    return
  fi
  testDir=`pwd`/utils
  if [ -d "${testDir}" ] ; then
    RET=${testDir}
    return
  else
    getDcacheInstallPrefix
    testDir="${RET}/share/dCacheConfigure/utils/"
    RET=""
    if [ -d "${testDir}" ] ; then
      RET=${testDir}
      return
    fi
  fi
  return
}


requiresAlist=0
while [ $# -ne 0 ]
do	
  # Default to shifting to next parameter
  shift_size=1		
  if [ $1 = "--help" -o $1 = "-h" ] 
  then
    
    help
    exit 0
  fi
  if [ $1 = "--site-info" -o $1 = "-s" ]
  then
    
    SITEINFOFILELOCATION=$2
    shift_size=2	
  fi
  if [ $1 = "--configure" -o $1 = "-c" ]
  then
    
    SELECTEDMODULES="${SELECTEDMODULES} ${2}"
    shift_size=2
  fi
  if [ $1 = "--list" -o $1 = "-l" ]
  then
    
    requiresAlist=1
  fi
  if [ $1 = "--dcache-home" -o $1 = "-d" ]
  then
    HOMEDCACHE=$2
    shift_size=2
  fi
  if [ $1 = "--dirmodules" -o $1 = "-m" ]
  then
    MODULEDIR=$2
    shift_size=2
  fi
  if [ $1 = "--dirutils" -o $1 = "-u" ]
  then
    UTILSDIR=$2
    shift_size=2
  fi
  if [ $# -ge $shift_size ]
  then
    # Shift correctly for paramters
    shift $shift_size
  else
    # Ok so a paramaterised function contains no valid attribute
    echo "${0} parameter ${1} requires an attribute"
    exit 1
  fi
done
getUtilsDir
UTILSDIR=${RET}
getModulesDir
MODULEDIR=${RET}

if [ "${UTILSDIR}" == "" ] ; then
  help
  echo Utility directory not found please specify it on the command line.
  exit 1
fi

if [ "${MODULEDIR}" == "" ] ; then
  help
  echo Modules directory not found please specify it on the command line.
  exit 1
fi

if [ ! -d "${UTILSDIR}" ] ; then
  echo Utility directory ${UTILSDIR} does not exist.
  exit 1
fi

if [ ! -d "${MODULEDIR}" ] ; then
  echo Modules directory ${MODULEDIR} does not exist.
  exit 1
fi

for util in ${UTILMODULES}
do
  if [ -f "${UTILSDIR}/${util}" ] ; then
    . ${UTILSDIR}/${util}
  else
    echo "ERROR UTIL '${UTILSDIR}/${util}' not found"
    exit 1
  fi
done

# now source all the modules.
for module in `ls ${MODULEDIR}/`
do
  if [ -f "${MODULEDIR}/$module" ] ; then
    worthSrcing=$(grep ${module}_check ${MODULEDIR}/$module)
    # Evil code which should be deleted when lfield fixes his code
    if [ "${module}" == "config_bdii_only" -o "${module}" == "config_gip_only" ] ; then
      worthSrcing="yes"
    fi
    # Evil code end.
    if [ -n "${worthSrcing}" ] ; then
      . ${MODULEDIR}/${module}
      #echo ${module}_check
      ${module}_check 2> /dev/null
      moduleRet=$?
      if [ "${moduleRet}" == "0" ] ;  then
        AVAILABLEMODS="${AVAILABLEMODS} ${module}"
      fi
    else
      echo "INFO module '$module' does not have a ${module}_check function."
    fi
  else
    echo "INFO module '$module' not found"
    exit 1
  fi
done

# now list the modules
if [ "${requiresAlist}" == "1" ] ; then 
  echo "The Following configuration modules exist"
  for module in ${AVAILABLEMODS}
  do
    echo $module
  done
  exit 0
fi

VERIFIEDMODS=""
for selmodule in ${SELECTEDMODULES}
do
  if [ "$selmodule" == "all" ] ; then
    VERIFIEDMODS=$AVAILABLEMODS
  else
    for vermodule in ${AVAILABLEMODS}
    do
      if [ "${vermodule}" == "${selmodule}" ] ; then 
        VERIFIEDMODS="${VERIFIEDMODS} ${vermodule}"
      fi
    done
  fi
done

if [ "${AVAILABLEMODS}" == "" ] ; then
  echo No modules available please check your modules directory.
  echo Modules directory is currently set to ${MODULEDIR}.
  exit 1
fi

if [ "${VERIFIEDMODS}" == "" ] ; then
  echo "No valid modules selected to be run."
  echo "The following modules can be selected"
  for amodule in ${AVAILABLEMODS}
  do
    echo ${amodule}
  done
  exit 1
fi

if [ -z "$SITEINFOFILELOCATION" ] ; then
  echo site-info file not set.
  help
  exit 1
fi

if [ ! -f "$SITEINFOFILELOCATION" ] ; then
  echo site-info.def file not found.
  exit 1
fi
. $SITEINFOFILELOCATION

# Now run the modules
for module in ${VERIFIEDMODS}
do
  ${module}_run
done
