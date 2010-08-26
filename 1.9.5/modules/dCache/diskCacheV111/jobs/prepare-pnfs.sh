#!/bin/sh
#
xid=`id`
x=`expr "${xid}" : "uid=\([0-9]*\)(.*" 2>/dev/null`
if [ "${x}" != "0" ] 
  then
     echo "You need to be root to run this script ... " >&2
     exit 4
  fi
#
# split the arguments into the options -<key>=<value> and the 
# positional arguments.
#
DEFINED="XDEFINEDX"
   args=""
   opts=""
   while [ $# -gt 0 ] ; do
     if expr "$1" : "-.*" >/dev/null ; then
        a=`expr "$1" : "-\(.*\)" 2>/dev/null`
        key=`echo "$a" | awk -F= '{print $1}' 2>/dev/null`
        value=`echo "$a" | awk -F= '{print $2 }' 2>/dev/null`
        if [ -z "$value" ] ; then a="${key}=${DEFINED}" ; fi
        eval "$a"
        a="export ${key}"
        eval "$a"
        opts="${opts} $1"
     else
        args="${args} $1"
     fi
     shift 1
   done
#
if [ ! -z "$args" ] ; then
   set `echo "$args" | awk '{ for(i=1;i<=NF;i++)print $i }'`
fi
#
if [ "${help}" = $DEFINED ] ; then
   echo ""
   echo " Options : "
   echo "   -pnfs=<pnfsBaseDirectory>"
   echo "   -domain=<DomainNameToBeUsed>"
   echo "   -host=<ourHostName>"
   echo "   -dcache.conf=<dcache.conf>"
   exit 0 
fi
#     end of init
#
getDomainName() {

  res=`grep search /etc/resolv.conf | grep -v "#" 2>/dev/null`
  if [ -z "$res" ]
    then
      res=`grep domain /etc/resolv.conf 2>/dev/null`
  fi
  if [ ! -z "${res}" ]
    then
      echo ${res} | awk '{ print $2 }' 2>/dev/null
      return 0
  fi  
  return 1
}
autoDomainName=`getDomainName`
if [ -z "$domain" ]
  then
    domain=${autoDomainName}
fi
if [ -z "${domain}" ] 
  then
  
  echo "Domain neither specified nor could it be autodetected." >&2
  exit 4
fi
#
#  The the Full specified Hostname
#
host=`hostname 2>/dev/null`
if [ `expr "${host}" : ".*\." 2>/dev/null` -eq 0 ] 
  then
    host=${host}.${domain}
fi
#
#  Get the pnfs mount point (if not specified)
#
isPnfsBaseDirectory() {
   if [ \( ! -f "${mp}/.(const)($$)" \) -o \
        \( ! -f "${mp}/README" \) -o \
        \( ! -d "${mp}/usr"    \) -o \
        \( ! -d "${mp}/admin" \) ] 
      then
        return 0
   else
        return 1
   fi 

}
if [ -z "${pnfs}" ] ; then
  pnfs=`df -k 2>/dev/null | grep "localhost:" | awk '{ print $1,$NF }' | \
  while read dummy mp
       do

     if [ \( ! -f "${mp}/.(const)($$)" \) -o \
          \( ! -f "${mp}/README" \) -o \
          \( ! -d "${mp}/usr"    \) -o \
          \( ! -d "${mp}/admin" \)  -o \
          \( ! -d "${mp}/admin/etc/config" \) ] 
        then
          continue
     fi 
     
     echo "${mp}"
     exit 0
  done`
else
  mp=${pnfs}
  if [ \( ! -f "${mp}/.(const)($$)" \) -o \
       \( ! -f "${mp}/README" \) -o \
       \( ! -d "${mp}/usr"    \) -o \
       \( ! -d "${mp}/admin"  \) -o \
       \( ! -d "${mp}/admin/etc/config" \) ] 
     then
       echo "The specified pnfs directory (${pnfs}) is invalid" >&2
       pnfs=""
  fi 
  
fi
if [ -z "${pnfs}" ] 
  then
    echo "Couldn't find a valid pnfs base directory" >&2
    echo "Please mount pnfs via localhost:/fs" >&2
    exit 4
fi
#
usrId=`cat "${pnfs}/.(id)(usr)" 2>/dev/null`
if [ -z "${usrId}" ] ; then
    echo "Couldn't determine pnfsId of 'usr'" >&2
    exit 4
fi
#
[ -z "$dcacheconf" ] && dcacheconf=${host}:22125
#
echo "       Pnfs : ${pnfs}"
echo "     Domain : ${domain}"
echo "       Host : ${host}"
echo "     usr ID : ${usrId}"
echo "dcache.conf : ${dcacheconf}"
echo ""
#
while : 
 do
    printf "Is this ok [yes/no] : "
    read result
    if [ "${result}" = "no" ] 
      then
       echo ""
       echo "Please run script with appropriate options : "
       echo ""
       echo "    ...   -domain=<domain>"
       echo "          -host=<hostname>"
       echo "          -pnfs=<pnfsMount>"
       echo "          -dcacheconf=<dcache.conf>"
       echo ""
       exit 1
    fi
    [ "${result}" = "yes" ]  && break
done
#
#   preparing pnfs config
#
cd ${pnfs}/admin/etc/config
touch ./serverName
touch ./serverId
touch ./serverRoot
touch ".(fset)(serverName)(io)(on)"
touch ".(fset)(serverId)(io)(on)"
touch ".(fset)(serverRoot)(io)(on)"
echo "${host}" >./serverName
echo "${domain}" >./serverId
echo "${usrId} ." >./serverRoot
mkdir dCache 2>/dev/null
cd dCache 2>/dev/null
if [ $? -ne 0 ] ; then
  echo "Can't change to dCache configuration dir." 2>/dev/null
  exit 6
fi
touch ./dcache.conf
touch ".(fset)(dcache.conf)(io)(on)"
echo "${host}:22125" >./dcache.conf
echo ""
echo "Done"
exit 0
