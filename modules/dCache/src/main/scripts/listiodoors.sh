#!/bin/sh

if [ $# -lt 1 ] ; then
   echo "Usage : ... <doorDomainName>" >&2
   exit 4
fi
doordomain=$1

USERX=""
#USERX="-l admin"

node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`

if [ -r /usr/local/bin/ENSTORE_HOME ]; then
   . /usr/local/bin/ENSTORE_HOME
else
   echo `date` ERROR: Can NOT determine E_H.  Add /usr/local/bin/ENSTORE_HOME link
   exit 1
fi

set +u;. $E_H/dcache-deploy/config/dCacheSetup; set -u

TMP=/tmp/$$-1
TMP0=/tmp/$$-0
TMP2=/tmp/$$-2
TMP3=/tmp/$$-3
TMP4=/tmp/$$-4
rm -rf $TMP0 $TMP $TMP2 $TMP3 $TMP4 2>/dev/null

msg="UNRESPONSIVE - query has been killed for door $doordomain after  seconds"

echo "exit
set dest System@${doordomain}
ps -f
exit
exit
" > $TMP0

cmd="ssh -vvv -1 -x -t -a -c blowfish ${USERX} -p ${sshPort} $serviceLocatorHost"
$E_H/dcache-deploy/dcache-fermi-config/timed_cmd.sh --buffer_stdin 60 $cmd <$TMP0 >$TMP  2>/dev/null

cat $TMP | egrep " DCapDoor |FTP.*nknown" | tr -d "\r"|sed "s/A / A /" | awk '{ if( NF > 0 )printf "ps -f %s\n",$1 }' >>$TMP3

tfile_size=`stat  $TMP3|grep Size |awk '{print $2 }'`

if [ $tfile_size != 0  ];then 

  #
  #   scan the client jobs list and prepares
  #   a dCache executable which 'ps -f <cells>'
  #   for each client door cell.
  #
  echo exit >$TMP2 
  echo "set dest System@${doordomain}" >>$TMP2
  cat $TMP3  >>$TMP2
  echo "exit" >>$TMP2
  echo "exit" >>$TMP2

  cmd="ssh -vvv -1 -x -t -a -c blowfish ${USERX} -p ${sshPort} $serviceLocatorHost"
  $E_H/dcache-deploy/dcache-fermi-config/timed_cmd.sh --buffer_stdin 60 $cmd <$TMP2 >$TMP4  2>/dev/null
  #cat $TMP4

  #
  #  make a "nice" output of if. One line per file.
  #  Format :
  #
  #   <dCacheJobNumber> 
  #   <clientIpNumber>  
  #   <requestType=[io/stage/check]>
  #   [<assignedPoolName>]
  #   [<pnfsId>][<fileSequenceNumber>][<clientUserId>][<clientProcessId>]
  #   <currentStatus>(<time spend in this status>)

  cat $TMP4 | tr -d "\r"|tr "[]()" "    " |\
    awk -v domain=${doordomain}  '{
    if( $1 == "uid")
    {
      uid=$3
    }
    if($1 =="pid")
    {
      pid=$3
    }
    if( $5 == "DCapDoor" ){ 
        doortype="DCAP"
        cell=$1 
        split(cell,a,"-")
        cell=a[1] 
	host=$6 
    }
    if( $4 == "DCapDoor" ){ 
        doortype="DCAP"
        cell=$1 
        split(cell,a,"-")
        cell=a[1] 
	host=$5
    }
    if( $2 == "->" ){
       pool=$4
       pnfsid=$5
       transfer_num=$7
       uid=$8
       pid=$9
       status=$10
       seconds=$11
       printf"<tr>"
       printf "<td>%s</td><td>%s</td><td> %s</td><td> %s</td> ",uid,pid,cell,host
       printf "<td>%s</td>",pool
       printf "<td>%s</td>",pnfsid
       printf "<td>%s</td>",transfer_num
       printf "<td>%s</td>",status
       printf "<td>%s</td>",seconds
       printf "</tr>"
       printf "\n"
    }

    if( $1 == "User")
    {
      uid=$3
    }
    if ( $1 ~ /FTP-/ )  {
        doortype="FTP"
        cell=$1 
	host=$6 
        split(host,a,"@")
        split(a[2],b,"/")
        if( length(b[1]) == 0 ){ host=b[2] }else{ host=b[1] } ;
    }
    if( $1 == "Last")
    {
       cmd=$4
       f="???"
       if( $c == "RETR")
       {
          f=$5
       }
       if( $c == "STOR")
       {
          f=$5
       }
    }
    if( $2 == "Count")
    {
       if ( doortype != "DCAP" ) 
       {
         transfer_num = $4
         printf"<tr>"
         printf "<td>%s</td><td>%s</td><td> %s</td><td> %s</td> ",uid,"pid",cell,host
         printf "<td>%s</td>","pool"
         printf "<td>%s</td>","pnfsid"
         printf "<td>%s</td>",transfer_num
         printf "<td>%s</td>","status"
         printf "<td>%s</td>","seconds"
         printf "</tr>"
         printf "\n"
       }
    }

    if( $1 == "UNRESPONSIVE")
    {
      printf $0
    }
    }' 
    cat $TMP4 | tr -d "\r" >/tmp/$doordomain
else
    rm -fr /tmp/$doordomain
fi

rm -rf $TMP0 $TMP $TMP2 $TMP3 $TMP4 2>/dev/null
exit 0
