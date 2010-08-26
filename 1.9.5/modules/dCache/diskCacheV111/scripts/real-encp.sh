#!/bin/sh
out=/tmp/real-encp/`date +'%Y-%m-%d:%H:%M:%S'`.$$.$1.$2
dir=`dirname $out`
if [ ! -d $dir ]; then mkdir $dir; fi
export out
exec >>$out 2>&1 <&-

set -xv

atrap1() { if [ -n "${LOGFILE-}" ]; then  echo `date` trapped SIGHUP >>$LOGFILE; fi
                                          echo `date` trapped SIGHUP
          }
atrap2() { if [ -n "${LOGFILE-}" ]; then  echo `date` trapped SIGINT >>$LOGFILE; fi
                                          echo `date` trapped SIGINT
          }
trap atrap1 1
trap atrap2 2

node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`

if [ -r /usr/local/bin/ENSTORE_HOME ]; then
   . /usr/local/bin/ENSTORE_HOME
else
   echo `date` ERROR: Can NOT determine E_H.  Add /usr/local/bin/ENSTORE_HOME link
   exit 1
fi
. $E_H/dcache-deploy/config/dCacheSetup  # to get defaultPnfsServer

LOGFILE=$E_H/dcache-log/real-encp.log

echo ---------------------- >> $LOGFILE
echo ----------------------
date >> $LOGFILE
date
#
echo $0 >>$LOGFILE
echo $0
echo "args: " $* >> $LOGFILE
echo "args: " $*
#
if [ $# -lt 3 ] ;then
   echo "Illegal usage : $0" >>$LOGFILE
   exit 4
fi
#
command=$1
pnfsid=$2
filepath=$3
shift; shift; shift;
#
# find -pnfs=<pnfs root>
#
pnfs_root=""
while [ $# -gt 0 ] ;do
	if expr "$1" : "-pnfs=" ; then
		pnfs_root=`echo $1 | sed -e "s/^-pnfs=//g"`
	elif expr "$1" : "-si="; then
	    size=`echo $1 | cut -d';' -f1 |sed -e "s/^-si=//g" | cut -d'=' -f2`
	fi
	shift
done
if [ -z "$pnfs_root" ] ;then
	echo Warning: PNFS root not specified or not found >> $LOGFILE
fi

echo $command $pnfsid $filepath $pnfs_root>> $LOGFILE

#sleep 10
#dd if=/dev/zero of=$filepath bs=$size count=1
#exit 0

#
#  ENCP specific setup
#
# Because there are several different ways to access encp, we've
# made it the responsibility of the calling shell to make sure it's in the path

ENCP=encp
echo encp: `which encp` >>$LOGFILE 2>&1
echo command: $command >>$LOGFILE 2>&1

#===============
source $E_H/dcache-deploy/scripts/encp.options
echo encp options are \" $options \" >>$LOGFILE 2>&1
echo `date` encp options are \" $options \"
#===============

#
if [ "$command" = "get" ] ; then
  if [ -z "$pnfs_root" ] ;then
	echo encp command: $ENCP $options --get-cache $pnfsid $filepath >>$LOGFILE 2>&1
	$ENCP $options --get-cache $pnfsid $filepath >>$LOGFILE 2>&1
  else
	echo encp command: $ENCP $options --pnfs-mount $pnfs_root --get-cache $pnfsid $filepath >>$LOGFILE 2>&1
	                   $ENCP $options --pnfs-mount $pnfs_root --get-cache $pnfsid $filepath >>$LOGFILE 2>&1
  fi
  ENCP_EXIT=$?
  echo `date` encp exit status $ENCP_EXIT >>$LOGFILE
  echo `date` encp exit status $ENCP_EXIT 
  if [ $ENCP_EXIT -eq 0 ]; then rm -f $out; fi
  exit $ENCP_EXIT

elif [ "$command" = "put" ] ; then
  if [ -z "$pnfs_root" ] ;then
	echo PUT COMMAND: $ENCP $options --put-cache $pnfsid $filepath >>$LOGFILE 2>&1
  	                  $ENCP $options --put-cache $pnfsid $filepath >>$LOGFILE 2>&1
        ENCP_EXIT=$?
  else

    #a user may put a file into the dcache, execute an 'rm' on the pnfs
    #mount and then later when the dcache tries to upload the file there 
    #isn't a pnfs id anymore!

    #XXX this code should be improved to handle this case using
    #enstore or pnfs commands not weird crap like this

    #check if the file is still in the pnfs database
    cd $pnfs_root
    if cat ".(nameof)($pnfsid)"; then
        #check
        echo PUT COMMAND `date` - $PPID : $ENCP $options --pnfs-mount $pnfs_root --put-cache $pnfsid $filepath >>$LOGFILE 2>&1
                                          $ENCP $options --pnfs-mount $pnfs_root --put-cache $pnfsid $filepath >>$LOGFILE 2>&1
    else
        echo "The file seems to have been delete from pnfs space.  We're going to stash it away in case that comes back to haunt us" >> $LOGFILE

        #first check what machine hosts the mount
        mountHost=`mount |grep /pnfs/fs | cut -f1 -d":"`
        
        #then get the filename from the host
        destinationName=`rsh $defaultPnfsServer "grep $pnfsid /diska/pnfs/log/pnfsd.log.names /diska/pnfs/log/pnfsd.log | grep name | grep -v nameof | sed 's/.* name //' | cut -f1 -d\ |tail -n1  " 2>/dev/null`

        #the filename is only the filename, not the full path.  So
        #we'll add the date to make it unique (man I hope this code is
        #never the difference between success and failure!)

        destinationName=$pnfs_root/usr/dcache_trash_bin/$destinationName.`date +%s`

        #now we can copy the file into enstore
        echo $ENCP $options $filepath $destinationName >>$LOGFILE 2>&1
             $ENCP $options $filepath $destinationName >>$LOGFILE 2>&1
    fi

    ENCP_EXIT=$?

    echo PUT COMMAND `date`: done $size : exit code $ENCP_EXIT>> $LOGFILE 2>&1

    #XXX - dcache creates the pnfs entry, then when encp copies it into enstore and
    #updates the file size pnfs sets it to 0.  Need to talk with PF about this
    #for now, just change it back again
    if [ $ENCP_EXIT -eq 0 ]; then
	cd /pnfs/fs/usr
	filename=`enstore pnfs --info=$pnfsid |grep original_name |cut -d' ' -f2` 
	echo setting filesize for $filename to $size >> $LOGFILE 2>&1
	cd `dirname $filename`
	echo $PWD  >> $LOGFILE 2>&1
	echo touch ".(fset)(`basename $filename`)(size)($size)"  >> $LOGFILE 2>&1
	touch ".(fset)(`basename $filename`)(size)($size)"  >> $LOGFILE 2>&1
    fi
  fi
  echo `date` encp exit status $ENCP_EXIT >>$LOGFILE
  echo `date` encp exit status $ENCP_EXIT
  if [ $ENCP_EXIT -eq 0 ]; then rm -f $out; fi
  exit $ENCP_EXIT
else
  echo " Command not yet supported : $command" >>$LOGFILE
  exit 5
fi
# how did we get here?
exit 99
