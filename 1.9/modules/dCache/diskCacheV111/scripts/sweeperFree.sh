#!/bin/sh
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-r" ] ; then SWEEP=1; shift; else SWEEP=0;  fi
if [ "${1:-}" = "-q" ] ; then QUIET=1; shift; else QUIET=0;  fi

node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`

if [ -r /usr/local/bin/ENSTORE_HOME ]; then
   . /usr/local/bin/ENSTORE_HOME
else
   echo `date` ERROR: Can NOT determine E_H.  Add /usr/local/bin/ENSTORE_HOME link
   exit 1
fi

. $E_H/dcache-deploy/scripts/setup-enstore
set +u;. $E_H/dcache-deploy/config/dCacheSetup; set -u

out=`dirname $billingDb`/dcache-queues
out=~enstore/dcache-queues

minGigFree=2
bytes=`python -c "print $minGigFree.0 * 1073741824."|cut -f1 -d. ` #absolutely ridiculous, but expr is broken
for file in $out/q.*;do
  pool=`echo $file | sed -e 's/.*q\.//'`
  #Domain=`echo $pool | cut -f2 -d-`Domain
  #if [ `echo $Domain | grep -c '^data'` -ne 0 ]; then Domain=fcdf$Domain;fi  #HACK!
  Domain="${pool}Domain"
  Domain=`echo $Domain | sed -e 's/r-data/r-fcdfdata/' | sed -e 's/w-data/w-fcdfdata/'` #HACK!
  total=`grep Total $file | cut -f2 -d:|cut -f1 -dG`
  free=`grep Free $file | cut -f2 -d:`
  total=${total:-"-1"}
  free=${free:-0}
  freeG=`python -c "print $free.0 / 1073741824."|cut -f1 -d. ` #absolutely ridiculous, but expr is broken
  if [ $QUIET -eq 0 ];then 
    echo $pool $total $freeG
  fi
  
  if [ $total -gt 0 -a $freeG -lt $minGigFree -a $freeG -ge 0 ]; then
     if [ $QUIET -eq 1 ];then #not quiet on too much
        echo $pool $total $freeG
     fi
     cmd="exit
set dest $pool@$Domain
sweeper free $bytes
exit
exit"
     #echo "$cmd"
     echo $pool@$Domain ssh -c blowfish -p $sshPort $serviceLocatorHost
     if [ $SWEEP -eq 1 ]; then
        echo "$cmd" | ssh -c blowfish -p $sshPort $serviceLocatorHost 
     fi
  fi
done
