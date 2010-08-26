#!/bin/sh
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

. ~enstore/dcache-deploy/config/dCacheSetup

set -u  
out=`dirname $billingDb`/dcache-logins
out=~enstore/dcache-logins
if [ ! -r $out ]; then mkdir $out; fi
rm $out/l.* 2>/dev/null

lt=0

oldIFS="$IFS"
IFS="
"
for line in ~enstore/dcache-deploy/config/door*.batch;do
  basename=`basename $line`
  door=`echo $basename | sed -e 's/.batch//' -e 's/door//'`
  cmd="exit
set dest DCap${door}@door${door}Domain
info
exit
exit"
   echo DCap${door}@door${door}Domain: ssh -c blowfish -p $sshPort $serviceLocatorHost
   echo "$cmd" | ssh -c blowfish -p $sshPort $serviceLocatorHost > $out/l.$door 2>/dev/null
   now=`date +"%Y-%m-%d:%H:%M:%S"`

   ll=`grep 'Logins/max' $out/l.$door`
   l=`echo $ll | sed -e 's,.*: ,,' -e 's,/.*,,'`; if [ -z "$l" ]; then l=0; fi
   lt=`expr $lt + $l` ; echo $lt >| /$out/lt
   
   echo $now $l >> $out/L.$door 2>/dev/null
done
IFS="$oldIFS"

lt=`cat /$out/lt` 
echo `date +"%Y-%m-%d:%H:%M:%S"`  $lt >> $out/L.alldoors 2>/dev/null

cmd="exit
set dest Prestager@dCacheDomain
info
exit
exit"
echo Prestager@dCacheDomain: ssh -c blowfish -p $sshPort $serviceLocatorHost
echo "$cmd" | ssh -c blowfish -p $sshPort $serviceLocatorHost > $out/l.stage 2>/dev/null
now=`date +"%Y-%m-%d:%H:%M:%S"`
l=`grep 'Outstanding' $out/l.stage|awk '{print $NF}'`
echo $now $l >> $out/L.stage 2>/dev/null

#last1week=`date -d '1 week ago' +'%Y-%m-%d:00:01'`
#last6months=`date -d '6 months ago' +'%Y-%m-%d:00:01'`
#lastmonday=`date -d 'last monday' +'%Y-%m-%d:00:01'`

lastmonth=`date -d '1 month ago' +'%Y-%m-%d:00:01'`
tomorrow=`date -d '1 day' +'%Y-%m-%d:00:01'`

web=/fnal/ups/prd/www_pages/dcache/logins
if [ ! -r $web ]; then web=/tmp;fi

# adjust path for convert (either in /usr/bin or /usr/X11R6/bin
PATH=`$E_H/dropit /usr/X11R6/bin`; PATH=/usr/X11R6/bin:$PATH

for i in $out/L.*; do
  f=`basename $i |sed -e 's/L\.//'`
  echo $f ..............................................
  file=`mktemp /tmp/cronplot.XXXXXXXX`
  count=`tail -n1 $i | awk '{print $2}'`
  now=`date`
  echo "
set term postscript enhanced color solid 'Helvetica' 10
set output '$web/$f.ps'
set xlabel 'Date'
set label 'Plotted $now'  at graph 1.01,0 rotate font 'Helvetica,10'
set timefmt '%Y-%m-%d:%H:%M:%S'
set xdata time
set xrange ['$lastmonth' :'$tomorrow']
set yrange [0:]
set key left box
set grid
set title '$f'
set format x '%m-%d'
set label '$count' at graph .2,.5 font 'Helvetica,80'
plot '$i' using 1:2 t 'login count' with impulses
" >> $file
 gnuplot $file
 rm $file
 convert -rotate 90 $web/$f.ps $web/$f.jpg
 convert -rotate 90 -geometry 120x120  $web/$f.ps $web/${f}_stamp.jpg
done

node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`
if [ $node = fcdfdcache6 ]; then slh=cdfdca; else slh=$serviceLocatorHost;fi

# move data to monitoring node if required
SH_IP=`host $slh|grep 'has address'|awk '{print $NF}'`
THIS_IP=`host $node|grep 'has address'|awk '{print $NF}'`
if [ "$SH_IP" != "$THIS_IP" ];then
   echo rcp $web   $slh:$web/
        rcp $web/* $slh:$web/
fi
