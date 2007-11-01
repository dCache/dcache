#!/bin/sh
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

. ~enstore/dcache-deploy/config/dCacheSetup

set -u  
out=`dirname $billingDb`/dcache-queues
out=~enstore/dcache-queues
if [ ! -r $out ]; then mkdir $out; fi
rm $out/q.* 2>/dev/null

mat=0
mqt=0
sat=0
sqt=0
rat=0
rqt=0

oldIFS="$IFS"
IFS="
"
for line in `cat ~enstore/dcache-deploy/config/[rw]*.poollist`;do
  pool=`echo $line | awk '{print $1}'`
  #Domain="`echo $line | cut -f2 -d-`Domain"
  #if [ `echo $Domain | grep -c '^data'` -ne 0 ]; then Domain=fcdf$Domain;fi  #HACK!
  Domain="${pool}Domain"
  Domain=`echo $Domain | sed -e 's/r-data/r-fcdfdata/' | sed -e 's/w-data/w-fcdfdata/'` #HACK!
  cmd="exit
set dest $pool@$Domain
info
exit
exit"
   #echo $pool@$Domain 
   #echo "$cmd"
   echo $pool@$Domain ssh -c blowfish -p $sshPort $serviceLocatorHost
   echo "$cmd" | ssh -c blowfish -p $sshPort $serviceLocatorHost > $out/q.$pool 2>/dev/null
   now=`date +"%Y-%m-%d:%H:%M:%S"`

   mql=`grep 'Mover Queue' $out/q.$pool`
   ma=`echo $mql | sed -e 's,.* ,,' -e 's,(.*,,'`; if [ -z "$ma" ]; then ma=0; fi
   mat=`expr $mat + $ma` ; echo $mat >| /$out/mat
   mq=`echo $mql | sed -e 's,.*/,,'`             ; if [ -z "$mq" ]; then mq=0; fi
   mqt=`expr $mqt + $mq` ; echo $mqt >| /$out/mqt

   sql=`grep 'to store'  $out/q.$pool`
   sa=`echo $sql | sed -e 's,.* ,,' -e 's,(.*,,'`; if [ -z "$sa" ]; then sa=0; fi
   sat=`expr $sat + $sa` ; echo $sat >| /$out/sat
   sq=`echo $sql | sed -e 's,.*/,,'`             ; if [ -z "$sq" ]; then sq=0; fi
   sqt=`expr $sqt + $sq` ; echo $sqt >| /$out/sqt

   rql=`grep 'from store'  $out/q.$pool`
   ra=`echo $rql | sed -e 's,.* ,,' -e 's,(.*,,'`; if [ -z "$ra" ]; then ra=0; fi
   rat=`expr $rat + $ra` ; echo $rat >| /$out/rat
   rq=`echo $rql | sed -e 's,.*/,,'`             ; if [ -z "$rq" ]; then rq=0; fi
   rqt=`expr $rqt + $rq` ; echo $rqt >| /$out/rqt

   s1=`expr $ma + $mq`
   s2=`expr $s1 + $sa`
   s3=`expr $s2 + $sq`
   s4=`expr $s3 + $ra`
   s5=`expr $s4 + $rq`
   
   echo $now $ma $mq $sa $sq $ra $rq X $ma $s1 $s2 $s3 $s4 $s5 >> $out/Q.$pool 2>/dev/null
done
IFS="$oldIFS"

ma=`cat /$out/mat` 
mq=`cat $out/mqt`
sa=`cat $out/sat` 
sq=`cat $out/sqt` 
ra=`cat $out/rat` 
rq=`cat $out/rqt`
s1=`expr $ma + $mq`
s2=`expr $s1 + $sa`
s3=`expr $s2 + $sq`
s4=`expr $s3 + $ra`
s5=`expr $s4 + $rq`

echo `date +"%Y-%m-%d:%H:%M:%S"`  $ma $mq $sa $sq $ra $rq X $ma $s1 $s2 $s3 $s4 $s5 >> $out/Q.allpools 2>/dev/null

#last1week=`date -d '1 week ago' +'%Y-%m-%d:00:01'`
#last6months=`date -d '6 months ago' +'%Y-%m-%d:00:01'`
#lastmonday=`date -d 'last monday' +'%Y-%m-%d:00:01'`

lastmonth=`date -d '1 month ago' +'%Y-%m-%d:00:01'`
tomorrow=`date -d '1 day' +'%Y-%m-%d:00:01'`

web=/fnal/ups/prd/www_pages/dcache/queue
if [ ! -r $web ]; then web=/tmp;fi

# adjust path for convert (either in /usr/bin or /usr/X11R6/bin
PATH=`$E_H/dropit /usr/X11R6/bin`; PATH=/usr/X11R6/bin:$PATH

for i in $out/Q.*; do
  f=`basename $i |sed -e 's/Q\.//'`
  echo $f ..............................................
  file=`mktemp /tmp/cronplot.XXXXXXXX`
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
plot '$i' using 1:14 t 'queued restore' with impulses, '$i' using 1:13 t 'active restore' with impulses, '$i' using 1:12 t 'queued store' with impulses, '$i' using 1:11 t 'active store' with impulses, '$i' using 1:10 t 'queued mover' with impulses, '$i' using 1:9 t 'active mover' with impulses
" >> $file
 gnuplot $file
 rm $file
 convert -rotate 90 $web/$f.ps $web/$f.jpg
 convert -rotate 90 -geometry 120x120  $web/$f.ps $web/${f}_stamp.jpg
done

id
node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`

if [ -r /usr/local/bin/ENSTORE_HOME ]; then
   . /usr/local/bin/ENSTORE_HOME
else
   echo `date` ERROR: Can NOT determine E_H.  Add /usr/local/bin/ENSTORE_HOME link
   exit 1
fi
PATH=`$E_H/dropit /usr/krb5/bin`; PATH=/usr/krb5/bin:$PATH
. ~enstore/gettkt
klist -fea

if [ $node = fcdfdcache6 ]; then slh=cdfdca; else slh=$serviceLocatorHost;fi

# move data to monitoring node if required
SH_IP=`host $slh|grep 'has address'|awk '{print $NF}'`
THIS_IP=`host $node|grep 'has address'|awk '{print $NF}'`
if [ "$SH_IP" != "$THIS_IP" ];then
   echo rcp $web   $slh:$web/
        rcp $web/* $slh:$web/
fi
