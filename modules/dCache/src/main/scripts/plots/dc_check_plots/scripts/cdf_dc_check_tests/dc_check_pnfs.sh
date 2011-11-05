#!/bin/sh
#setup dcap
LOG=/tmp/dc_check.$$.log
TIMELOG=/tmp/dc_check.$$.timelog
SQLLOG=/tmp/dc_check.$$.sqllog
if [ "$1" = ""  -o "$2" = "" ]
then
  echo "usage: dc_check_url host port"
  exit 1
fi
host=$1
port=$2
#i=0
#while [ $i -lt 1 ] 
while /bin/true
do
#  let i++
    export TIMEFORMAT=%R
    cmd="dc_check /pnfs/fnal.gov/usr/cdfen/filesets/CA/CA30/CA3000/CA3000.1/ar017a14.04b5phys"
    date=`date`
    (time  $cmd >> $LOG  2>&1) >$TIMELOG 2>&1
    duration=`cat $TIMELOG`
    grep "passed" $LOG >/dev/null
    rc=$?
    if [ "$rc" = "0" ]
    then
      result=t
    else
      result=f
    fi
#    echo "$host        $port   $date   $duration       $result" >$SQLLOG
#    ./update.sh $SQLLOG
    echo ./update.sh "$host" "$port" "$date" "$duration" "$result"
    ./update.sh "$host" "$port" "$date" "$duration" "$result"

    #rm $SQLLOG
    rm $LOG
    rm $TIMELOG
    sleep 120
done

