#!/bin/sh
#setup dcap
LOG=/tmp/dc_check.$$.log
LOG1=/tmp/dc_check.$$.log1
if [ "$1" = ""  -o "$2" = "" ]
then
  echo "usage: dc_check_url host port"
  exit 1
fi

i=0
while [ $i -lt 2 ] 
do
  let i++
    echo ----------------run $i at `date`------------------------------------------
    export TIMEFORMAT=%R
    cmd="sleep 10" 
#    cmd="dc_check dcap://$1:$2/pnfs/fnal.gov/usr/cdfen/filesets/CA/CA30/CA3000/CA3000.1/ar017a14.04b5phys"
    (time  $cmd >> $LOG  2>&1) >$LOG1 2>&1
    result=`cat $LOG1`
    echo result=$result
    rm $LOG
    rm $LOG1
done

