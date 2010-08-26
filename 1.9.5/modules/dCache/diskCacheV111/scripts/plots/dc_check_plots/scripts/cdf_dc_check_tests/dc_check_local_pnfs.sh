#!/bin/sh
#setup dcap
LOG=/tmp/dc_check.$$.log
i=0
while [ $i -lt 10 ] 
do
  let i++
    echo ----------------run $i at `date`------------------------------------------ 
    export TIMEFORMAT=%R
    cmd="dc_check /pnfs/fnal.gov/usr/cdfen/filesets/CA/CA30/CA3000/CA3000.1/ar017a14.04b5phys"

    result=`time $cmd >$LOG 2>&1`
    echo result = $result 
    echo LOGFILE:
    #cat $LOG
    rm $LOG
done

