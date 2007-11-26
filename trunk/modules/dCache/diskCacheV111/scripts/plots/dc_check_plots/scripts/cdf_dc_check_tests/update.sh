#!/bin/sh
LOG=log/update.log
if [ "$5" = "" ]
then
  echo "not all arguments are  specified"
  exit 1
fi
host=$1
port=$2
date=$3
duration=$4
result=$5
command="insert into dc_check_log values('$host','$port','$date','$duration','$result')"
. /usr/local/etc/setups.sh
setup fipc
#setup postgres
#fipc create queue /cdf/update/queue
#fipc append /cdf/update/queue
#fipc qwait -t 100 /cdf/update/queue 9
#rc=$?
#while [ "$rc" != "0" ]
#do
#  echo "  $$ fipc qwait failed, trying again" >> $LOG
#  fipc clean queue /cdf/update/queue
#  fipc qwait -t 100 /cdf/update/queue 9
#  rc=$?
#done
echo ">>>>> $$ updating db with $command" >> $LOG
psql -c "$command" dcache -h  cdfdcam >>$LOG 2>&1 
echo "<<<< $$ done updating db " >> $LOG
#fipc remove /cdf/update/queue
