#!/bin/sh
LOG=log/update.log
if [ "$1" = "" ]
then
  echo "data file is not specified"
  exit 1
fi

command="\\copy dc_check_log from '$1'"
. /usr/local/etc/setups.sh
setup fipc
#setup postgres
fipc create queue /cdf/update/queue
fipc append /cdf/update/queue
fipc qwait -t 100 /cdf/update/queue 9
rc=$?
while [ "$rc" != "0" ]
do
  echo "  $$ fipc qwait failed, trying again" >> $LOG
  fipc clean queue /cdf/update/queue
  fipc qwait -t 100 /cdf/update/queue 9
  rc=$?
done
echo ">>>>> $$ updating db with `cat $1`" >> $LOG
psql -c "$command" dcache -h  fcdfcaf055 
echo "<<<< $$ done updating db " >> $LOG
fipc remove /cdf/update/queue
