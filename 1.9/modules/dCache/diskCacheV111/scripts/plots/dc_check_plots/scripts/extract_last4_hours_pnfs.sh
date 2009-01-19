#!/bin/sh
if [ "$1" = "" ]
then
   echo " no file specified, ouput to stdout"
   psql -h cdfdcam -c "select * from dc_check_pnfs where date_trunc > (current_timestamp-interval '4 hours')" dcache
   $cmd
else
   echo "output to $1"
   psql -h cdfdcam -c "select * from dc_check_pnfs where date_trunc > (current_timestamp-interval '4 hours')" dcache | egrep -v "(---|rows\)|date_trunc)" >$1 2>&1
fi

