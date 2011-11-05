#!/bin/sh
. /usr/local/etc/setups.sh
setup dcap
setup postgres
#while /bin/true
#do
  echo running dc_check timing tests  on `date` >>log/runtests.log
  for str in `cat cdf_host_port.txt`
  do
   str=`echo $str | tr ":" " "`
   host=`echo $str | ./host.sh`
   port=`echo $str | ./port.sh`
   setsid ./dc_check_url.sh $host $port &
  done
  host="pnfs"
  port="0"
  setsid ./dc_check_pnfs.sh $host $port &
  setsid ./dc_check_pnfs.sh $host $port &
  setsid ./dc_check_pnfs.sh $host $port &
#sleep 60
#done
