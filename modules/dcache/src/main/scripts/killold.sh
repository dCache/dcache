#!/bin/sh

. ~enstore/dcache-deploy/config/dCacheSetup

old_junk_hours=72
old_stage_hours=72
old_work_hours=72

old_stage_date=`date -d "$old_stage_hours hours ago" +"%s"`
old_work_date=`date -d "$old_work_hours hours ago" +"%s"`
old_junk_date=`date -d "$old_junk_hours hours ago" +"%s"`

kill_file=/tmp/K
rm -f $kill_file 2>/dev/null

cat /tmp/KILLDOORS | while read door mon day ttime kill cell active type file; do
   sec=`date -d "$mon $day $ttime" +"%s" 2>/dev/null`;    rc=$?
   if [ $rc -ne 0 ]; then continue; fi
   if [ $sec -le $old_stage_date -a "$type" = "stage" ]; then
       echo "OLD STAGE" $door $month $day $ttime $cell $active $type $file
       echo "echo -e \"exit\nset dest System@door${door}Domain\nkill $cell\nexit\nexit\n\"" \|ssh -c blowfish -p  $sshPort $serviceLocatorHost >> $kill_file
   elif [ $sec -le $old_work_date -a "$type" = "open" ]; then
       echo "OLD WORK " $door $month $day $ttime $cell $active $type $file
       echo "echo -e \"exit\nset dest System@door${door}Domain\nkill $cell\nexit\nexit\n\"" \|ssh -c blowfish -p  $sshPort $serviceLocatorHost >> $kill_file
   elif [ $sec -le $old_junk_date -a "$type" != "stage" -a "$type" != "open" ]; then
       echo "OLD UNKNOWN JUNK " $door $month $day $ttime $cell $active $type $file
       echo "echo -e \"exit\nset dest System@door${door}Domain\nkill $cell\nexit\nexit\n\"" \|ssh -c blowfish -p  $sshPort $serviceLocatorHost >> $kill_file
   fi
done 

if [ -r $kill_file ]; then 
   if [ ${1:-ignore} = "kill" ]; then 
       . $kill_file
   fi
   rm -f $kill_file 2>/dev/null
fi
