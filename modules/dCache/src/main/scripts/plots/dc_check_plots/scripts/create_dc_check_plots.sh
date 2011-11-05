#!/bin/sh

node=`uname -n| sed -e 's/\([^\.]\)\..*/\1/'`

if [ -r /usr/local/bin/ENSTORE_HOME ]; then
   . /usr/local/bin/ENSTORE_HOME
else
   echo `date` ERROR: Can NOT determine E_H.  Add /usr/local/bin/ENSTORE_HOME link
   exit 1
fi

echo starrting on `date`
scriptdir=/var/enstore/tomcat/latest/webapps/dc_check_plots/scripts
extract_url=extract_last4_hours_url.sh
extract_pnfs=extract_last4_hours_pnfs.sh

date=`date |  tr ":" "." | tr " " "-"`
time=`date "+%T" | tr ":" "."`
date_dir=`date -I`
app_dir="/var/enstore/tomcat/latest/webapps/dc_check_plots/"
images_dir="/var/enstore/tomcat/latest/webapps/dc_check_plots/images"
if [ ! -d $images_dir ]
then
  mkdir $images_dir
fi
images_dir=$images_dir/$date_dir
if [ ! -d $images_dir ]
then
  mkdir $images_dir
fi
data=/tmp/dc_check_data.$$.dat
gnuplotfile=/tmp/gnuplot.$$.cmd
gnuimage=/tmp/dc_check_plot.$$.eps
echo "set timefmt '%Y-%m-%d %H:%M:%S'" >$gnuplotfile
echo "set xdata time" >>$gnuplotfile
echo "set log y" >>$gnuplotfile
echo "set size 2,2" >>$gnuplotfile
echo "set terminal postscript eps color solid 'Arial' 24" >>$gnuplotfile
echo "set grid" >>$gnuplotfile
echo "set output '$gnuimage'" >>$gnuplotfile
echo "set xlabel 'Time'" >>$gnuplotfile
echo "set ylabel 'Execution time (sec)'" >>$gnuplotfile
echo "set title \"execution time of dc_check using dcap url\"" >>$gnuplotfile
echo "plot '$data' using 1:6 t 'min' with line, '' using 1:8 t 'max' with line, '' using 1:10 t 'avg' with line " >>$gnuplotfile

$scriptdir/$extract_url $data
echo plotting $gnuplotfile
gnuplot $gnuplotfile
# adjust path for convert (either in /usr/bin or /usr/X11R6/bin
PATH=`$E_H/dropit /usr/X11R6/bin`; PATH=/usr/X11R6/bin:$PATH
echo converting from $gnuimage to $images_dir/$time.url.png
convert -geometry 720x720 -modulate 95,95 $gnuimage $images_dir/$time.url.png


echo "set timefmt '%Y-%m-%d %H:%M:%S'" >$gnuplotfile
echo "set xdata time" >>$gnuplotfile
echo "set log y" >>$gnuplotfile
echo "set size 2,2" >>$gnuplotfile
echo "set terminal postscript eps color solid 'Arial' 24" >>$gnuplotfile
echo "set grid" >>$gnuplotfile
echo "set output '$gnuimage'" >>$gnuplotfile
echo "set xlabel 'Time'" >>$gnuplotfile
echo "set ylabel 'Execution time (sec)'" >>$gnuplotfile
echo "set title \"execution time of dc_check using local pnfs\"" >>$gnuplotfile
echo "plot '$data' using 1:6 t 'min' with line, '' using 1:8 t 'max' with line, '' using 1:10 t 'avg' with line " >>$gnuplotfile

$scriptdir/$extract_pnfs $data
echo plotting $gnuplotfile
gnuplot $gnuplotfile
echo converting from $gnuimage to $images_dir/$time.pnfs.png
/usr/X11R6/bin/convert -geometry 720x720 -modulate 95,95 $gnuimage $images_dir/$time.pnfs.png

echo cleaning $data $gnuplotfile $gnuimage
rm $data $gnuplotfile $gnuimage

echo done on `date`
