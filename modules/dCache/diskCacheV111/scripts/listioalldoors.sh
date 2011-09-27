#/bin/sh
set +u; . ~enstore/dcache-deploy/config/dCacheSetup ; set -u
TEMP_HTML=$1.tmp
echo "<html>" >$TEMP_HTML
echo "<head>" >>$TEMP_HTML 
echo "<meta HTTP-EQUIV=\"Refresh\" CONTENT=\"300\">" >>$TEMP_HTML 
echo "<title>Recent dCache Transfers</title>"  >>$TEMP_HTML 
echo "</head>" >>$TEMP_HTML 
echo "<body background=\"bg.svg\" link=red vlink=red alink=red>" >>$TEMP_HTML 
echo "<table border=0 cellpadding=10 cellspacing=0 width=\"90%\">" >>$TEMP_HTML 
echo "<tr><td align=center valign=center width=\"1%\">" >>$TEMP_HTML 
echo "<a href=\"http://$serviceLocatorHost:443\"><img border=0 src=\"eagleredtrans.gif\"></a>" >>$TEMP_HTML 
echo "<br><font color=red>dCache Home</font></td>" >>$TEMP_HTML
echo "<td align=center><h1>Active Transfers</h1></td></tr></table>" >>$TEMP_HTML
echo "<table border=0  cellpadding=0 cellspacing=0 width=\"100%\">" >>$TEMP_HTML 
echo "<tr><td> <h3>`date`</h3> </td></tr>" >>$TEMP_HTML
echo "<tr><td>" >>$TEMP_HTML
echo "<table align=left valign=top border=0 cellpadding=5 cellspacing=0 width=\"90%\">" >>$TEMP_HTML  
echo "<tr><td><b>Uid</b></td><td><b>Pid</b></td><td><b>Door</b></td><td><b>Client Host</b></td><td><b>Pool</b></td><td><b>PNFS ID</b></td><td><b>Transfer #</b></td><td><b>State</b></td><td><b>Seconds in this state</b></td></tr>" >>$TEMP_HTML

SCRIPT=~enstore/dcache-deploy/scripts/listiodoors.sh

for doors in `ls ~/dcache-deploy/config/*door*Setup`; do
  door=`echo $doors | sed -e 's#.*/config/##' -e 's/Setup//'`
  echo `date` $SCRIPT ${door}Domain
  $SCRIPT ${door}Domain | tee /tmp/listiodoor.output
  echo `date` finished
  cat /tmp/listiodoor.output >> $TEMP_HTML 
done

echo `date` Finished. Now making html active login page
echo "</table> </table>" >>$TEMP_HTML
echo "<h3>Finished at `date`</h3>"  >>$TEMP_HTML
echo "</table>"  >>$TEMP_HTML
echo "</body></html>" >>$TEMP_HTML

mv $TEMP_HTML $1

echo `date` listioalldoors.sh finished 
