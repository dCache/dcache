#!/bin/sh
#
batchFile=/usr/people/patrick/cvs-cells/cells/dmg/apps/osm/osm.oho.batch
#
while : ; do
   echo "Restarted `date`" >>/tmp/osmAgent.restarted
   java dmg.cells.services.Domain osmAgent -spy 22222 -telnet 22123 -batch $batchFile >/dev/null 2>/dev/null
   if [ $? -eq 0 ] ;then break  ; fi
done
exit 0
