#!/bin/sh
#
. /fnal/ups/etc/setups.sh

#setup python #v2_1
. ~enstore/dcache-deploy/scripts/setup-enstore

HOST=$HOSTNAME
if [ $HOST = fcdfcaf301.fnal.gov -o $HOST = fcdfcaf055 -o $HOST = fcdfdcache4 ]; then
  COPY=1
  HOST=cdfdca.fnal.gov
else
  COPY=0
fi

case $HOST in
  stken*) diskname=~enstore/dcache-billing
          ;;
  cdfen*) diskname=~enstore/dcache-billing 
          ;;
   d0en*) diskname=~enstore/dcache-billing
          ;;
  gyoza*) diskname=~enstore/dcache-billing
          ;;
       *) diskname=UNKNOWN
          ;;
esac

cd ~enstore/dcache-deploy/scripts/plots

python completed_transfers.py ./myBilling.html "http://${HOST}/dcache" "http://${HOST}:443" ./pnfs_cache.txt ${diskname}/billing-2* 
