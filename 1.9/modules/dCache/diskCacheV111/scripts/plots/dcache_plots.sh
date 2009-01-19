#! /usr/bin/env bash
#
. /fnal/ups/etc/setups.sh

#setup python #v2_1
. ~enstore/dcache-deploy/scripts/setup-enstore
 
if [[ "$HOSTNAME" = "gyoza7.fnal.gov" ]]; then
    setup gnuplot -t
fi

PATH=`~enstore/dropit /usr/X11R6/bin` ; PATH=$PATH:/usr/X11R6/bin
destdir=/local/ups/prd/www_pages/dcache

cd ~enstore/dcache-deploy/scripts/plots

python dcache_plots.py billing-2*.daily

cp billing-2*.daily*.{eps,jpg,pre,2jpg} ${destdir}

rm billing-2*.daily*.{eps,jpg,pre,2jpg}

