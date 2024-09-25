#!/bin/sh

. /init-el9-ui.sh

dnf -y -q install xrootd-client

xrdcp -f /etc/profile xroot://store-door-svc:1095//data/testfile

for i in `seq 1 1000`
do
  echo "File Nr. $i"
	xrdcp -f xroot://store-door-svc:1095//data/testfile /dev/null
done