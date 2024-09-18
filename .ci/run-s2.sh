#!/bin/sh

. /init-el9-ui.sh

# SLEEP_SOR is the sleep-time when polling an SRM GetStatusOf...Request
export SLEEP_SOR=2
export S2_SUPRESS_PROGRESS=1

export SRM_HOST=store-door-svc.$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace).svc.cluster.local
export SRM_PORT=8443
export SRM_DATAPATH=/data/s2
export SRM_ENDPOINT="srm://${SRM_HOST}:${SRM_PORT}/srm/managerv2?SFN=${SRM_DATAPATH}"

# s3 test depends on needs USER env to be set
export USER=dcache-ci

RC=0
for i in /usr/share/s2/testing/scripts/protos/srm/2.2/{avail,basic,usecase}
do
  S2_LOGS_DIR=/ /usr/bin/xrunner.py -d $i
  if [ $? -ne 0 ]; then
    RC=1
  fi
done
exit $RC
