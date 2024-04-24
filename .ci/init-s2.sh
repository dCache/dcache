#!/bin/sh

AUTOCA_URL=https://ci.dcache.org/ca

dnf -q install -y epel-release which
dnf -q install -y https://download.dcache.org/nexus/repository/s2-testsuite/el9/x86_64/s2-20240423-1.x86_64.rpm
dnf -q install -y voms-clients-cpp fetch-crl openssl globus-gass-copy-progs

rpm -i https://www.desy.de/~tigran/ca_dCacheORG-3.0-6.noarch.rpm
rpm -i https://linuxsoft.cern.ch/wlcg/centos7/x86_64/desy-voms-all-1.0.0-1.noarch.rpm

curl https://repository.egi.eu/sw/production/cas/1/current/repo-files/egi-trustanchors.repo -o /etc/yum.repos.d/egi-trustanchors.repo

dnf -y install ca_USERTrustRSACertificationAuthority \
  ca_ResearchandEducationTrustRSARootCA \
  ca_GEANTeScienceSSLCA4 \
  ca_USERTrustECCCertificationAuthority \
  ca_GEANTeScienceSSLECCCA4 \
  ca_GEANTTCSAuthenticationRSACA4B

curl --silent https://raw.githubusercontent.com/kofemann/autoca/v1.0-py3/pyclient/autoca-client -o autoca-client && chmod a+x autoca-client
python3 ./autoca-client -n -k userkey.pem -c usercert.pem ${AUTOCA_URL} "Kermit the frog"


/usr/sbin/fetch-crl

voms-proxy-init -cert=usercert.pem -key=userkey.pem -voms=desy
voms-proxy-info -all

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
