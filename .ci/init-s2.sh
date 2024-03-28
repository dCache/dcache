#!/bin/sh

AUTOCA_URL=https://ci.dcache.org/ca

dnf -q install -y epel-release which
dnf -q install -y https://gitlab.desy.de/dcache/s2/-/jobs/386149/artifacts/raw/RPM-BUILD/RPMS/x86_64/s2-20240312-1.x86_64.rpm
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

# globus libraries depend on the SHA1
# https://twiki.cern.ch/twiki/bin/view/LCG/EL9vsSHA1CAs
update-crypto-policies --set DEFAULT:SHA1

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
FAILED_TESTS=""
PASSED_TESTS=""

for i in /usr/share/s2/testing/scripts/protos/srm/2.2/basic/*.s2;
do
  t=`basename $i`
  echo -n "$t : "
  cmd=`(echo $i | sed -e 's/\.s2$/\.sh/')`
  $cmd > /dev/null 2>&1
  rc=$?
  if [ $rc -ne 0 ]; then
    RC=1
    echo "FAILED"
    FAILED_TESTS="$FAILED_TESTS $t"
    ofile=`(echo $i | sed -e 's/\.s2$/\.out/')`
    cat `basename $ofile`

    efile=`(echo $i | sed -e 's/\.s2$/\.e1/')`
    cat `basename $ofile`
  else
    echo "PASSED"
    PASSED_TESTS="$PASSED_TESTS $t"
  fi
done

echo
echo
echo "PASSED_TESTS: $PASSED_TESTS"
echo
echo
echo "FAILED_TESTS: $FAILED_TESTS"
exit $RC
