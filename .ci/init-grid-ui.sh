#!/bin/sh

# fail on error
set -e

AUTOCA_URL=https://ci.dcache.org/ca

yum -q install -y openssl libtool-ltdl glibmm24 libxslt epel-release
yum -q install -y fetch-crl

rpm -i https://www.desy.de/~tigran/ca_dCacheORG-3.0-6.noarch.rpm
rpm -i https://linuxsoft.cern.ch/wlcg/centos7/x86_64/desy-voms-all-1.0.0-1.noarch.rpm

rpm -i https://repository.egi.eu/sw/production/cas/1/current/RPMS/ca_USERTrustRSACertificationAuthority-1.122-1.noarch.rpm
rpm -i https://repository.egi.eu/sw/production/cas/1/current/RPMS/ca_ResearchandEducationTrustRSARootCA-1.122-1.noarch.rpm
rpm -i https://repository.egi.eu/sw/production/cas/1/current/RPMS/ca_GEANTeScienceSSLCA4-1.122-1.noarch.rpm
rpm -i https://repository.egi.eu/sw/production/cas/1/current/RPMS/ca_USERTrustECCCertificationAuthority-1.122-1.noarch.rpm
rpm -i https://repository.egi.eu/sw/production/cas/1/current/RPMS/ca_GEANTeScienceSSLECCCA4-1.122-1.noarch.rpm
rpm -i https://repository.egi.eu/sw/production/cas/1/current/RPMS/ca_GEANTTCSAuthenticationRSACA4B-1.122-1.noarch.rpm


curl --silent https://raw.githubusercontent.com/kofemann/autoca/v1.0-py2/pyclient/autoca-client -o autoca-client && chmod a+x autoca-client
./autoca-client -n -k userkey.pem -c usercert.pem https://ci.dcache.org/ca "Kermit the frog"

. /cvmfs/grid.cern.ch/centos7-umd4-ui-4_200423/etc/profile.d/setup-c7-ui-example.sh
export LD_LIBRARY_PATH=/cvmfs/grid.cern.ch/centos7-umd4-ui-4_200423/usr/lib64/dcap:$LD_LIBRARY_PATH

# Since we rely on our dCache.org CA, which is not present
# in CVMFS, we need to manually alter X509_CERT_DIR to
# a local path that contains the CACerts from CVMFS and
# additionally the dCache.org CACerts
export X509_CERT_DIR=/etc/grid-security/certificates
export X509_VOMS_DIR=/etc/grid-security/vomsdir/

/usr/sbin/fetch-crl

arcproxy -C usercert.pem -K userkey.pem  -T ${X509_CERT_DIR} --vomses=${VOMS_USERCONF} --vomsdir=${X509_VOMS_DIR} --voms=desy
arcproxy -I
