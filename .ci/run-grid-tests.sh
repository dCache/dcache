#!/bin/sh

AUTOCA_URL=https://ci.dcache.org/ca

yum -q install -y openssl libtool-ltdl glibmm24
rpm -i https://www.desy.de/~tigran/ca_dCacheORG-3.0-6.noarch.rpm
rpm -i https://linuxsoft.cern.ch/wlcg/centos7/x86_64/desy-voms-all-1.0.0-1.noarch.rpm

curl --silent https://raw.githubusercontent.com/kofemann/autoca/v1.0-py2/pyclient/autoca-client -o autoca-client && chmod a+x autoca-client
./autoca-client -n -k userkey.pem -c usercert.pem https://ci.dcache.org/ca "Kermit the frog"

. /cvmfs/grid.cern.ch/umd-c7wn-latest/etc/profile.d/setup-c7-wn-example.sh 

arcproxy -C usercert.pem -K userkey.pem  -T ${X509_CERT_DIR} --vomses=${VOMS_USERCONF} --vomsdir=/etc/grid-security/vomsdir/ --voms=desy
arcproxy -I

