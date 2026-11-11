#!/bin/sh

#
# Required to support grid tools that sill use SHA1
#
update-crypto-policies --set DEFAULT:SHA1

AUTOCA_URL=https://ci.dcache.org/ca

dnf -q install -y epel-release which dnf-plugins-core
dnf -q install -y https://download.dcache.org/nexus/repository/s2-testsuite/el9/x86_64/s2-20240423-1.x86_64.rpm
dnf -q install -y voms-clients-cpp fetch-crl openssl globus-gass-copy-progs

rpm -i https://www.desy.de/~tigran/ca_dCacheORG-3.0-6.noarch.rpm

curl --silent https://raw.githubusercontent.com/kofemann/autoca/v1.0-py3/pyclient/autoca-client -o autoca-client && chmod a+x autoca-client
python3 ./autoca-client -n -k userkey.pem -c usercert.pem ${AUTOCA_URL} "Kermit the frog"


/usr/sbin/fetch-crl

voms-proxy-init -cert=usercert.pem -key=userkey.pem
voms-proxy-info -all

# standard location for the proxy
export X509_USER_PROXY=/tmp/x509up_u$(id -u)


