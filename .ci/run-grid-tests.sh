#!/bin/sh

. /init-el9-ui.sh

dnf install -y -q git-core python3-pip
dnf install -y -q https://www.dcache.org/old/downloads/1.9/repo/9.2/dcache-srmclient-9.2.0-1.noarch.rpm
dnf install -y -q dcap gfal2-all python3-gfal2-util

# version that works with centos7
pip3 install robotframework==3.2.2

git clone --depth 1 https://github.com/dCache/Grid-tools-functional-test-suite.git
cd Grid-tools-functional-test-suite

export DFTS_SUT=store-door-svc.$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace).svc.cluster.local
export HTTP_PORT=8080
export SRM_PORT=8443
export GSIDCAP_PORT=22128
export GSIFTP_PORT=2811
export DCAP_PORT=22125
export REMOTE_DIR=/data/g2/
export WORKSPACE=`pwd`


# test groups to run
TESTS="DccpTests GlobusurlcpTests SrmlsTests SrmcpTests"

#FIXME: remove this when the tests are fixed
TESTS="DccpTests GlobusurlcpTests SrmlsTests"

# robot returns the number of failed tests in its return code.
# So we add up the retvals using ERRORS as an accumulator
declare -i ERRORS=0

for name in $TESTS; do
  robot -o ${name}_output --name ${name} -x /xunit/xunit-${name}.xml ${name}.robot || ERRORS+=$?
done

exit $ERRORS