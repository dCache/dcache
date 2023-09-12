#!/bin/sh

. /init-grid-ui.sh

yum install -y -q git-core python-pip boost-python

# version that works with centos7
pip install robotframework==3.2.2

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

# robot returns the number of failed tests in its return code.
# So we add up the retvals using ERRORS as an accumulator
declare -i ERRORS=0

for name in $TESTS; do
  robot -o ${name}_output --variable SRM_VERSION:2 --name ${name} -x /xunit/xunit-${name}.xml ${name}.robot || ERRORS+=$?
done

exit $ERRORS