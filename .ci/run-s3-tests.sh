#!/bin/bash
#
# Tests of the S3 doors: an AWS CLI round trip over HTTP and HTTPS, then
# the ceph/s3-tests listed in /s3-tests-pass-list.txt. The s3-tests
# results are written to /junit-s3tests.xml.
#
# The access keys are the fixed test keys of the dcache helm chart.
#
# smoke's exit code is checked with $? on purpose, see below.
# shellcheck disable=SC2181

set -u

S3_HOST=${S3_HOST:-store-door-svc}

# The ceph/s3-tests version that /s3-tests-pass-list.txt was recorded with;
# it is the last commit of that repository, the tests have moved into
# ceph/ceph. Pinned so that a failure means a change in dCache, not in the
# tests. Update it only together with the pass list.
S3_TESTS_COMMIT=5b3987116eca0b4b8fa57d6b10f3db57bab196b6

export AWS_ACCESS_KEY_ID=s3main
export AWS_SECRET_ACCESS_KEY=let-me-in-s3main
export AWS_DEFAULT_REGION=us-east-1

pip install -q awscli || exit 1
aws configure set default.s3.addressing_style path
aws configure set default.s3.multipart_threshold 8MB
aws configure set default.s3.multipart_chunksize 8MB

# smoke <endpoint> <bucket> <size> [aws options...]
smoke() (
    set -ex
    endpoint=$1
    bucket=$2
    size=$3
    shift 3
    opts=("$@")
    s3() { aws --endpoint-url "$endpoint" "${opts[@]}" s3 "$@"; }

    head -c "$size" /dev/urandom > "/tmp/$bucket.in"
    s3 mb "s3://$bucket"
    s3 cp --no-progress "/tmp/$bucket.in" "s3://$bucket/dir/object"
    s3 ls "s3://$bucket/dir/" | grep -q ' object$'
    s3 cp --no-progress "s3://$bucket/dir/object" "/tmp/$bucket.out"
    cmp "/tmp/$bucket.in" "/tmp/$bucket.out"
    s3 rm "s3://$bucket/dir/object"
    s3 rb "s3://$bucket"
)

# smoke must not be called in a || or && list or an if condition: bash
# ignores set -e there.

# 20 MB to have a multipart upload
smoke "http://$S3_HOST:9000" ci-smoke-http 20M
[ $? -eq 0 ] || exit 1
# the tester does not trust the chart's test CA
smoke "https://$S3_HOST:9001" ci-smoke-https 1M --no-verify-ssl
[ $? -eq 0 ] || exit 1

cd / || exit 1
git clone -q https://github.com/ceph/s3-tests || exit 1
cd s3-tests || exit 1
git checkout -q "$S3_TESTS_COMMIT" || exit 1
pip install -q -r requirements.txt || exit 1

# The bucket cleanup of the harness sends BypassGovernanceRetention, an
# object-lock header the door refuses as not implemented.
sed -i 's/^\(\s*\)BypassGovernanceRetention=True)/\1)/' s3tests/functional/__init__.py
if grep -q BypassGovernanceRetention s3tests/functional/__init__.py; then
    echo "Failed to remove BypassGovernanceRetention from the harness" >&2
    exit 1
fi

python3 - "$S3_HOST" <<'EOF' || exit 1
import configparser
import sys

c = configparser.RawConfigParser()
c.read('s3tests.conf.SAMPLE')
c['DEFAULT']['host'] = sys.argv[1]
c['DEFAULT']['port'] = '9000'
c['DEFAULT']['is_secure'] = 'False'
c['DEFAULT']['ssl_verify'] = 'False'
c['fixtures']['bucket prefix'] = 'ci-{random}-'
for section, key in (('s3 main', 's3main'), ('s3 alt', 's3alt'), ('s3 tenant', 's3tenant')):
    c[section]['access_key'] = key
    c[section]['secret_key'] = 'let-me-in-' + key
with open('/s3tests.conf', 'w') as f:
    c.write(f)
EOF

mapfile -t tests < <(grep -v '^#' /s3-tests-pass-list.txt)
S3TEST_CONF=/s3tests.conf pytest -q -rfE --junitxml=/junit-s3tests.xml "${tests[@]}"
