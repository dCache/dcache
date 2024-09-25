#!/bin/sh

. /init-el9-ui.sh

dnf install -y -q python3-urllib3


export DFTS_SUT=https://store-door-svc.$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace).svc.cluster.local:8083



cat > propfind.py <<EOF

import sys
requestURL = sys.argv[1]

import os
x509vo = os.environ['X509_USER_PROXY']

import ssl
import urllib
import urllib.request

PROPFIND_HDR = {'User-Agent': "org.cms.SE-WebDAV", 'Depth': "1",
                    'Content-Type': "text/xml; charset=UTF-8"}

PROPFIND_DATA = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><d:propfin" + \
                    "d xmlns:d=\"DAV:\"><d:allprop/></d:propfind>"

cntxt = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
cntxt.verify_mode = ssl.CERT_REQUIRED
cntxt.verify_flags = ssl.VERIFY_DEFAULT
cntxt.check_hostname = True

CSWD_VERSION = "v2.00.01"
CSWD_CAPATH = "/etc/grid-security/certificates"
CSWD_PROPOCOL_BAD = ["SSL", "SSLv2", "SSLv3", "TLS", "TLSv1", "TLSv1.1"]

cntxt.load_verify_locations(capath=CSWD_CAPATH)
cntxt.load_cert_chain(x509vo, x509vo)

requestObj = urllib.request.Request(requestURL, data=PROPFIND_DATA.encode("utf-8"), headers=PROPFIND_HDR, method="PROPFIND")
responseObj = urllib.request.urlopen(requestObj, timeout=90, context=cntxt)

print(responseObj.status)
print(responseObj.msg)
print(responseObj.reason)
print(responseObj.code)

urlCharset = responseObj.headers.get_content_charset()
xmlData = responseObj.read().decode( urlCharset )
print(xmlData)

EOF

python3 ./propfind.py $DFTS_SUT
