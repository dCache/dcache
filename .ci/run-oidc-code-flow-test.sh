#!/bin/sh

dnf -q install -y epel-release
dnf install -q -y oidc-agent-cli jq

#
# See: https://github.com/indigo-dc/oidc-agent/pull/601
#
mkdir -p ~/.config/oidc-agent
echo '{}' > ~/.config/oidc-agent/issuer.config

eval `oidc-agent --quiet`
oidc-agent  --status

#
# oidc-gen opens a local redirect listener and prints the URL to log in at.
# There is no real browser here, so curl plays that role: it loads the
# login page, submits the credentials, and follows the final redirect -
# which lands on oidc-gen's local listener and lets it finish the flow
# itself, same as it would for a real browser.
#
stdbuf -oL -eL oidc-gen --pub --scope-max \
    --iss http://keycloak:8080/realms/dcache-test \
    --flow=code \
    --client-id=dcache  \
    --redirect-uri=http://localhost:8080 \
    --no-save dcache-test > /tmp/oidc-gen.log 2>&1 &

TIMEOUT=30
ELAPSED=0
while ! grep -q 'https\?://[^ ]*' /tmp/oidc-gen.log 2>/dev/null; do
    sleep 1
    ELAPSED=$((ELAPSED + 1))
    if [ "$ELAPSED" -ge "$TIMEOUT" ]; then
        echo "Timed out waiting for oidc-gen to print an authorization URL:"
        cat /tmp/oidc-gen.log
        exit 1
    fi
done
AUTH_URL=$(grep -o 'https\?://[^ ]*' /tmp/oidc-gen.log | tail -1)

COOKIES=$(mktemp)
LOGIN_PAGE=$(curl -s -k -c "$COOKIES" "$AUTH_URL")
FORM_ACTION=$(echo "$LOGIN_PAGE" | grep -o 'action="[^"]*"' | head -1 | sed -e 's/action="//' -e 's/"$//' -e 's/&amp;/\&/g')

curl -s -k -b "$COOKIES" -c "$COOKIES" -L -o /dev/null \
    --data-urlencode "username=kermit" \
    --data-urlencode "password=let-me-in" \
    "$FORM_ACTION"

wait

TOKEN=$(oidc-token dcache-test)
echo $TOKEN | cut -d '.' -f 2 | base64 -d | jq

echo "hello from the OIDC code flow test" > /tmp/upload_content.txt

WEBDAV_URL="https://store-door-svc:8083/data/pool-b/oidc-code-flow-test-file.txt"

PUT_STATUS=$(curl -s -k -o /tmp/put_response.txt -w "%{http_code}" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/octet-stream" \
    -T /tmp/upload_content.txt \
    "$WEBDAV_URL")
echo "PUT status: $PUT_STATUS"
cat /tmp/put_response.txt
if [ "$PUT_STATUS" != "201" ]; then
    exit 1
fi

GET_STATUS=$(curl -s -k -L -o /tmp/hello.txt -w "%{http_code}" \
    -H "Authorization: Bearer ${TOKEN}" \
    "$WEBDAV_URL")
echo "GET status: $GET_STATUS"
cat /tmp/hello.txt
if [ "$GET_STATUS" != "200" ]; then
    exit 1
fi
