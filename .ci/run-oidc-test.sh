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

oidc-gen --pub --scope-max \
    --iss http://keycloak:8080/realms/dcache-test \
    --flow=password \
    --op-username=kermit \
    --op-password=let-me-in \
    --client-id=dcache  \
    --redirect-uri="" \
    --no-save dcache-test

TOKEN=$(oidc-token dcache-test)
echo $TOKEN | cut -d '.' -f 2 | base64 -d | jq

curl --fail -s -k  -H "Authorization: Bearer ${TOKEN}" https://store-door-svc:3881/api/v1/user



