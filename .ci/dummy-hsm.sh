#!/bin/bash
# Dummy HSM script for use-case-to-tape and use-case-diskandtape
ACTION="$1"
PNFSID="$2"
FILEPATH="$3"

echo "$(date -Iseconds) CALLED: $0 $*" >> /tmp/dummy-hsm-calls.log

case "$ACTION" in
    put)
        echo "dummy://dummy/?store=default&group=default&bfid=${PNFSID:-000000000000}"
        exit 0
        ;;
    get)
        # write dummy content back so dCache has an actual disk copy
        echo "dummy restored content for ${PNFSID}" > "$FILEPATH"
        exit 0
        ;;
    remove)
        exit 0
        ;;
    *)
        exit 1
        ;;
esac