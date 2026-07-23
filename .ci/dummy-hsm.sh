#!/bin/bash

# Dummy HSM script for use-case-to-tape and use-case-diskandtape
# Arguments passed by dCache: <action> <pnfsid> <filepath> [options]

ACTION="$1"
PNFSID="$2"

case "$ACTION" in
    put)
        # Print dummy storage URI to stdout for dCache registration
        echo "dummy://dummy/?store=default&group=default&bfid=${PNFSID:-000000000000}"
        exit 0
        ;;
    get|remove|*)
        exit 0
        ;;
esac