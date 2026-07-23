#!/bin/bash

# Dummy HSM script for use-case-to-tape and use-case-diskandtape
# Arguments passed by dCache: <action> <pnfsid> <filepath> [options]

ACTION="$1"
PNFSID="$2"
FILEPATH="$3"

case "$ACTION" in
    put)
        # Simulate successful write to tape
        exit 0
        ;;
    get)
        # Simulate successful restore/stage from tape
        exit 0
        ;;
    remove)
        # Simulate removal from tape
        exit 0
        ;;
    *)
        exit 0
        ;;
esac