#!/bin/bash

apt-get update && apt-get install -y wget unzip
# Define paths
PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
JACOCO_VERSION="${JACOCO_VERSION:-0.8.14}"  # Use env var or fallback
JACOCO_DIR="${PROJECT_ROOT}/jacoco-$JACOCO_VERSION"
JACOCO_CLI_JAR="$JACOCO_DIR/lib/jacococli.jar"

echo "DEBUG: Checking if $JACOCO_CLI_JAR exists..."
###
#
# Check if JaCoCo CLI JAR exists in the cache directory
if [ ! -f "$JACOCO_CLI_JAR" ]; then
    echo "JaCoCo CLI JAR not found in cache directory. Downloading..."
    mkdir -p "$JACOCO_DIR"
    wget -q "https://github.com/jacoco/jacoco/releases/download/v$JACOCO_VERSION/jacoco-$JACOCO_VERSION.zip" -O "/tmp/jacoco-$JACOCO_VERSION.zip"
    unzip -q "/tmp/jacoco-$JACOCO_VERSION.zip" -d "$JACOCO_DIR"
    rm -f "/tmp/jacoco-$JACOCO_VERSION.zip"
    echo "DEBUG: Downloaded JaCoCo CLI to $JACOCO_DIR"
    ls -la "$JACOCO_DIR"
else
    echo "DEBUG: JaCoCo CLI JAR found at $JACOCO_CLI_JAR"
fi

# Check if JaCoCo CLI JAR exists after download
if [ ! -f "$JACOCO_CLI_JAR" ]; then
    echo "Error: JaCoCo CLI JAR not found at $JACOCO_CLI_JAR"
    exit 1
fi

export JACOCO_CLI_JAR