#!/bin/bash

# Define paths
PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
JACOCO_VERSION="${JACOCO_VERSION:-0.8.14}"  # Use env var or fallback
JACOCO_DIR="${PROJECT_ROOT}/jacoco-$JACOCO_VERSION"
JACOCO_AGENT_JAR="$JACOCO_DIR/lib/jacocoagent.jar"
JACOCO_CLI_JAR="${JACOCO_DIR}/lib/jacococli.jar"

echo "DEBUG: Checking for $JACOCO_CLI_JAR and $JACOCO_AGENT_JAR..."

# Check if JaCoCo CLI JAR exists in the cache directory
if [ ! -f "$JACOCO_CLI_JAR" ] || [ ! -f "$JACOCO_AGENT_JAR" ]; then
    echo "JaCoCo JARs not found (or incomplete) in cache directory. Downloading..."
    mkdir -p "$JACOCO_DIR"
    wget -q "https://github.com/jacoco/jacoco/releases/download/v$JACOCO_VERSION/jacoco-$JACOCO_VERSION.zip" -O "/tmp/jacoco-$JACOCO_VERSION.zip"
    unzip -o -q "/tmp/jacoco-$JACOCO_VERSION.zip" -d "$JACOCO_DIR"
    rm -f "/tmp/jacoco-$JACOCO_VERSION.zip"
    echo "DEBUG: Downloaded JaCoCo to $JACOCO_DIR"
    ls -la "$JACOCO_DIR/lib/"
else
    echo "DEBUG: JaCoCo JARs found: $JACOCO_CLI_JAR and $JACOCO_AGENT_JAR"
fi

# Check both JARs exist after download
if [ ! -f "$JACOCO_CLI_JAR" ]; then
    echo "Error: JaCoCo CLI JAR not found at $JACOCO_CLI_JAR"
    exit 1
fi
if [ ! -f "$JACOCO_AGENT_JAR" ]; then
    echo "Error: JaCoCo agent JAR not found at $JACOCO_AGENT_JAR"
    exit 1
fi

export JACOCO_CLI_JAR