#!/bin/bash

# Define paths
PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
JACOCO_VERSION=0.8.14
JACOCO_DIR="${PROJECT_ROOT}/jacoco-$JACOCO_VERSION"
JACOCO_CLI_JAR="$JACOCO_DIR/lib/jacococli.jar"
MERGED_EXEC="$PROJECT_ROOT/target/coverage-reports/merged.exec"
REPORT_DIR="$PROJECT_ROOT/target/coverage-reports/site"

# Check if JaCoCo CLI JAR exists get-jacoco.sh
if [ ! -f "$JACOCO_CLI_JAR" ]; then
    echo "Error: JaCoCo CLI JAR not found at $JACOCO_CLI_JAR"
    exit 1
fi

# Ensure the report directory exists
mkdir -p "$REPORT_DIR"

# Issue 1: Simplified find command (avoiding array initialization)
EXEC_FILES=$(find "$PROJECT_ROOT" -name "jacoco-ut.exec" -type f)

# Issue 2: Simplified check for empty results using -z
if [ -z "$EXEC_FILES" ]; then
    echo "Error: No jacoco-ut.exec files found in $PROJECT_ROOT"
    exit 1
fi

# Merge execution data files
echo "Merging execution data files..."
# Issue 4: Passing the string directly
java -jar "$JACOCO_CLI_JAR" merge ${EXEC_FILES} --destfile "$MERGED_EXEC"

if [ $? -ne 0 ]; then
    echo "Error: Failed to merge execution data files"
    exit 1
fi

# Generate the coverage report
echo "Generating coverage report..."

# Build classfiles and sourcefiles arguments dynamically
CLASSFILES_ARGS=()
SOURCEFILES_ARGS=()

# Issue 5: Iterating over the string list instead of an array
for exec_file in ${EXEC_FILES}; do
    # Extract module path (e.g., core, dlm, rquota) from the exec file path
    module_path=$(dirname "$(dirname "$(dirname "$exec_file")")")

    # Add classfiles and sourcefiles arguments
    CLASSFILES_ARGS+=("--classfiles" "$module_path/target/classes")
    SOURCEFILES_ARGS+=("--sourcefiles" "$module_path/src/main/java")
done

# Generate the report with dynamic arguments
java -jar "$JACOCO_CLI_JAR" report "$MERGED_EXEC" \
    "${CLASSFILES_ARGS[@]}" \
    "${SOURCEFILES_ARGS[@]}" \
    --html "$REPORT_DIR"

if [ $? -ne 0 ]; then
    echo "Error: Failed to generate coverage report"
    exit 1
fi

echo "Coverage report generated successfully at ${REPORT_DIR}/index.html"