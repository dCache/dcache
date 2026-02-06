#!/bin/bash

# Define paths
PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
JACOCO_VERSION="${JACOCO_VERSION:-0.8.14}"  # Use env var or fallback
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
EXEC_FILES=$(find "$PROJECT_ROOT" -name "*.exec" -type f)

if [ -z "$EXEC_FILES" ]; then
    echo "Error: No .exec files found in $PROJECT_ROOT"
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

for module in $(find modules -maxdepth 1 -mindepth 1 -type d); do
    if [ -d "$module/target/classes" ]; then
        CLASSFILES_ARGS+=("--classfiles" "$module/target/classes")
        SOURCEFILES_ARGS+=("--sourcefiles" "$module/src/main/java")
    fi
done

# Generate the report with dynamic arguments
java -jar "$JACOCO_CLI_JAR" report "$MERGED_EXEC" \
    "${CLASSFILES_ARGS[@]}" \
    "${SOURCEFILES_ARGS[@]}" \
    --html "$REPORT_DIR" \
    --xml "$PROJECT_ROOT/target/coverage-reports/jacoco-total.xml" \
    --csv "$PROJECT_ROOT/target/coverage-reports/jacoco-total.csv"

if [ $? -ne 0 ]; then
    echo "Error: Failed to generate coverage report"
    exit 1
fi

echo "Coverage report generated successfully at ${REPORT_DIR}/index.html"