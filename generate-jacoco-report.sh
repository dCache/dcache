#!/bin/bash

# Define paths
PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
JACOCO_VERSION="${JACOCO_VERSION:-0.8.14}"  # Use env var or fallback
JACOCO_DIR="${PROJECT_ROOT}/jacoco-$JACOCO_VERSION"
JACOCO_CLI_JAR="$JACOCO_DIR/lib/jacococli.jar"
MERGED_EXEC="$PROJECT_ROOT/target/coverage-reports/merged.exec"
REPORT_DIR="$PROJECT_ROOT/target/coverage-reports/site"
DUMPED_CLASSES_DIR="$PROJECT_ROOT/integration-results/classes-dump"
FILTERED_CLASSES_DIR="$PROJECT_ROOT/target/filtered-classes"
rm -rf "$FILTERED_CLASSES_DIR"
mkdir -p "$FILTERED_CLASSES_DIR"

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
java -jar "$JACOCO_CLI_JAR" merge ${EXEC_FILES} --destfile "$MERGED_EXEC"

if [ $? -ne 0 ]; then
    echo "Error: Failed to merge execution data files"
    exit 1
fi

#Prepare a temporary directory for filtered classes
FILTERED_CLASSES_DIR="$PROJECT_ROOT/target/filtered-classes"
rm -rf "$FILTERED_CLASSES_DIR"
mkdir -p "$FILTERED_CLASSES_DIR"

# Build classfiles and sourcefiles arguments dynamically
CLASSFILES_ARGS=()
SOURCEFILES_ARGS=()

echo "Collecting and filtering class files..."

for module in $(find modules -maxdepth 1 -mindepth 1 -type d); do
    if [ -d "$module/target/classes" ]; then
        cp -R "$module/target/classes/." "$FILTERED_CLASSES_DIR/"
    fi
    if [ -d "$module/src/main/java" ]; then
        SOURCEFILES_ARGS+=("--sourcefiles" "$module/src/main/java")
    fi
done

EXCLUSIONS=(
    "org/dcache/srm/v2_2"
    "gov/fnal/srm/util"
    "org/dcache/services/billing/db/data"
    "diskCacheV111/poolManager/jmh_generated"
    "org/dcache/util/jmh_generated"
    "org/dcache/chimera/jmh_generated"
)

for pkg in "${EXCLUSIONS[@]}"; do
    if [ -d "$FILTERED_CLASSES_DIR/$pkg" ]; then
        echo "  - Excluding: $pkg"
        rm -rf "$FILTERED_CLASSES_DIR/$pkg"
    fi
done

# Add the cleaned directory to JaCoCo arguments
CLASSFILES_ARGS+=("--classfiles" "$FILTERED_CLASSES_DIR")

# Preserve main functionality: Add the Woven Classes (AspectJ)
if [ -d "$DUMPED_CLASSES_DIR" ]; then
    echo "Adding woven classes from dump directory..."
    CLASSFILES_ARGS+=("--classfiles" "$DUMPED_CLASSES_DIR")
fi

# Generate the report with dynamic arguments
java -jar "$JACOCO_CLI_JAR" report "$MERGED_EXEC" \
    "${CLASSFILES_ARGS[@]}" \
    "${SOURCEFILES_ARGS[@]}" \
    --html "$REPORT_DIR" \
    --xml "$PROJECT_ROOT/target/coverage-reports/jacoco.xml"

if [ $? -ne 0 ]; then
    echo "Error: Failed to generate coverage report"
    exit 1
fi

echo "Coverage report generated successfully at ${REPORT_DIR}/index.html"