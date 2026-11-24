#!/bin/bash

# Define paths
PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
JACOCO_VERSION="${JACOCO_VERSION:-0.8.14}"  # Use env var or fallback
JACOCO_DIR="${PROJECT_ROOT}/jacoco-$JACOCO_VERSION"
JACOCO_CLI_JAR="$JACOCO_DIR/lib/jacococli.jar"
MERGED_EXEC="$PROJECT_ROOT/target/coverage-reports/merged.exec"
REPORT_DIR="$PROJECT_ROOT/target/coverage-reports/site"
FINAL_CLASSES_DIR="${PROJECT_ROOT}/target/coverage-reports/classes"

if [ ! -f "$JACOCO_CLI_JAR" ]; then
    echo "Error: JaCoCo CLI JAR not found at $JACOCO_CLI_JAR"
    exit 1
fi

#Ensure the report directory exists
rm -rf "$FINAL_CLASSES_DIR"
mkdir -p "$REPORT_DIR" "$FINAL_CLASSES_DIR"

#Collect all .exec files and merge
mapfile -t EXEC_FILES < <(find "$PROJECT_ROOT" -name "*.exec" -type f)

if [ ${#EXEC_FILES[@]} -eq 0 ]; then
    echo "Error: No .exec files found"
    exit 1
fi

echo "Merging ${#EXEC_FILES[@]} execution data files..."
java -jar "$JACOCO_CLI_JAR" merge "${EXEC_FILES[@]}" --destfile "$MERGED_EXEC"

#Collect compiled classes
#Unit and integration test data both match these classes, as the tar build that
#runs in the pods compiles the same bytes.

echo "Collecting compiled classes from modules..."
SOURCEFILES_ARGS=()

for module in $(find modules -maxdepth 1 -mindepth 1 -type d | sort); do
    [ -d "$module/target/classes" ] && cp -R "$module/target/classes/." "$FINAL_CLASSES_DIR/"
    [ -d "$module/src/main/java" ]  && SOURCEFILES_ARGS+=("--sourcefiles" "$module/src/main/java")
done

#Remove known generated/excluded packages
for pkg in \
    "org/dcache/services/billing/db/data" \
    "diskCacheV111/poolManager/jmh_generated" \
    "org/dcache/util/jmh_generated" \
    "org/dcache/chimera/jmh_generated"
do
    target="$FINAL_CLASSES_DIR/$pkg"
    if [ -d "$target" ]; then
        echo "  - Excluding generated: $pkg"
        rm -rf "$target"
    fi
done

#Generate report

echo "Generating coverage report..."
java -jar "$JACOCO_CLI_JAR" report "$MERGED_EXEC" \
    --classfiles "$FINAL_CLASSES_DIR" \
    "${SOURCEFILES_ARGS[@]}" \
    --html "$REPORT_DIR" \
    --xml "${PROJECT_ROOT}/target/coverage-reports/jacoco.xml"

echo "Coverage report generated successfully at ${REPORT_DIR}/index.html"