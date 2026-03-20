#!/bin/bash

# Define paths
PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
JACOCO_VERSION="${JACOCO_VERSION:-0.8.14}"  # Use env var or fallback
JACOCO_DIR="${PROJECT_ROOT}/jacoco-$JACOCO_VERSION"
JACOCO_CLI_JAR="$JACOCO_DIR/lib/jacococli.jar"
MERGED_EXEC="$PROJECT_ROOT/target/coverage-reports/merged.exec"
REPORT_DIR="$PROJECT_ROOT/target/coverage-reports/site"
DUMP_DIR="${PROJECT_ROOT}/integration-results/classes-dump"
FINAL_CLASSES_DIR="${PROJECT_ROOT}/target/coverage-reports/classes"

if [ ! -f "$JACOCO_CLI_JAR" ]; then
    echo "Error: JaCoCo CLI JAR not found at $JACOCO_CLI_JAR"
    exit 1
fi

#Ensure the report directory exists
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

echo "Collecting compiled classes from modules..."
SOURCEFILES_ARGS=()

for module in $(find modules -maxdepth 1 -mindepth 1 -type d | sort); do
    [ -d "$module/target/classes" ] && cp -R "$module/target/classes/." "$FINAL_CLASSES_DIR/"
    [ -d "$module/src/main/java" ]  && SOURCEFILES_ARGS+=("--sourcefiles" "$module/src/main/java")
done

#Remove known generated/excluded packages
for pkg in \
    "org/dcache/srm/v2_2" \
    "gov/fnal/srm/util" \
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

#Overlay woven classes from pods

if [ -d "$DUMP_DIR" ]; then
    echo "Overlaying woven classes from pod dumps..."

    declare -A WOVEN_LOOKUP

    while IFS= read -r -d '' woven_file; do
        filename=$(basename "$woven_file")

        #Skip AspectJ synthetic classes
        if echo "$filename" | grep -qE '^\.[0-9a-f]{8,}\.class$|^AjcClosure[0-9].*\.class$'; then
            continue
        fi
        #Strip hash suffix if present
        clean_name=$(echo "$filename" | sed -E 's/\.[0-9a-f]{8,}\.class$/.class/')

        #Relative path within the pods dump subdir
        rel_dir=$(dirname "$woven_file" | sed -E "s|.*classes-dump/[^/]+/||")
        key="${rel_dir}/${clean_name}"

        #First pod wins, should be identical for the same class version
        [ -z "${WOVEN_LOOKUP[$key]+x}" ] && WOVEN_LOOKUP[$key]="$woven_file"

    done < <(find "$DUMP_DIR" -name "*.class" -print0)

    REPLACED=0
    SKIPPED=0
    for compiled_file in $(find "$FINAL_CLASSES_DIR" -name "*.class"); do
        rel="${compiled_file#$FINAL_CLASSES_DIR/}"
        if [ -n "${WOVEN_LOOKUP[$rel]+x}" ]; then
            cp "${WOVEN_LOOKUP[$rel]}" "$compiled_file"
            (( REPLACED++ )) || true
        else
            (( SKIPPED++ )) || true
        fi
    done

    echo "  Woven overlay: $REPLACED classes replaced, $SKIPPED kept as compiled"
else
    echo "Warning: No pod dump directory found at $DUMP_DIR — integration coverage will show mismatches"
fi

#Generate report

echo "Generating coverage report..."
java -jar "$JACOCO_CLI_JAR" report "$MERGED_EXEC" \
    --classfiles "$FINAL_CLASSES_DIR" \
    "${SOURCEFILES_ARGS[@]}" \
    --html "$REPORT_DIR" \
    --xml "${PROJECT_ROOT}/target/coverage-reports/jacoco.xml"

echo "Coverage report generated successfully at ${REPORT_DIR}/index.html"