#!/bin/bash

# Define paths
PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
JACOCO_VERSION="0.8.14"
JACOCO_DIR="$HOME/jacoco-$JACOCO_VERSION"
JACOCO_CLI_JAR="$JACOCO_DIR/lib/jacococli.jar"
MERGED_EXEC="$PROJECT_ROOT/target/coverage-reports/merged.exec"
REPORT_DIR="$PROJECT_ROOT/target/coverage-reports/site"

# Ensure the report directory exists
mkdir -p "$REPORT_DIR"

# Debug: Print paths
echo "DEBUG: PROJECT_ROOT: $PROJECT_ROOT"
echo "DEBUG: JACOCO_DIR: $JACOCO_DIR"
echo "DEBUG: JACOCO_CLI_JAR: $JACOCO_CLI_JAR"
echo "DEBUG: MERGED_EXEC: $MERGED_EXEC"
echo "DEBUG: REPORT_DIR: $REPORT_DIR"

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

# Find all jacoco-ut.exec files dynamically
echo "DEBUG: Searching for jacoco-ut.exec files in $PROJECT_ROOT"
EXEC_FILES=($(find "$PROJECT_ROOT" -name "jacoco-ut.exec" -type f))

# Debug: Print found exec files
echo "DEBUG: Found execution data files:"
for exec_file in "${EXEC_FILES[@]}"; do
    echo "DEBUG: $exec_file"
done

# Check if any execution data files were found
if [ ${#EXEC_FILES[@]} -eq 0 ]; then
    echo "Error: No jacoco-ut.exec files found in $PROJECT_ROOT"
    echo "DEBUG: Listing all files in $PROJECT_ROOT to verify existence of jacoco-ut.exec files:"
    find "$PROJECT_ROOT" -type f -name "*.exec" -exec ls -la {} \;
    exit 1
fi

# Check if all execution data files exist
for exec_file in "${EXEC_FILES[@]}"; do
    if [ ! -f "$exec_file" ]; then
        echo "Error: Execution data file not found at $exec_file"
        exit 1
    fi
done

# Merge execution data files
echo "Merging execution data files..."
java -jar "$JACOCO_CLI_JAR" merge "${EXEC_FILES[@]}" --destfile "$MERGED_EXEC"

if [ $? -ne 0 ]; then
    echo "Error: Failed to merge execution data files"
    exit 1
fi

# Generate the coverage report
echo "Generating coverage report..."

# Build classfiles and sourcefiles arguments dynamically
CLASSFILES_ARGS=()
SOURCEFILES_ARGS=()

for exec_file in "${EXEC_FILES[@]}"; do
    # Extract module path (e.g., core, dlm, rquota) from the exec file path
    module_path=$(dirname "$(dirname "$(dirname "$exec_file")")")
    module_name=$(basename "$module_path")

    # Debug: Print module paths
    echo "DEBUG: Module path: $module_path"
    echo "DEBUG: Module name: $module_name"

    # Check if the target/classes directory exists
    if [ ! -d "$module_path/target/classes" ]; then
        echo "DEBUG: Directory $module_path/target/classes does not exist"
    else
        echo "DEBUG: Directory $module_path/target/classes exists"
    fi

    # Check if the src/main/java directory exists
    if [ ! -d "$module_path/src/main/java" ]; then
        echo "DEBUG: Directory $module_path/src/main/java does not exist"
    else
        echo "DEBUG: Directory $module_path/src/main/java exists"
    fi

    # Add classfiles and sourcefiles arguments
    CLASSFILES_ARGS+=("--classfiles" "$module_path/target/classes")
    SOURCEFILES_ARGS+=("--sourcefiles" "$module_path/src/main/java")
done

# Debug: Print all classfiles and sourcefiles arguments
echo "DEBUG: CLASSFILES_ARGS:"
printf "%s\n" "${CLASSFILES_ARGS[@]}"
echo "DEBUG: SOURCEFILES_ARGS:"
printf "%s\n" "${SOURCEFILES_ARGS[@]}"

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