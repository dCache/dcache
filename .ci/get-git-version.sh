#!/bin/bash

# Function to decrement a version string
decrement_version() {
    local version=$1
    local major minor patch

    # Extract major, minor, and patch components
    IFS='.' read -r major minor patch <<< "$version"

    if [[ $patch -gt 0 ]]; then
        # Decrement patch version
        patch=$((patch - 1))
    elif [[ $minor -gt 0 ]]; then
        # Decrement minor version and reset patch to max value (assumed to be 9 for simplicity)
        minor=$((minor - 1))
        patch=0
    elif [[ $major -gt 0 ]]; then
        # Decrement major version, reset minor and patch to max value
        major=$((major - 1))
        minor=`git tag -l "$major.*" | sort -V -r | head -1 | cut -d '.' -f 2`
        patch=0
    else
        # If all components are zero, cannot decrement further
        echo "Version cannot be decremented further."
        exit 1
    fi

    echo "$major.$minor.$patch"
}

# Main script
if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <version>"
    exit 1
fi

version=$1

# Validate version format
if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Invalid version format. Expected format: major.minor.patch (e.g., 0.25.0)"
    exit 1
fi

previous_version=$(decrement_version "$version")
echo "$previous_version"

