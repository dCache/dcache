#!/bin/sh

# Derive repo root from git, or accept it as $1.
REPO_ROOT="${1:-$(git rev-parse --show-toplevel 2>/dev/null)}"

if [ -z "$REPO_ROOT" ]; then
    echo "ERROR: Could not determine repo root." \
         "Either run from inside the git repo or pass the path as \$1." >&2
    exit 1
fi

DEB_NAME=$(ls "$REPO_ROOT/packages/fhs/target/" 2>/dev/null | grep 'dcache.*\.deb$' | head -1)
TAR_NAME=$(ls "$REPO_ROOT/packages/tar/target/" 2>/dev/null | grep 'dcache.*\.tar\.gz$' | head -1)
RPM_NAME=$(ls "$REPO_ROOT/packages/fhs/target/rpmbuild/RPMS/noarch/" 2>/dev/null | grep 'dcache.*\.rpm$' | head -1)

# Validate all packages were found before trying to checksum them
missing=0
for varname in DEB_NAME TAR_NAME RPM_NAME; do
    eval val=\$$varname
    if [ -z "$val" ]; then
        echo "ERROR: Could not find package for $varname under $REPO_ROOT" >&2
        missing=1
    fi
done
[ $missing -eq 0 ] || exit 1

DEB_SUM=$(md5sum "$REPO_ROOT/packages/fhs/target/$DEB_NAME" | cut -d ' ' -f 1)
TAR_SUM=$(md5sum "$REPO_ROOT/packages/tar/target/$TAR_NAME" | cut -d ' ' -f 1)
RPM_SUM=$(md5sum "$REPO_ROOT/packages/fhs/target/rpmbuild/RPMS/noarch/$RPM_NAME" | cut -d ' ' -f 1)
DATE=$(date +"%Y.%m.%d")

echo "| Download   | Build date | md5 sum  |"
echo "|:-----------|:-----------|----------|"
echo "| $RPM_NAME  | $DATE      | $RPM_SUM |"
echo "| $DEB_NAME  | $DATE      | $DEB_SUM |"
echo "| $TAR_NAME  | $DATE      | $TAR_SUM |"

echo ; echo; echo
CURRENT_TAG=$(git -C "$REPO_ROOT" describe --tags --abbrev=0 --always)
PREV_TAG=$(git -C "$REPO_ROOT" describe --tags --abbrev=0 --always HEAD^)
git -C "$REPO_ROOT" log \
    "${PREV_TAG}...${CURRENT_TAG}" \
    --no-merges \
    --format='[%h](https://github.com/dcache/dcache/commit/%H)%n:    %s%n'

