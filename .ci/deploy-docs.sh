#!/usr/bin/env bash
# =============================================================
# deploy-docs.sh  <DOC_TYPE> <ARTIFACT> <DOC_PATH>
#
# DOC_TYPE  : "Book" or "UserGuide"
# ARTIFACT  : path to .tar.gz from Maven build
# DOC_PATH  : repo path, e.g. "docs/TheBook"
#
# Required env vars:
#   BOOK_VERSION  e.g. "11.2"  (= CI_COMMIT_BRANCH)
#   MANUALS_DIR   e.g. "/data/www/old-dcache.org/manuals"
#   FRAGS_DIR     e.g. "/data/www/old-dcache.org/template/frags"
#   GLOBAL_FRAG   e.g. "header-docs-global.shtml"
# =============================================================
set -euo pipefail

DOC_TYPE="$1"
ARTIFACT="$2"
DOC_PATH="$3"

DEPLOY_DIR="${MANUALS_DIR}/${DOC_TYPE}-${BOOK_VERSION}"
STAGING_DIR="${DEPLOY_DIR}_staging"
GLOBAL_FRAG_PATH="${FRAGS_DIR}/${GLOBAL_FRAG}"

echo "=== Deploying ${DOC_TYPE} ${BOOK_VERSION} ==="

# ------------------------------------------------------------------
# Phase 1: Resolve the source fragment for the global header
#
# Use the global fragment itself if it already exists. 
# Otherwise find the highest-versioned fragment on the server
# ------------------------------------------------------------------
echo "=== Phase 1: Resolve nav header source ==="

if [ -f "${GLOBAL_FRAG_PATH}" ]; then
  echo "Using existing global fragment as base (in place, no copy needed)"
else
  SOURCE_FRAG=$(ls -1 "${FRAGS_DIR}"/header-docs-[0-9]*.shtml 2>/dev/null \
    | sort -t'-' -k3 -V | tail -1 || true)
  if [ -z "${SOURCE_FRAG}" ]; then
    echo "ERROR: No fragment found in ${FRAGS_DIR} to use as base." >&2
    exit 1
  fi
  echo "First-time bootstrap — using highest versioned fragment: ${SOURCE_FRAG}"
  cp "${SOURCE_FRAG}" "${GLOBAL_FRAG_PATH}"
fi

chmod 644 "${GLOBAL_FRAG_PATH}"

# ------------------------------------------------------------------
# Phase 2: Patch the global header with auto-discovered version list
# ------------------------------------------------------------------
echo "=== Phase 2: Rebuild nav version links ==="

# Discover all deployed versions from filesystem (sort by version, not name)
UG_VERSIONS=$(ls -d "${MANUALS_DIR}"/UserGuide-*/ 2>/dev/null \
  | sed 's|.*/UserGuide-||;s|/$||' | sort -V | paste -sd' ' || true)
BK_VERSIONS=$(ls -d "${MANUALS_DIR}"/Book-*/ 2>/dev/null \
  | sed 's|.*/Book-||;s|/$||' | sort -V | paste -sd' ' || true)

# Include the version being deployed right now, even before its dir exists
echo "$UG_VERSIONS" | grep -qw "$BOOK_VERSION" \
  || UG_VERSIONS="${UG_VERSIONS} ${BOOK_VERSION}"
echo "$BK_VERSIONS" | grep -qw "$BOOK_VERSION" \
  || BK_VERSIONS="${BK_VERSIONS} ${BOOK_VERSION}"

export UG_VERSIONS BK_VERSIONS BOOK_VERSION

perl -0777 -pi -e '
  my @ug_vers = split(" ", $ENV{"UG_VERSIONS"});
  my @bk_vers = split(" ", $ENV{"BK_VERSIONS"});

  sub nav_links {
    my ($type, $list) = @_;
    return join(", ", map { qq(<a href="/manuals/$type-$_/">$_</a>) } @$list);
  }

  my $ug_html = nav_links("UserGuide", \@ug_vers);
  my $bk_html = nav_links("Book",      \@bk_vers);

  # Replace the version link sections in-place
  s|(User\s*Guide:\s*)(.*?)(\s*\|)|${1}${ug_html} <br/> |is;
  s|(Book:\s*)(.*?)(\s*\|\s*<a[^>]*Wiki[^>]*>)|${1}${bk_html} |is;

  unless (/addEventListener.*book-navi/s) {
    $_ .= q(<script>
window.addEventListener("DOMContentLoaded", () => {
  const p = window.location.pathname;
  document.querySelectorAll(".book-navi a").forEach(a => {
    const h = a.getAttribute("href");
    if (h && p.startsWith(h)) {
      a.style.cssText = "font-weight:bold;color:black;text-decoration:underline";
    }
  });
});
</script>);
  }
' "${GLOBAL_FRAG_PATH}"

echo "Global header updated: ${GLOBAL_FRAG_PATH}"

# ------------------------------------------------------------------
# Phase 3: Build staging area for the new version
# ------------------------------------------------------------------
echo "=== Phase 3: Extract artifact to staging ==="
rm -rf "$STAGING_DIR" && mkdir -p "$STAGING_DIR"
tar -xzf "${ARTIFACT}" -C "$STAGING_DIR"

# ------------------------------------------------------------------
# Phase 4: Patch Maven placeholders + wire up global header in new version
# ------------------------------------------------------------------
echo "=== Phase 4: Patch placeholders in new version ==="
GITHUB_PREFIX="https://github.com/dCache/dcache/blob/master/${DOC_PATH}/src/main/markdown"

find "$STAGING_DIR" -name "*.shtml" -print0 | xargs -0 sed -i \
  -e "s|@parsedVersion.majorVersion@.@parsedVersion.minorVersion@|${BOOK_VERSION}|g" \
  -e "s|@github.url-prefix@|${GITHUB_PREFIX}|g" \
  -e "s|github-fork-ribbon[^\"]*\.shtml|github-fork-ribbon.md|g" \
  -e "s|header-docs-[a-zA-Z0-9._-]*\.shtml|${GLOBAL_FRAG}|g"

# ------------------------------------------------------------------
# Phase 5: Atomic swap: new version goes live
# ------------------------------------------------------------------
echo "=== Phase 5: Atomic swap ==="
[ -d "${DEPLOY_DIR}" ] && mv "${DEPLOY_DIR}" "${DEPLOY_DIR}_old"
mv "$STAGING_DIR" "${DEPLOY_DIR}"
chmod -R 755 "${DEPLOY_DIR}"
echo "${DOC_TYPE} ${BOOK_VERSION} is live at ${DEPLOY_DIR}"

# ------------------------------------------------------------------
# Phase 6: Patch nav reference in ALL other deployed versions on disk
# ------------------------------------------------------------------
echo "=== Phase 6: Propagate global header to all other deployed versions ==="
find "${MANUALS_DIR}" -name "*.shtml" \
  ! -path "${DEPLOY_DIR}/*" \
  -print0 | xargs -0 --no-run-if-empty sed -i \
  "s|header-docs-[a-zA-Z0-9._-]*\.shtml|${GLOBAL_FRAG}|g"

echo "All deployed versions now reference ${GLOBAL_FRAG}"
echo "=== Done: ${DOC_TYPE} ${BOOK_VERSION} deployed and all versions patched ==="