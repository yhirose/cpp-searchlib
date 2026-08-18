#!/usr/bin/env bash
# Updates the vendored copy of cpp-peglib (third_party/peglib) to the newest
# release tag, and records it in README.md.
#
# Unlike fstlib/unicodelib, upstream tags releases (vX.Y.Z), so this tracks
# the newest tag rather than the default branch's tip -- a vendor bump never
# pulls in unreleased work.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/vendor_update_common.sh"
VENDOR_DIR="${SCRIPT_DIR}/../third_party/peglib"
REPO="yhirose/cpp-peglib"

TAG="$(latest_semver_tag "${REPO}")"
if [[ -z "${TAG}" ]]; then
    echo "error: could not resolve the newest tag for ${REPO}" >&2
    exit 1
fi
CURRENT_REV="$(current_revision "${VENDOR_DIR}")"

if [[ "${TAG}" == "${CURRENT_REV}" ]]; then
    echo "peglib: already at ${TAG}."
    exit 0
fi

echo "Updating peglib: ${CURRENT_REV} -> ${TAG}"
curl -fsSL "https://raw.githubusercontent.com/${REPO}/${TAG}/peglib.h" -o "${VENDOR_DIR}/peglib.h"
curl -fsSL "https://raw.githubusercontent.com/${REPO}/${TAG}/LICENSE" -o "${VENDOR_DIR}/LICENSE"
{
    echo "# cpp-peglib (vendored)"
    echo
    echo "Upstream: https://github.com/${REPO}"
    echo "Revision: ${TAG}"
    echo
    echo "\`peglib.h\` is copied verbatim from that tag; \`LICENSE\` is its own."
    echo "Update with \`just vendor-update peglib\`, which tracks the newest"
    echo "\`vX.Y.Z\` tag and writes it here."
    echo
    echo "The one consumer is \`src/query.cpp\`, which includes it as"
    echo '`"peglib/peglib.h"`.'
} > "${VENDOR_DIR}/README.md"

echo "Done. peglib drives query parsing end to end -- rebuild, run" \
     "\`just test\`, and read the tag's own changelog/diff before committing;" \
     "a grammar or API change here is not always source-compatible."
