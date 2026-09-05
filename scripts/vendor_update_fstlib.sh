#!/usr/bin/env bash
# Updates the vendored copy of cpp-fstlib (third_party/fstlib) to the tip of
# upstream's default branch, and records the revision in README.md.
#
# Upstream publishes no tags, so "latest" always means the tip commit of the
# default branch at the time this runs.
#
# This is the only copy of fstlib in the tree: third_party/cpp-fstlib is a
# two-line forward to it, there for the path segmentlib's own headers spell.
# So an update here moves segmentlib's FST too -- run the tests, SegmentTest
# included. See third_party/README.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/vendor_update_common.sh"
VENDOR_DIR="${SCRIPT_DIR}/../third_party/fstlib"
REPO="yhirose/cpp-fstlib"

REV="$(latest_head_sha "${REPO}")"
CURRENT_REV="$(current_revision "${VENDOR_DIR}")"

if [[ "${REV}" == "${CURRENT_REV}" ]]; then
    echo "fstlib: already at upstream HEAD (${REV})."
    exit 0
fi

echo "Updating fstlib: ${CURRENT_REV} -> ${REV}"
curl -fsSL "https://raw.githubusercontent.com/${REPO}/${REV}/fstlib.h" -o "${VENDOR_DIR}/fstlib.h"
curl -fsSL "https://raw.githubusercontent.com/${REPO}/${REV}/LICENSE" -o "${VENDOR_DIR}/LICENSE"
update_revision_line "${VENDOR_DIR}" "${REV}"

echo "Done. Review the diff (upstream may have moved behavior, not just this" \
     "header's contents), then rebuild and \`just test\` before committing."
