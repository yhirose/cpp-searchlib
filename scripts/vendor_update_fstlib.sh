#!/usr/bin/env bash
# Updates the vendored copy of cpp-fstlib (third_party/fstlib) to the newest
# release tag, and records it in README.md.
#
# Upstream tags releases (vX.Y.Z) and bumps minor when the byte code format
# changes, so this tracks the newest tag rather than the default branch's tip
# -- a vendor bump never pulls in unreleased work.
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

TAG="$(latest_semver_tag "${REPO}")"
if [[ -z "${TAG}" ]]; then
    echo "error: could not resolve the newest tag for ${REPO}" >&2
    exit 1
fi
CURRENT_REV="$(current_revision "${VENDOR_DIR}")"

if [[ "${TAG}" == "${CURRENT_REV}" ]]; then
    echo "fstlib: already at ${TAG}."
    exit 0
fi

echo "Updating fstlib: ${CURRENT_REV} -> ${TAG}"
curl -fsSL "https://raw.githubusercontent.com/${REPO}/${TAG}/fstlib.h" -o "${VENDOR_DIR}/fstlib.h"
curl -fsSL "https://raw.githubusercontent.com/${REPO}/${TAG}/LICENSE" -o "${VENDOR_DIR}/LICENSE"
update_revision_line "${VENDOR_DIR}" "${TAG}"

echo "Done. Review the diff (upstream may have moved behavior, not just this" \
     "header's contents), then rebuild and \`just test\` before committing."
