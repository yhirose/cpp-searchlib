#!/usr/bin/env bash
# Updates the vendored copy of cpp-unicodelib (third_party/unicodelib) to the
# tip of upstream's default branch, and records the revision in README.md.
#
# Upstream publishes no tags, so "latest" always means the tip commit of the
# default branch at the time this runs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/vendor_update_common.sh"
VENDOR_DIR="${SCRIPT_DIR}/../third_party/unicodelib"
REPO="yhirose/cpp-unicodelib"

REV="$(latest_head_sha "${REPO}")"
CURRENT_REV="$(current_revision "${VENDOR_DIR}")"

if [[ "${REV}" == "${CURRENT_REV}" ]]; then
    echo "unicodelib: already at upstream HEAD (${REV})."
    exit 0
fi

echo "Updating unicodelib: ${CURRENT_REV} -> ${REV}"
for f in unicodelib.h unicodelib_encodings.h unicodelib_names.h; do
    curl -fsSL "https://raw.githubusercontent.com/${REPO}/${REV}/${f}" -o "${VENDOR_DIR}/${f}"
done
curl -fsSL "https://raw.githubusercontent.com/${REPO}/${REV}/LICENSE" -o "${VENDOR_DIR}/LICENSE"
update_revision_line "${VENDOR_DIR}" "${REV}"

echo "Done. is_letter()/script() results can shift with a Unicode data update" \
     "-- rebuild, run \`just test\`, and check for term-boundary changes on" \
     "any text this project indexes before committing."
