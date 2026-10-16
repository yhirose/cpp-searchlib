#!/usr/bin/env bash
# Shared helpers for scripts/vendor_update_*.sh. Sourced, never executed
# directly -- it has no shebang-driven behavior of its own.
#
# The callers run under `set -euo pipefail`, where a grep matching nothing
# would end them with no message; so a lookup that finds nothing answers
# empty instead, and the empty answer is reported with what was missing.

# current_revision <vendor_dir> -- the Revision: value already recorded in
# <vendor_dir>/README.md. Fails, with a message, when there is none: an empty
# revision would make every run look like an update.
current_revision() {
    local rev
    rev="$(awk '/^Revision:/ { print $2; exit }' "$1/README.md" 2>/dev/null || true)"
    if [[ -z "${rev}" ]]; then
        echo "error: no 'Revision:' line in $1/README.md (or no such file)" >&2
        return 1
    fi
    echo "${rev}"
}

# latest_head_sha <owner/repo> -- the tip commit SHA of the repo's default
# branch. For upstreams that publish no tags (unicodelib), this is what
# "latest" means. Empty when the lookup fails; the caller reports it.
latest_head_sha() {
    curl -fsSL "https://api.github.com/repos/$1/commits/HEAD" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["sha"])' 2>/dev/null || true
}

# latest_semver_tag <owner/repo> -- the newest vX.Y.Z tag by `sort -V`,
# skipping anything that looks like a prerelease (a `-` in the name). Empty
# when the remote cannot be reached or has no such tag; the caller reports it.
latest_semver_tag() {
    git ls-remote --tags --refs "https://github.com/$1.git" \
        | awk '{print $2}' | sed 's#refs/tags/##' | grep -v -- '-' | sort -V | tail -1 || true
}

# update_revision_line <vendor_dir> <revision> -- rewrites the Revision: line
# in <vendor_dir>/README.md in place. Only fits scripts whose README is a
# hand-maintained file with one line to update (fstlib, unicodelib); peglib
# and segmentlib regenerate their whole README instead, so they don't use this.
update_revision_line() {
    sed -i.bak "s/^Revision: .*/Revision: $2/" "$1/README.md"
    rm -f "$1/README.md.bak"
}
