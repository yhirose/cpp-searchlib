#!/usr/bin/env bash
# Shared helpers for scripts/vendor_update_*.sh. Sourced, never executed
# directly -- it has no shebang-driven behavior of its own.
#
# The callers run under `set -euo pipefail`, where a grep that matches
# nothing ends the whole script on the spot, with no message at all. So the
# lookups below swallow a no-match with `|| true` and leave the decision to
# an explicit check that says what was missing: an empty answer from
# latest_semver_tag / latest_head_sha is reported by the caller, and
# current_revision reports its own.

# current_revision <vendor_dir> -- the Revision: value already recorded in
# <vendor_dir>/README.md. Fails, with a message, when there is no README or
# no Revision: line in it: comparing against an empty revision would make
# every run look like an update, and say "Updating :  -> vX.Y.Z".
current_revision() {
    local readme="$1/README.md"
    if [[ ! -f "${readme}" ]]; then
        echo "error: ${readme} does not exist, so the vendored revision is unknown" >&2
        return 1
    fi
    local rev
    rev="$(grep '^Revision:' "${readme}" | awk '{print $2}' || true)"
    if [[ -z "${rev}" ]]; then
        echo "error: no 'Revision:' line in ${readme}" >&2
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
