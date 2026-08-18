#!/usr/bin/env bash
# Shared helpers for scripts/vendor_update_*.sh. Sourced, never executed
# directly -- it has no shebang-driven behavior of its own.

# current_revision <vendor_dir> -- the Revision: value already recorded in
# <vendor_dir>/README.md.
current_revision() {
    grep '^Revision:' "$1/README.md" | awk '{print $2}'
}

# latest_head_sha <owner/repo> -- the tip commit SHA of the repo's default
# branch. For upstreams that publish no tags, this is what "latest" means.
latest_head_sha() {
    curl -fsSL "https://api.github.com/repos/$1/commits/HEAD" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["sha"])'
}

# latest_semver_tag <owner/repo> -- the newest vX.Y.Z tag by `sort -V`,
# skipping anything that looks like a prerelease (a `-` in the name).
latest_semver_tag() {
    git ls-remote --tags --refs "https://github.com/$1.git" \
        | awk '{print $2}' | sed 's#refs/tags/##' | grep -v -- '-' | sort -V | tail -1
}

# update_revision_line <vendor_dir> <revision> -- rewrites the Revision: line
# in <vendor_dir>/README.md in place. Only fits scripts whose README is a
# hand-maintained file with one line to update (fstlib, unicodelib); peglib
# and segmentlib regenerate their whole README instead, so they don't use this.
update_revision_line() {
    sed -i.bak "s/^Revision: .*/Revision: $2/" "$1/README.md"
    rm -f "$1/README.md.bak"
}
