#!/usr/bin/env bash
# Dispatcher for `just vendor-update [name]`. With no argument, runs every
# third_party/*/vendor_update_<name>.sh script in turn; with a name, runs
# just that one. Each script updates its working tree in place -- review the
# diff, rebuild, and `just test` before committing (see third_party/README.md
# for what each one tracks and why they are not committed automatically).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ALL=(fstlib unicodelib peglib segmentlib)

if [[ $# -gt 1 ]]; then
    echo "usage: $0 [${ALL[*]// /|}]" >&2
    exit 1
fi

if [[ $# -eq 1 ]]; then
    name="$1"
    script="${SCRIPT_DIR}/vendor_update_${name}.sh"
    if [[ ! -x "${script}" ]]; then
        echo "error: no vendor_update script for '${name}' (known: ${ALL[*]})" >&2
        exit 1
    fi
    exec "${script}"
fi

for name in "${ALL[@]}"; do
    echo "=== ${name} ==="
    "${SCRIPT_DIR}/vendor_update_${name}.sh"
    echo
done
