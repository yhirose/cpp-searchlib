#!/usr/bin/env bash
# Updates the vendored cpp-segmentlib tree (third_party/segmentlib, plus its
# bundled third_party/cpp-fstlib and the test suite's model copy) to the
# newest release tag, and records it in the two README.md files.
#
# Unlike the single-header libraries, this is a directory tree, so the update
# is a tarball fetch + targeted copy rather than a raw.githubusercontent.com
# curl per file. Everything replaced here comes from that one tag, so the
# three pieces (segmentlib headers, its bundled fstlib, the MLP reference
# model) always move together.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/vendor_update_common.sh"
ROOT_DIR="${SCRIPT_DIR}/.."
REPO="yhirose/cpp-segmentlib"

TAG="$(latest_semver_tag "${REPO}")"
if [[ -z "${TAG}" ]]; then
    echo "error: could not resolve the newest tag for ${REPO}" >&2
    exit 1
fi
CURRENT_REV="$(current_revision "${ROOT_DIR}/third_party/segmentlib")"

if [[ "${TAG}" == "${CURRENT_REV}" ]]; then
    echo "segmentlib: already at ${TAG}."
    exit 0
fi

echo "Updating segmentlib: ${CURRENT_REV} -> ${TAG}"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

curl -fsSL "https://github.com/${REPO}/archive/refs/tags/${TAG}.tar.gz" | tar -xz -C "${WORK_DIR}"

# Validated before anything below touches the existing vendored tree: an empty
# or unexpected archive layout must fail here, not after third_party/segmentlib
# has already been rm -rf'd with nothing to replace it.
extracted_dirs=("${WORK_DIR}"/*/)
if [[ ${#extracted_dirs[@]} -ne 1 || ! -d "${extracted_dirs[0]}" ]]; then
    echo "error: expected exactly one extracted directory under ${WORK_DIR}, found ${#extracted_dirs[@]}" >&2
    exit 1
fi
EXTRACTED="${extracted_dirs[0]}"

rm -rf "${ROOT_DIR}/third_party/segmentlib"
cp -R "${EXTRACTED}include/segmentlib" "${ROOT_DIR}/third_party/segmentlib"
rm -rf "${ROOT_DIR}/third_party/cpp-fstlib"
cp -R "${EXTRACTED}third_party/cpp-fstlib" "${ROOT_DIR}/third_party/cpp-fstlib"

# The bundled MLP reference model, plus its own NOTICE (CC BY-SA 4.0, unlike
# this repository's MIT code -- see test/models/NOTICE). Requiring exactly one
# .mod file means a future tag that ships a second model fails loudly here
# instead of `find -exec` silently picking whichever one it visits last.
mod_files=()
while IFS= read -r -d '' f; do mod_files+=("$f"); done \
    < <(find "${EXTRACTED}models/mlp" -maxdepth 1 -name '*.mod' -print0)
if [[ ${#mod_files[@]} -ne 1 ]]; then
    echo "error: expected exactly one .mod file in ${EXTRACTED}models/mlp, found ${#mod_files[@]}" >&2
    exit 1
fi
cp "${mod_files[0]}" "${ROOT_DIR}/test/models/ja-ud-gsd.mod"
[[ -f "${EXTRACTED}models/mlp/NOTICE" ]] && cp "${EXTRACTED}models/mlp/NOTICE" "${ROOT_DIR}/test/models/NOTICE"

{
    echo "# cpp-segmentlib (vendored)"
    echo
    echo "Upstream: https://github.com/${REPO}"
    echo "Revision: ${TAG}"
    echo
    cat <<'BODY'
Japanese word segmentation (KyTea-compatible and MLP backends), for the CJK
tokenization the built-in `UTF8PlainTextTokenizer` cannot do: it splits on
Unicode letter runs, so a Japanese sentence with no spaces becomes one enormous
term.

Copied verbatim from that tag's `include/segmentlib/`. Update with
`just vendor-update segmentlib`, which replaces the tree, the bundled
`../cpp-fstlib/`, the test suite's model copy, and this line together, all
from the same tag.

Inference upstream is header-only and targets C++17, which is what makes it
droppable into this project (peglib/unicodelib/fstlib are here on the same
terms). Upstream's trainer and CLI need C++23 and are not vendored; models are
built there and only loaded here. Upstream also now bundles a trained MLP
reference model (`models/mlp/`, CC BY-SA 4.0, see upstream's NOTICE); a copy of
it lives in `test/models/` for the test suite, as described below.

## Using it

The headers refer to each other as `segmentlib/...`, so the include root is
`third_party`, not `third_party/segmentlib`:

    -isystem third_party

`-isystem` rather than `-I`: the automaton below is third-party code and is not
warning-clean under this project's flags (nine `-Wunused-parameter` hits at
`-Wall -Wextra`).

The one consumer is `src/segmentsplitter.cpp`, which implements
`searchlib::load_segmenting_splitter` (`include/searchlib_segment.h`). It is
built as its own CMake target, `searchlib-segment`, and that target carries the
only `-isystem third_party` in the tree -- see below for why the scoping
matters. Callers get a `TextSplitter`; no segmentlib type reaches the public
header.

Models are not vendored with the headers, since which one to load is the
caller's choice. `test/models/ja-ud-gsd.mod` is upstream's MLP reference model,
copied for the test suite together with its NOTICE (it is CC BY-SA 4.0, unlike
this repository's MIT code).

## The two cpp-fstlib copies

`../cpp-fstlib/fstlib.h` is segmentlib's own vendored fstlib, copied alongside
because `mlp/dictionary.h` holds an `fst::map` and includes it as
`"cpp-fstlib/fstlib.h"`. It is **a different revision** from this project's
own copy at `../fstlib/fstlib.h`, which `src/termdict.h` includes as
`"fstlib/fstlib.h"`. Keeping both is deliberate: segmentlib was written and
tested against its copy, and swapping in ours would be an unverified bet on
API compatibility. The directory names deliberately differ (`cpp-fstlib` vs
`fstlib`) precisely so the two can coexist under one include root without a
path collision -- `cpp-fstlib` keeps segmentlib's own vendoring choice
verbatim, `fstlib` is this project's own.

The two paths never collide on their own -- `cpp-fstlib/fstlib.h` and
`fstlib/fstlib.h` are distinct spellings -- but both define `namespace fst`, so
**one translation unit must not include both**. Verified: a file including
`src/termdict.h` and `segmentlib/segmenter.h` together fails with 20
redefinition errors.

This does not constrain the splitter that uses this, because the public header
does not pull in fstlib: `<searchlib.h>` plus `segmentlib/segmenter.h` compiles
cleanly. Only `src/termdict.h` (and the two `.cpp` files that include it) is off
limits in the same file as segmentlib.

The `searchlib-segment` target is how that stays true without relying on anyone
remembering it: the include path reaching these headers is `PRIVATE` to a
target holding exactly one source file, so no other translation unit can spell
`segmentlib/...` at all, let alone alongside `termdict.h`.
BODY
} > "${ROOT_DIR}/third_party/segmentlib/README.md"

{
    echo "# cpp-fstlib (vendored, for segmentlib)"
    echo
    echo "Upstream: https://github.com/yhirose/cpp-fstlib"
    CPP_FSTLIB_REV="$(sed -n 's/^Revision: *//p' "${EXTRACTED}third_party/cpp-fstlib/README.md" 2>/dev/null | head -1)"
    echo "Revision: ${CPP_FSTLIB_REV:-see the upstream third_party/cpp-fstlib/README.md at ${TAG}}"
    echo
    cat <<'BODY'
This is **segmentlib's** copy of fstlib, taken from
`third_party/cpp-fstlib/` of the cpp-segmentlib revision recorded in
`../segmentlib/README.md`. `segmentlib/mlp/dictionary.h` includes it as
`"cpp-fstlib/fstlib.h"`, which resolves here when `third_party` is on the
include path.

It is a different revision from this project's own copy at
`../fstlib/fstlib.h`, which `src/termdict.h` includes as `"fstlib/fstlib.h"`.
Both are kept, and the reason, plus the one rule that follows from it (no
translation unit may include both), are in `../segmentlib/README.md`.

Update this file only together with the segmentlib tree it belongs to (run
`just vendor-update segmentlib`, not this file by hand): its revision is
whatever that segmentlib revision vendored, not whatever is current upstream.
BODY
} > "${ROOT_DIR}/third_party/cpp-fstlib/README.md"

echo "Done. Segmentation output can change with a model or algorithm update --" \
     "rebuild, run \`just test\`, and check SegmentTest output before committing."
