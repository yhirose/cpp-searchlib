#!/usr/bin/env bash
# Updates the vendored cpp-segmentlib tree (third_party/segmentlib, plus the
# test suite's model copy) to the newest release tag, and records it in the
# README.md.
#
# Unlike the single-header libraries, this is a directory tree, so the update
# is a tarball fetch + targeted copy rather than a raw.githubusercontent.com
# curl per file. Both pieces replaced here (segmentlib headers, the MLP
# reference model) come from that one tag, so they always move together.
#
# Upstream's own third_party/cpp-fstlib is deliberately NOT copied: this
# project has its own fstlib and segmentlib is built against it (see
# third_party/cpp-fstlib/README.md). Segmentation output is what proves that
# still holds, so run the tests after every update.
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
`just vendor-update segmentlib`, which replaces the tree, the test suite's
model copy, and this line together, all from the same tag.

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

The one consumer is `include/searchlib_segment.h`, which implements
`searchlib::load_segmenting_splitter`. It is a header of its own rather than
part of `searchlib.h` so that only a caller who wants CJK segmentation
compiles segmentlib and carries `-isystem third_party`; the CMake target that
supplies that path is `searchlib-segment`. Callers get a `TextSplitter`; no
segmentlib type reaches `searchlib.h`.

Models are not vendored with the headers, since which one to load is the
caller's choice. `test/models/ja-ud-gsd.mod` is upstream's MLP reference model,
copied for the test suite together with its NOTICE (it is CC BY-SA 4.0, unlike
this repository's MIT code).

## fstlib

`mlp/dictionary.h` holds an `fst::map` and includes it as
`"cpp-fstlib/fstlib.h"`, upstream's own vendoring path. This project does not
keep a second copy there: `../cpp-fstlib/fstlib.h` forwards to
`../fstlib/fstlib.h`, so both spellings resolve to one library and one
`namespace fst`.

Upstream bundles its own revision, and it used to be copied here, which meant
no translation unit could include both. That stopped being workable when
`searchlib.h` became a single header carrying the FST term dictionary --
every consumer sees fstlib now. Segmentation was verified against this
project's revision before the copy was dropped, and `SegmentTest` is what
keeps it verified.
BODY
} > "${ROOT_DIR}/third_party/segmentlib/README.md"

echo "Done. Segmentation output can change with a model or algorithm update --" \
     "rebuild, run \`just test\`, and check SegmentTest output before committing."
