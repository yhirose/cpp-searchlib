# cpp-segmentlib (vendored)

Upstream: https://github.com/yhirose/cpp-segmentlib
Revision: v0.1.0 (cb8b73d)

Japanese word segmentation (KyTea-compatible and MLP backends), for the CJK
tokenization the built-in `UTF8PlainTextTokenizer` cannot do: it splits on
Unicode letter runs, so a Japanese sentence with no spaces becomes one enormous
term.

Copied verbatim from that tag's `include/segmentlib/`. Upstream now tags
releases (`vX.Y.Z`, `scripts/release.sh`); track a tag here rather than an
arbitrary commit. Update by replacing the tree and this line together -- the
diff against the previous vendored revision (5987d9c) was 8 lines, the new
`SEGMENTLIB_VERSION`/`SEGMENTLIB_VERSION_NUM` macros in `types.h`.

Inference upstream is header-only and targets C++17, which is what makes it
droppable into this project (peglib/unicodelib/fstlib are here on the same
terms). Upstream's trainer and CLI need C++23 and are not vendored; models are
built there and only loaded here. Upstream also now bundles a trained MLP
reference model (`models/mlp/`, CC BY-SA 4.0, see upstream's NOTICE); a copy of
it lives in `test/models/` for the test suite, as described below.

## Using it

The headers refer to each other as `segmentlib/...`, so the include root is
`src/lib`, not `src/lib/segmentlib`:

    -isystem src/lib

`-isystem` rather than `-I`: the automaton below is third-party code and is not
warning-clean under this project's flags (nine `-Wunused-parameter` hits at
`-Wall -Wextra`).

The one consumer is `src/segmentsplitter.cpp`, which implements
`searchlib::load_segmenting_splitter` (`include/searchlib_segment.h`). It is
built as its own CMake target, `searchlib-segment`, and that target carries the
only `-isystem src/lib` in the tree -- see below for why the scoping matters.
Callers get a `TextSplitter`; no segmentlib type reaches the public header.

Models are not vendored with the headers, since which one to load is the
caller's choice. `test/models/ja-ud-gsd.mod` is upstream's MLP reference model,
copied for the test suite together with its NOTICE (it is CC BY-SA 4.0, unlike
this repository's MIT code).

## The two cpp-fstlib copies

`../cpp-fstlib/fstlib.h` is segmentlib's own vendored fstlib, copied alongside
because `mlp/dictionary.h` holds an `fst::map` and includes it as
`"cpp-fstlib/fstlib.h"`. It is **a different revision** from this project's
`../fstlib.h`, which `src/termdict.h` includes as `"lib/fstlib.h"`. Keeping
both is deliberate: segmentlib was written and tested against its copy, and
swapping in ours would be an unverified bet on API compatibility.

The two paths never collide on their own -- `cpp-fstlib/fstlib.h` and
`lib/fstlib.h` are distinct spellings -- but both define `namespace fst`, so
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
