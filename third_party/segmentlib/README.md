# cpp-segmentlib (vendored)

Upstream: https://github.com/yhirose/cpp-segmentlib
Revision: v0.2.0

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
