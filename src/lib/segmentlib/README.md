# cpp-segmentlib (vendored)

Upstream: https://github.com/yhirose/cpp-segmentlib
Revision: 5987d9c5762093d6f765921d868dc18845d83eaa

Japanese word segmentation (KyTea-compatible and MLP backends), for the CJK
tokenization the built-in `UTF8PlainTextTokenizer` cannot do: it splits on
Unicode letter runs, so a Japanese sentence with no spaces becomes one enormous
term.

Copied verbatim from that revision's `include/segmentlib/`. Upstream publishes
no tags, so the revision above is what "current" means here. Update by
replacing the tree and this line together.

Inference upstream is header-only and targets C++17, which is what makes it
droppable into this project (peglib/unicodelib/fstlib are here on the same
terms). Upstream's trainer and CLI need C++23 and are not vendored; models are
built there and only loaded here.

## Using it

The headers refer to each other as `segmentlib/...`, so the include root is
`src/lib`, not `src/lib/segmentlib`:

    -isystem src/lib

`-isystem` rather than `-I`: the automaton below is third-party code and is not
warning-clean under this project's flags (nine `-Wunused-parameter` hits at
`-Wall -Wextra`).

Nothing consumes this yet. There is no `Tokenizer<T>` adapter and no build
target refers to these files, so the vendored tree costs nothing until one is
written.

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

This does not constrain the adapter that will eventually use this, because the
public header does not pull in fstlib: `<searchlib.h>` plus
`segmentlib/segmenter.h` compiles cleanly. Only `src/termdict.h` (and the two
`.cpp` files that include it) is off limits in the same file as segmentlib.
