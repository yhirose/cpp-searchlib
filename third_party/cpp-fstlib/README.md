# cpp-fstlib (path shim for segmentlib)

No third-party code lives here. `segmentlib/mlp/dictionary.h` includes fstlib
as `"cpp-fstlib/fstlib.h"`, which is upstream's own vendoring path; the
`fstlib.h` here forwards that spelling to `../fstlib/fstlib.h`, this
project's copy, so both spellings resolve to one library.

It used to be a second vendored copy, at whatever revision segmentlib
bundled, and no translation unit could include both because both define
`namespace fst`. That rule ended when `searchlib.h` became a single header:
every consumer now sees fstlib, segmentlib's consumers included. Segmentation
was verified against this revision before the copy was dropped -- upstream's
reference model segments the same text identically.

The directory stays because it is the include path segmentlib's own headers
expect, and editing a tree that is supposed to be a verbatim copy is worse
than keeping a two-line file here.
