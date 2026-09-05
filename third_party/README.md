# Third-party code

Every subdirectory here except `cpp-fstlib/` is copied from an upstream
repository, not written in this project. Each one carries its own `README.md`
recording the upstream URL and the exact revision vendored, so "is this
current" is always answerable by reading a file rather than guessing from a
commit date. Update with `just vendor-update [name]` (`just vendor-update`
with no argument checks and updates all of them); see
`scripts/vendor_update_*.sh`.

| Directory      | Upstream          | Tracks           |
|----------------|--------------------|-------------------|
| `fstlib/`      | cpp-fstlib         | branch tip (no tags) |
| `peglib/`      | cpp-peglib         | latest `vX.Y.Z` tag |
| `unicodelib/`  | cpp-unicodelib     | branch tip (no tags) |
| `segmentlib/`  | cpp-segmentlib     | latest `vX.Y.Z` tag |
| `cpp-fstlib/`  | --                 | a forward to `fstlib/` |

`searchlib.h` includes the first three as `<fstlib.h>`, `<peglib.h>`,
`<unicodelib.h>` and `<unicodelib_encodings.h>`, so each of those directories
is itself an include path. A project that already vendors any of them points
at its own copy and gets one definition of each; that is what lets culebra
build this library against its own peglib and unicodelib.

`segmentlib/` is the exception: its headers refer to each other as
`segmentlib/...`, so its include root is `third_party` itself, and only the
opt-in `searchlib_segment.h` needs it.

`cpp-fstlib/` holds no vendored code. `segmentlib/mlp/dictionary.h` includes
fstlib as `"cpp-fstlib/fstlib.h"`, upstream's own vendoring path, so a
two-line header there forwards it to `fstlib/`. It used to be a second copy
of fstlib at segmentlib's revision, which meant no translation unit could
include both; the single-header `searchlib.h` ended that arrangement, since
every consumer now sees fstlib. See `cpp-fstlib/README.md`.
