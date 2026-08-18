# Third-party code

Every subdirectory here is copied from an upstream repository, not written in
this project. Each one carries its own `README.md` recording the upstream URL
and the exact revision vendored, so "is this current" is always answerable by
reading a file rather than guessing from a commit date. Update with
`just vendor-update [name]` (`just vendor-update` with no argument checks and
updates all of them); see `scripts/vendor_update_*.sh`.

| Directory      | Upstream          | Tracks           |
|----------------|--------------------|-------------------|
| `fstlib/`      | cpp-fstlib         | branch tip (no tags) |
| `peglib/`      | cpp-peglib         | latest `vX.Y.Z` tag |
| `unicodelib/`  | cpp-unicodelib     | branch tip (no tags) |
| `segmentlib/`  | cpp-segmentlib     | latest `vX.Y.Z` tag |
| `cpp-fstlib/`  | cpp-fstlib (again) | whatever `segmentlib/` bundles |

`cpp-fstlib/` is not a separate top-level dependency of this project -- it is
**segmentlib's own** vendored copy of fstlib, carried along because
`segmentlib/mlp/dictionary.h` includes it as `"cpp-fstlib/fstlib.h"`. It sits
here, alongside `segmentlib/`, rather than nested inside it, because that is
the include path segmentlib's own headers expect. It is deliberately a
different revision from `fstlib/`, this project's own direct copy of the same
library -- see `segmentlib/README.md` for why both exist and the one rule
that follows from it (no translation unit may include both; both define
`namespace fst`).

The directory names encode that split: `fstlib` is this project's own choice
of name for its own copy; `cpp-fstlib` is segmentlib's own vendoring choice,
kept verbatim so its internal `#include "cpp-fstlib/fstlib.h"` keeps
resolving without editing anything inside a tree that is supposed to be a
verbatim copy.
