# cpp-fstlib (vendored)

Upstream: https://github.com/yhirose/cpp-fstlib
Revision: 2d545d23500e814d56733693be25245054d01690

`fstlib.h` is copied verbatim from that revision; `LICENSE` is its own.
Upstream publishes no tags, so the revision above is what "current" means
here. Update with `just vendor-update fstlib`, which replaces `fstlib.h` and
this line together.

The one consumer is `src/termdict.h`, which includes it as
`"fstlib/fstlib.h"`.

This is a **different revision** from `../cpp-fstlib/`, segmentlib's own
bundled copy of the same library. See `../cpp-fstlib/README.md` and
`../segmentlib/README.md` for why both exist and the one rule that follows
from it (no translation unit may include both).
