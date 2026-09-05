# cpp-peglib (vendored)

Upstream: https://github.com/yhirose/cpp-peglib
Revision: v1.17.0

`peglib.h` is copied verbatim from that tag; `LICENSE` is its own.
Update with `just vendor-update peglib`, which tracks the newest
`vX.Y.Z` tag and writes it here.

The one consumer is `include/searchlib.h`, which includes it as
`<peglib.h>` -- so this directory itself is the include path.
