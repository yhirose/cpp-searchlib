# cpp-fstlib (vendored)

Upstream: https://github.com/yhirose/cpp-fstlib
Revision: v0.1.0

`fstlib.h` is copied verbatim from that tag; `LICENSE` is its own.
Update with `just vendor-update fstlib`, which tracks the newest `vX.Y.Z`
tag and replaces `fstlib.h` and this line together.

`include/searchlib.h` includes it as `<fstlib.h>`, so this directory itself
is the include path. It is also the copy segmentlib uses: `../cpp-fstlib/` is
a two-line forward to this one, standing in for the path segmentlib's own
headers spell (see its README). So an update here moves segmentlib's FST too
-- run `just test`, `SegmentTest` included.
