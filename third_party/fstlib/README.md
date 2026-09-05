# cpp-fstlib (vendored)

Upstream: https://github.com/yhirose/cpp-fstlib
Revision: 2d545d23500e814d56733693be25245054d01690

`fstlib.h` is copied verbatim from that revision; `LICENSE` is its own.
Upstream publishes no tags, so the revision above is what "current" means
here. Update with `just vendor-update fstlib`, which replaces `fstlib.h` and
this line together.

`include/searchlib.h` includes it as `<fstlib.h>`, so this directory itself
is the include path. It is also the copy segmentlib uses: `../cpp-fstlib/` is
a two-line forward to this one, standing in for the path segmentlib's own
headers spell (see its README). So an update here moves segmentlib's FST too
-- run `just test`, `SegmentTest` included.
