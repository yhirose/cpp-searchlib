# cpp-fstlib (vendored, for segmentlib)

Upstream: https://github.com/yhirose/cpp-fstlib
Revision: 2c9af63710777ee69b4b9062aa98be4349e93d88

This is **segmentlib's** copy of fstlib, taken from
`third_party/cpp-fstlib/` of the cpp-segmentlib revision recorded in
`../segmentlib/README.md`. `segmentlib/mlp/dictionary.h` includes it as
`"cpp-fstlib/fstlib.h"`, which resolves here when `third_party` is on the
include path.

It is a different revision from this project's own copy at
`../fstlib/fstlib.h`, which `src/termdict.h` includes as `"fstlib/fstlib.h"`.
Both are kept, and the reason, plus the one rule that follows from it (no
translation unit may include both), are in `../segmentlib/README.md`.

Update this file only together with the segmentlib tree it belongs to (run
`just vendor-update segmentlib`, not this file by hand): its revision is
whatever that segmentlib revision vendored, not whatever is current upstream.
