# cpp-unicodelib (vendored)

Upstream: https://github.com/yhirose/cpp-unicodelib
Revision: 469224e560fb3000615fde3186e654d617667c5a

`unicodelib.h`, `unicodelib_encodings.h` and `unicodelib_names.h` are copied
verbatim from that revision; `LICENSE` is its own. Upstream publishes no
tags, so the revision above is what "current" means here. Update with
`just vendor-update unicodelib`, which replaces all three headers and this
line together.

`include/searchlib.h` includes it as `<unicodelib.h>` and
`<unicodelib_encodings.h>`, so this directory itself is the include path.

This copy had sat at a 2021 snapshot since the initial commit until
2026-08-18, when it turned out to matter: the 2021 decoder accepted
ill-formed UTF-8 (bad continuation bytes, overlong encodings, surrogates,
values past U+10FFFF) and handed the garbage codepoint back as a success, so
`general_category()` read its block table out of range on it. Upstream fixed
both the decoder and every property-table lookup in `4a4d72c` (2025). Do not
roll this back on its own: the `len == 0` handling added to
`detail::for_each_letter_run` alongside this update only catches the *old*
decoder's truncated-input case (both revisions return 0 there); it does not catch bad
continuation bytes, overlong encodings, surrogates or out-of-range values,
which the old decoder reports as success with a garbage codepoint. Rolling
back this directory alone reopens the out-of-range read.
