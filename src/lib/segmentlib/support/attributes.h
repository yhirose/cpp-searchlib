#pragma once

// Inlining hints the header-only inference path needs to keep the code
// generation it had when it was compiled.
//
// The scoring helpers used to live in an anonymous namespace inside a .cpp,
// where each had exactly one caller and internal linkage, so the compiler
// inlined the automaton traversal and its callback into the caller as a matter
// of course. As inline functions in a header they have external linkage and a
// standalone body has to exist, and the traversal, the callback and the
// per-match scratch access stop being folded together. That measured as a
// double-digit slowdown of KyTea segmentation, which this attribute recovers.
//
// It is a hint, not a requirement: a compiler that does not understand it
// simply optimizes a little less, and nothing about the meaning of the code
// changes.
#if defined(__GNUC__) || defined(__clang__)
#define SEGMENTLIB_FLATTEN [[gnu::flatten]]
#else
#define SEGMENTLIB_FLATTEN
#endif
