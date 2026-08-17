#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "segmentlib/support/expected.h"
#include "segmentlib/types.h"
#include "segmentlib/unicode/utf8.h"

namespace segmentlib::unicode {

// Version of the Unicode data the generated grapheme-break property table was
// built from, encoded as major*100 + minor (16.0 -> 1600). UAX #29 rules and
// property assignments can change between Unicode versions, so model files
// record the version they were trained with (design.ja.md 4.7 field 4b) and
// the loader warns when it differs from this segmenter's tables.
inline constexpr std::uint16_t kEgcUnicodeVersion = 1600;

// UAX #29 Grapheme_Cluster_Break property values (exactly the set the
// extended-grapheme-cluster rules consume). Values must match the packed
// table emitted by scripts/gen_egc_table.py.
enum class GraphemeBreak : std::uint8_t {
    Other = 0,
    CR,
    LF,
    Control,
    Extend,
    ZWJ,
    RegionalIndicator,
    Prepend,
    SpacingMark,
    L,    // Hangul leading consonant
    V,    // Hangul vowel
    T,    // Hangul trailing consonant
    LV,   // Hangul LV syllable
    LVT,  // Hangul LVT syllable
};

// Indic_Conjunct_Break property (UCD "InCB", introduced in Unicode 15.1),
// consumed by rule GB9c which keeps conjuncts like Devanagari क्षि together.
// Values must match the packed table emitted by scripts/gen_egc_table.py.
enum class IndicConjunctBreak : std::uint8_t {
    None = 0,
    Consonant,
    Extend,  // includes ZWJ and most InCB-relevant combining marks
    Linker,  // viramas that join two consonants into a conjunct
};

// The per-codepoint properties the EGC rules need, unpacked from the
// generated two-stage table.
struct GraphemeProps {
    GraphemeBreak gcb;
    IndicConjunctBreak incb;
    bool extended_pictographic;  // Extended_Pictographic, for GB11 (emoji ZWJ)
};

namespace detail {

#include "segmentlib/unicode/egc_table.inc"  // kEgcStage1, kEgcStage2, kEgcTableUnicodeVersion

// The generated table and the header constant must describe the same Unicode
// version; regenerate the table and bump kEgcUnicodeVersion together.
static_assert(kEgcTableUnicodeVersion == kEgcUnicodeVersion);

inline constexpr std::size_t kBlockSize = 256;

}  // namespace detail

// Looks up the grapheme properties of a codepoint. Codepoints outside the
// Unicode range (> U+10FFFF) get the default properties (Other/None/false);
// callers pass codepoints that came from utf8::decode, so this never happens
// in practice.
[[nodiscard]] inline GraphemeProps grapheme_props(char32_t cp) noexcept {
    std::uint8_t packed = 0;
    if (cp < 0x110000) {
        const std::size_t block = detail::kEgcStage1[cp >> 8];
        packed = detail::kEgcStage2[block * detail::kBlockSize + (cp & 0xFF)];
    }
    // Positional, not designated: designated initializers are C++20, and the
    // member order here is the declaration order of GraphemeProps above.
    return GraphemeProps{
        static_cast<GraphemeBreak>(packed & 0x0F),
        static_cast<IndicConjunctBreak>((packed >> 4) & 0x03),
        (packed & 0x40) != 0,
    };
}

// Incremental UAX #29 extended-grapheme-cluster boundary detector.
//
// Feed the codepoints of a text left to right; `next` reports whether an EGC
// boundary lies immediately *before* the codepoint just fed. The first call
// after construction (or reset) always reports a boundary (rule GB1, start of
// text); end of text is always a boundary too (GB2) and needs no call.
//
// The state is a few bytes (previous property + three small rule states), so
// a breaker can live on the stack of any loop that already decodes UTF-8,
// which is how egc_split_into avoids a second decoding pass.
class GraphemeBreaker {
public:
    // Feeds the next codepoint; returns true if an extended grapheme cluster
    // boundary precedes it.
    [[nodiscard]] bool next(char32_t cp) noexcept {
        const GraphemeProps props = grapheme_props(cp);
        const GraphemeBreak cur = props.gcb;

        bool is_break;
        if (at_start_) {
            is_break = true;  // GB1: break at start of text
        } else if (prev_ == GraphemeBreak::CR && cur == GraphemeBreak::LF) {
            is_break = false;  // GB3: keep CR LF together
        } else if (prev_ == GraphemeBreak::Control || prev_ == GraphemeBreak::CR ||
                   prev_ == GraphemeBreak::LF) {
            is_break = true;  // GB4: break after controls
        } else if (cur == GraphemeBreak::Control || cur == GraphemeBreak::CR ||
                   cur == GraphemeBreak::LF) {
            is_break = true;  // GB5: break before controls
        } else if (prev_ == GraphemeBreak::L &&
                   (cur == GraphemeBreak::L || cur == GraphemeBreak::V ||
                    cur == GraphemeBreak::LV || cur == GraphemeBreak::LVT)) {
            is_break = false;  // GB6: Hangul L x (L|V|LV|LVT)
        } else if ((prev_ == GraphemeBreak::LV || prev_ == GraphemeBreak::V) &&
                   (cur == GraphemeBreak::V || cur == GraphemeBreak::T)) {
            is_break = false;  // GB7: Hangul (LV|V) x (V|T)
        } else if ((prev_ == GraphemeBreak::LVT || prev_ == GraphemeBreak::T) &&
                   cur == GraphemeBreak::T) {
            is_break = false;  // GB8: Hangul (LVT|T) x T
        } else if (cur == GraphemeBreak::Extend || cur == GraphemeBreak::ZWJ) {
            is_break = false;  // GB9: keep Extend / ZWJ with what precedes
        } else if (cur == GraphemeBreak::SpacingMark) {
            is_break = false;  // GB9a: keep spacing marks attached
        } else if (prev_ == GraphemeBreak::Prepend) {
            is_break = false;  // GB9b: keep prepend characters attached
        } else if (gb9c_ == Gb9cState::ConsonantLinker &&
                   props.incb == IndicConjunctBreak::Consonant) {
            is_break = false;  // GB9c: Indic conjunct (consonant linker x consonant)
        } else if (gb11_ == Gb11State::PictographicZwj && props.extended_pictographic) {
            is_break = false;  // GB11: emoji ZWJ sequence
        } else if (prev_ == GraphemeBreak::RegionalIndicator &&
                   cur == GraphemeBreak::RegionalIndicator && (ri_run_ % 2) == 1) {
            is_break = false;  // GB12/GB13: join regional indicators pairwise
        } else {
            is_break = true;  // GB999: break everywhere else
        }

        // Update the multi-codepoint rule states with the codepoint just fed.

        // GB11 tracks ExtPict Extend* ZWJ; the Extend/ZWJ continuations only
        // count while a pictographic sequence is open.
        if (gb11_ == Gb11State::Pictographic && cur == GraphemeBreak::Extend) {
            // still Pictographic
        } else if (gb11_ == Gb11State::Pictographic && cur == GraphemeBreak::ZWJ) {
            gb11_ = Gb11State::PictographicZwj;
        } else if (props.extended_pictographic) {
            gb11_ = Gb11State::Pictographic;
        } else {
            gb11_ = Gb11State::None;
        }

        // GB9c tracks Consonant [Extend|Linker]* with at least one Linker.
        if (props.incb == IndicConjunctBreak::Consonant) {
            gb9c_ = Gb9cState::Consonant;
        } else if (gb9c_ != Gb9cState::None && props.incb == IndicConjunctBreak::Linker) {
            gb9c_ = Gb9cState::ConsonantLinker;
        } else if (gb9c_ != Gb9cState::None && props.incb == IndicConjunctBreak::Extend) {
            // sequence stays open in its current state
        } else {
            gb9c_ = Gb9cState::None;
        }

        ri_run_ = cur == GraphemeBreak::RegionalIndicator ? ri_run_ + 1 : 0;

        at_start_ = false;
        prev_ = cur;
        return is_break;
    }

    // Returns to the start-of-text state, so the breaker can be reused for
    // another text without reconstruction.
    void reset() noexcept { *this = GraphemeBreaker{}; }

private:
    // GB11: have we seen Extended_Pictographic Extend* (then ZWJ)?
    enum class Gb11State : std::uint8_t { None, Pictographic, PictographicZwj };
    // GB9c: have we seen InCB=Consonant [InCB=Extend|Linker]* (with a Linker)?
    enum class Gb9cState : std::uint8_t { None, Consonant, ConsonantLinker };

    bool at_start_ = true;
    GraphemeBreak prev_ = GraphemeBreak::Other;
    Gb11State gb11_ = Gb11State::None;
    Gb9cState gb9c_ = Gb9cState::None;
    // Length of the run of Regional_Indicator codepoints ending at the
    // previous position; GB12/13 join RI codepoints pairwise, so only the
    // parity matters, but the count is kept for clarity (it cannot overflow
    // meaningfully: parity is preserved mod 2).
    std::uint32_t ri_run_ = 0;
};

// Same as egc_split(), but fills a caller-owned offsets vector (its buffer is
// reused, avoiding per-call allocation on the hot path). On error the vector
// contents are unspecified.
[[nodiscard]] inline Expected<void, Error>
egc_split_into(std::string_view utf8, std::vector<std::size_t>& offsets) {
    offsets.clear();
    GraphemeBreaker breaker;
    std::size_t pos = 0;
    while (pos < utf8.size()) {
        const auto decoded = decode(utf8.substr(pos));
        if (!decoded) {
            return Unexpected(Error{ErrorCode::InvalidUtf8,
                                    "malformed UTF-8 in EGC segmentation"});
        }
        if (breaker.next(decoded->codepoint)) {
            offsets.push_back(pos);
        }
        pos += decoded->length;
    }
    offsets.push_back(utf8.size());  // GB2: end of text
    return {};
}

// Splits UTF-8 text into extended grapheme clusters (UAX #29).
//
// Returns the byte offsets of the cluster starts plus a final entry equal to
// utf8.size(), i.e. offsets.size() == egc_count + 1 and cluster i occupies
// bytes [offsets[i], offsets[i+1]). Empty input yields {0} (zero clusters).
// The constituent codepoints of a cluster can be re-decoded from its byte
// span with utf8::decode. Returns InvalidUtf8 on malformed input.
[[nodiscard]] inline Expected<std::vector<std::size_t>, Error>
egc_split(std::string_view utf8) {
    std::vector<std::size_t> offsets;
    if (auto r = egc_split_into(utf8, offsets); !r) {
        return Unexpected(r.error());
    }
    return offsets;
}

}  // namespace segmentlib::unicode
