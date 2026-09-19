#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "segmentlib/bytes/binary_reader.h"
#include "segmentlib/mlp/dictionary.h"
#include "segmentlib/mlp/precompute.h"
#include "segmentlib/mlp/vocab.h"
#include "segmentlib/support/expected.h"
#include "segmentlib/support/span.h"
#include "segmentlib/types.h"
#include "segmentlib/unicode/egc.h"

namespace segmentlib::mlp {

// The header-line signature used by backend auto-detection (design.ja.md
// 5.7/2節); the full first line is "SegmentLibMLP <version>\n".
inline constexpr std::string_view kModelSignature = "SegmentLibMLP ";

// A file format whose body is this network's layout: the header line to expect,
// and the diagnostics for a file that does not match it. The ED backend stores
// identically shaped parameters — EDLA trains this same network with a
// different learning rule (design.md 11) — so it reuses this whole loader
// behind its own signature. The messages travel with the format because
// Error::message is a non-owning view of a static string, so the loader cannot
// build one from the signature it was handed.
struct FormatId {
    std::string_view signature;
    std::string_view version;
    std::string_view wrong_signature;
    std::string_view wrong_version;
    std::string_view malformed;
};

inline constexpr FormatId kMlpFormat{
    kModelSignature,
    "1",
    "not a SegmentLibMLP model",
    "unsupported SegmentLibMLP version",
    "malformed SegmentLibMLP model",
};

namespace detail {

inline std::vector<std::int16_t> read_i16_tensor(bytes::BinaryReader& in,
                                                 std::size_t count) {
    in.require_capacity(count, sizeof(std::int16_t));
    std::vector<std::int16_t> tensor(count);
    for (std::int16_t& v : tensor) {
        v = in.read<std::int16_t>();
    }
    return tensor;
}

inline double read_scale(bytes::BinaryReader& in, const char* what) {
    const double s = in.read<double>();
    if (!(s > 0.0) || !std::isfinite(s)) {
        throw bytes::ParseError(std::string("invalid scale: ") + what);
    }
    return s;
}

}  // namespace detail

struct Config {  // 5.7 fields 1-4b
    std::uint8_t char_window = 0;      // w
    std::uint16_t embed_dim = 0;       // d
    std::uint16_t hidden = 0;          // H
    std::uint8_t num_dicts = 0;
    std::uint16_t unicode_version = 0;
};

// An immutable loaded MLP model: the raw
// quantized parameters from the file, plus the load-time derived structures —
// precompute table, dictionary matcher, dictionary columns and biases already
// converted to accumulator integer scale. All the
// scorer touches is integers; no scale multiplication survives into the hot
// path.
//
// `precision` picks the table/accumulator representation (5.6): Int16 (the
// default) is the requantized fast path — half the table memory, saturating
// int16 accumulation, decisions can differ from the file's exact semantics
// only at the |y|≈0 margin; Int32 is the exact reference path, bit-identical
// to the trainer's int16 reference forward. The file format is identical;
// this is purely a load-time choice.
class Model {
public:
    [[nodiscard]] static Expected<Model, Error> load_from_bytes(
        Span<const std::byte> data, TablePrecision precision = TablePrecision::Int16) {
        return load_format(data, kMlpFormat, precision);
    }

    [[nodiscard]] static Expected<Model, Error> load(
        const std::filesystem::path& path,
        TablePrecision precision = TablePrecision::Int16) {
        return load_format(path, kMlpFormat, precision);
    }

    // Loads a body in this network's layout from behind `format`'s header
    // line. The ED backend calls these with its own FormatId: the two formats
    // differ in their signature alone, so sharing the parser is what keeps
    // their numeric behaviour identical by construction rather than by
    // maintenance.
    [[nodiscard]] static Expected<Model, Error> load_format(
        Span<const std::byte> data, const FormatId& format, TablePrecision precision) {
        try {
            bytes::BinaryReader in(data);
            const std::string header = in.read_line();
            // Not starts_with: that is C++20, and this library targets C++17.
            if (header.compare(0, format.signature.size(), format.signature) != 0) {
                return Unexpected(
                    Error{ErrorCode::UnsupportedModelFormat, format.wrong_signature});
            }
            if (header.substr(format.signature.size()) != format.version) {
                return Unexpected(
                    Error{ErrorCode::UnsupportedModelFormat, format.wrong_version});
            }
            return Model(parse(in, precision));
        } catch (const bytes::ParseError&) {
            return Unexpected(Error{ErrorCode::MalformedModel, format.malformed});
        }
    }

    [[nodiscard]] static Expected<Model, Error> load_format(
        const std::filesystem::path& path, const FormatId& format,
        TablePrecision precision) {
        std::ifstream in(path, std::ios::binary);
        if (!in) {
            return Unexpected(Error{ErrorCode::IoError, "cannot open model file"});
        }
        std::vector<char> data((std::istreambuf_iterator<char>(in)),
                               std::istreambuf_iterator<char>());
        if (in.bad()) {
            return Unexpected(Error{ErrorCode::IoError, "cannot read model file"});
        }
        return load_format(as_bytes(Span<const char>(data)), format, precision);
    }

    [[nodiscard]] TablePrecision table_precision() const noexcept {
        return parts_.precompute.precision();
    }

    [[nodiscard]] const Config& config() const noexcept { return parts_.config; }
    [[nodiscard]] const Vocab& vocab() const noexcept { return parts_.vocab; }
    [[nodiscard]] const PrecomputeTable& precompute() const noexcept {
        return parts_.precompute;
    }
    [[nodiscard]] const DictMatcher& dict() const noexcept { return parts_.dict; }

    // Raw quantized layers (5.7 fields 11/12/15).
    [[nodiscard]] Span<const std::int16_t> embedding() const noexcept {
        return parts_.embedding;
    }
    [[nodiscard]] Span<const std::int16_t> w1() const noexcept {
        return parts_.w1;
    }
    [[nodiscard]] Span<const std::int16_t> w2() const noexcept {
        return parts_.w2;
    }

    // Biases in accumulator integer scale (converted at load, 5.7 fields
    // 14/16; b2_q is int64 — the output dot product runs in int64, I.1-(5)).
    [[nodiscard]] Span<const std::int32_t> b1_q() const noexcept {
        return parts_.b1_q;
    }
    [[nodiscard]] std::int64_t b2_q() const noexcept { return parts_.b2_q; }

    // Column k of W_dict in accumulator scale (I.1-(3)): H int32 values to
    // add when dictionary feature k is active.
    [[nodiscard]] Span<const std::int32_t> dict_col(std::uint32_t k) const noexcept {
        return Span<const std::int32_t>(
            parts_.dict_cols.data() + static_cast<std::size_t>(k) * parts_.config.hidden,
            parts_.config.hidden);
    }

    // Int16-mode counterparts: everything requantized by requant_i16 (b2 by
    // the same rounded shift in int64), valid when table_precision() is Int16.
    [[nodiscard]] Span<const std::int16_t> b1_q16() const noexcept {
        return parts_.b1_q16;
    }
    [[nodiscard]] std::int64_t b2_q16() const noexcept { return parts_.b2_q16; }
    [[nodiscard]] Span<const std::int16_t> dict_col16(std::uint32_t k) const noexcept {
        return Span<const std::int16_t>(
            parts_.dict_cols16.data() +
                static_cast<std::size_t>(k) * parts_.config.hidden,
            parts_.config.hidden);
    }

    // True when the model was trained with different Unicode data than this
    // build's EGC splitter (5.7 field 4b); the loader also warns on stderr.
    [[nodiscard]] bool unicode_version_mismatch() const noexcept {
        return parts_.unicode_mismatch;
    }

private:
    struct Parts {
        Config config;
        Vocab vocab;
        std::vector<std::int16_t> embedding;  // V × d
        std::vector<std::int16_t> w1;         // H × 2w·d
        std::vector<std::int16_t> w2;         // H
        std::vector<std::int32_t> b1_q;       // H
        std::int64_t b2_q = 0;
        std::vector<std::int32_t> dict_cols;  // Fd × H, column-contiguous
        // Int16-mode requantizations of the three above (empty/0 otherwise).
        std::vector<std::int16_t> b1_q16;
        std::int64_t b2_q16 = 0;
        std::vector<std::int16_t> dict_cols16;
        PrecomputeTable precompute;
        DictMatcher dict;
        bool unicode_mismatch = false;
    };
    static Parts parse(bytes::BinaryReader& in, TablePrecision precision) {
        using bytes::ParseError;

        Parts parts;

        // Config (fields 1-4b).
        Config& c = parts.config;
        c.char_window = in.read<std::uint8_t>();
        c.embed_dim = in.read<std::uint16_t>();
        c.hidden = in.read<std::uint16_t>();
        c.num_dicts = in.read<std::uint8_t>();
        c.unicode_version = in.read<std::uint16_t>();
        if (c.char_window < 1 || c.embed_dim < 1 || c.embed_dim > 256 || c.hidden < 1) {
            throw ParseError("implausible model configuration");
        }
        if (c.unicode_version != unicode::kEgcUnicodeVersion) {
            parts.unicode_mismatch = true;
            std::fprintf(stderr,
                         "segmentlib: MLP model was trained with Unicode %u.%u EGC "
                         "rules but this build uses %u.%u; segmentation of "
                         "clusters affected by the difference may diverge\n",
                         c.unicode_version / 100, c.unicode_version % 100,
                         unicode::kEgcUnicodeVersion / 100,
                         unicode::kEgcUnicodeVersion % 100);
        }

        // Scales (fields 5-8b).
        const double emb_scale = detail::read_scale(in, "emb_scale");
        const double w1_scale = detail::read_scale(in, "w1_scale");
        const double wdict_scale =
            c.num_dicts > 0 ? detail::read_scale(in, "wdict_scale") : 1.0;
        const double w2_scale = detail::read_scale(in, "w2_scale");
        const double acc_scale = detail::read_scale(in, "acc_scale");

        // Vocabulary (fields 9-10).
        const std::uint32_t vocab_size = in.read<std::uint32_t>();
        if (vocab_size < 2) {
            throw ParseError("vocabulary smaller than the reserved rows");
        }
        in.require_capacity(vocab_size - 2, sizeof(std::uint32_t));
        std::vector<char32_t> codepoints(vocab_size - 2);
        char32_t prev = 0;
        for (std::size_t i = 0; i < codepoints.size(); ++i) {
            const auto cp = static_cast<char32_t>(in.read<std::uint32_t>());
            if (cp > 0x10FFFF || (i > 0 && cp <= prev)) {
                throw ParseError("vocabulary codepoints not strictly ascending");
            }
            prev = cp;
            codepoints[i] = cp;
        }
        parts.vocab = Vocab{std::move(codepoints)};

        // Layers (fields 11-16).
        const std::size_t d = c.embed_dim;
        const std::size_t h = c.hidden;
        const std::size_t in_dim = 2u * c.char_window * d;
        const std::size_t fd =
            static_cast<std::size_t>(c.num_dicts) * kDictFeaturesPerDict;
        parts.embedding = detail::read_i16_tensor(in, vocab_size * d);
        parts.w1 = detail::read_i16_tensor(in, h * in_dim);
        const std::vector<std::int16_t> wdict =
            c.num_dicts > 0 ? detail::read_i16_tensor(in, h * fd)
                            : std::vector<std::int16_t>{};
        in.require_capacity(h, sizeof(double));
        std::vector<double> b1(h);
        for (double& b : b1) {
            b = in.read<double>();
        }
        parts.w2 = detail::read_i16_tensor(in, h);
        const double b2 = in.read<double>();

        // Dictionaries (field 17): the compiled matcher, ready to use.
        CompiledDictionaries compiled;
        if (c.num_dicts > 0) {
            compiled = read_compiled_dictionaries(in, c.num_dicts);
        }
        if (!in.eof()) {
            throw ParseError("trailing bytes after the model");
        }
        parts.dict = DictMatcher(std::move(compiled));
        if (!parts.dict.valid()) {
            throw ParseError("dictionary FST is not usable");
        }

        // Load-time derived structures (I.4): everything converted to the
        // accumulator integer scale once, here.
        const double r = emb_scale * w1_scale / acc_scale;
        parts.precompute = PrecomputeTable(parts.embedding, parts.w1, vocab_size,
                                           c.embed_dim, c.hidden, c.char_window, r,
                                           precision);
        parts.dict_cols.resize(fd * h);
        const double dict_r = wdict_scale / acc_scale;
        for (std::size_t k = 0; k < fd; ++k) {
            for (std::size_t j = 0; j < h; ++j) {
                parts.dict_cols[k * h + j] =
                    static_cast<std::int32_t>(std::llround(dict_r * wdict[j * fd + k]));
            }
        }
        parts.b1_q.resize(h);
        for (std::size_t j = 0; j < h; ++j) {
            parts.b1_q[j] = static_cast<std::int32_t>(std::llround(b1[j] / acc_scale));
        }
        parts.b2_q = std::llround(b2 / (w2_scale * acc_scale));
        if (precision == TablePrecision::Int16) {
            // The int16-mode derivations: the same requant_i16 the table uses,
            // applied to the int32 quantities above; b2 gets the identical
            // rounded shift, in int64 (it is the output-sum offset, never an
            // int16 accumulator entry, so no saturation applies).
            parts.b1_q16.resize(h);
            for (std::size_t j = 0; j < h; ++j) {
                parts.b1_q16[j] = requant_i16(parts.b1_q[j]);
            }
            parts.dict_cols16.resize(fd * h);
            for (std::size_t i = 0; i < fd * h; ++i) {
                parts.dict_cols16[i] = requant_i16(parts.dict_cols[i]);
            }
            parts.b2_q16 = (parts.b2_q + (1 << (kAccShift - 1))) >> kAccShift;
        }
        return parts;
    }

    explicit Model(Parts parts) noexcept : parts_(std::move(parts)) {}
    Parts parts_;
};

}  // namespace segmentlib::mlp
