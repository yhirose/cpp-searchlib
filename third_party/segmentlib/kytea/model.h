#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "segmentlib/bytes/binary_reader.h"
#include "segmentlib/kytea/automaton.h"
#include "segmentlib/kytea/char_table.h"
#include "segmentlib/support/expected.h"
#include "segmentlib/support/span.h"
#include "segmentlib/types.h"

namespace segmentlib::kytea {

// Quantized feature-weight type used by KyTea's default (non-"NQ") models.
using FeatVal = std::int16_t;
using FeatVec = std::vector<FeatVal>;

// The subset of KyTea's training configuration that word segmentation needs.
// `num_tags`/`do_tags` are kept only to size and skip the model file's tag
// sections during parsing (this library does not retain or predict tags).
struct Config {
    std::uint8_t char_window = 0;  // characters on each side of a boundary
    std::uint8_t char_n = 0;       // max character n-gram order
    std::uint8_t type_window = 0;
    std::uint8_t type_n = 0;
    std::uint8_t dict_n = 0;  // dictionary match-length cap
    int num_tags = 0;
    bool do_ws = false;
    bool do_tags = false;
    int solver = 0;
};

// Per-dictionary-word data needed for the word-segmentation D features. The
// dictionary automaton carries this as its payload.
struct WordEntry {
    std::uint32_t char_length = 0;  // length of the word in characters
    std::uint8_t in_dict = 0;       // bitmask: which sub-dictionaries contain it
};

namespace detail {

using bytes::BinaryReader;
using bytes::ParseError;

// KyTea's KyteaChar is a 16-bit code unit; strings are length-prefixed arrays.
inline constexpr std::size_t kCharBytes = sizeof(CharId);

// Reads and discards a value (used to advance past fields we don't retain).
template <class T>
inline void discard(BinaryReader& r) {
    static_cast<void>(r.read<T>());
}
inline void discard_bool(BinaryReader& r) {
    static_cast<void>(r.read_bool());
}

inline FeatVec read_featvec(BinaryReader& r) {
    const auto n = r.read<std::uint32_t>();
    r.require_capacity(n, sizeof(FeatVal));
    FeatVec v(n);
    for (auto& x : v) {
        x = r.read<FeatVal>();
    }
    return v;
}

inline void skip_featvec(BinaryReader& r) {
    const auto n = r.read<std::uint32_t>();
    r.skip(std::size_t{n} * sizeof(FeatVal));
}

// Advances past a KyteaString (length-prefixed array of char ids) without
// materializing it. Tag candidate strings are stored this way.
inline void skip_kytea_string(BinaryReader& r) {
    const auto len = r.read<std::uint32_t>();
    r.skip(std::size_t{len} * kCharBytes);
}

// Advances past a Dictionary<Entry> without materializing it. Used for the
// selfDict (empty in WS models) and any dictionaries in skipped tag models.
inline void skip_dictionary(BinaryReader& r,
                            const std::function<void(BinaryReader&)>& skip_entry) {
    discard<std::uint8_t>(r);  // numDicts
    const auto n_states = r.read<std::uint32_t>();
    if (n_states == 0) {
        return;  // absent dictionary: no entries follow
    }
    for (std::uint32_t s = 0; s < n_states; ++s) {
        discard<std::uint32_t>(r);  // failure
        const auto n_gotos = r.read<std::uint32_t>();
        r.skip(std::size_t{n_gotos} * (kCharBytes + sizeof(std::uint32_t)));
        const auto n_out = r.read<std::uint32_t>();
        r.skip(std::size_t{n_out} * sizeof(std::uint32_t));
        discard<std::uint8_t>(r);  // isBranch
    }
    const auto n_entries = r.read<std::uint32_t>();
    for (std::uint32_t i = 0; i < n_entries; ++i) {
        skip_entry(r);
    }
}

// Advances past a FeatureLookup (the classifier attached to a KyteaModel).
// `active == 0` means no lookup and nothing further to skip.
inline void skip_feature_lookup(BinaryReader& r) {
    const auto active = r.read<std::uint8_t>();
    if (active == 0) {
        return;
    }
    skip_dictionary(r, skip_featvec);  // charDict
    skip_dictionary(r, skip_featvec);  // typeDict
    skip_dictionary(r, skip_featvec);  // selfDict
    skip_featvec(r);                   // dictVector (WS only)
    skip_featvec(r);                   // biases
    skip_featvec(r);                   // tagDictVector
    skip_featvec(r);                   // tagUnkVector
}

// Advances past a KyteaModel (classifier). A class count of 0 means "no
// model" and nothing further to skip.
inline void skip_tag_model(BinaryReader& r) {
    const auto num_classes = r.read<std::int32_t>();
    if (num_classes == 0) {
        return;
    }
    discard<std::uint8_t>(r);  // solver
    r.skip(std::size_t{static_cast<std::uint32_t>(num_classes)} * sizeof(std::int32_t));  // labels
    discard_bool(r);           // bias flag
    discard<double>(r);        // multiplier
    skip_feature_lookup(r);
}

// Reads one dictionary word entry, keeping only the D-feature fields; the
// per-word tag information (candidate strings, dictionary bitmasks, per-word
// tag models) is skipped.
inline WordEntry read_word_entry(BinaryReader& r, int num_tags) {
    WordEntry e;
    e.char_length = r.read<std::uint32_t>();          // word length in characters...
    r.skip(std::size_t{e.char_length} * kCharBytes);  // ...then the characters
    for (int lev = 0; lev < num_tags; ++lev) {
        const auto n_cands = r.read<std::uint32_t>();
        for (std::uint32_t j = 0; j < n_cands; ++j) {
            skip_kytea_string(r);      // tag candidate
            discard<std::uint8_t>(r);  // tagInDicts
        }
    }
    e.in_dict = r.read<std::uint8_t>();
    for (int lev = 0; lev < num_tags; ++lev) {
        skip_tag_model(r);  // per-word tag model (usually absent)
    }
    return e;
}

// Advances past one subword-dictionary entry (KyTea's ProbTagEntry).
inline void skip_prob_subword_entry(BinaryReader& r, int num_tags) {
    skip_kytea_string(r);  // word (key)
    for (int lev = 0; lev < num_tags; ++lev) {
        const auto n_cand = r.read<std::uint32_t>();
        for (std::uint32_t j = 0; j < n_cand; ++j) {
            skip_kytea_string(r);  // reading (raw char ids)
            discard<double>(r);    // probability
        }
    }
}

// Advances past one KyteaLM (a per-tag-level reading language model). `n ==
// 0` marks an absent model, with nothing further to skip.
inline void skip_lm(BinaryReader& r) {
    const auto n = r.read<std::uint32_t>();
    if (n == 0) {
        return;
    }
    discard<std::uint32_t>(r);  // vocabSize
    auto n_entries = r.read<std::uint32_t>();
    while (n_entries-- != 0) {
        const auto key_len = r.read<std::uint32_t>();
        r.skip(std::size_t{key_len} * kCharBytes);  // key
        discard<double>(r);                         // probability
        if (key_len != n) {
            discard<double>(r);  // fallback weight
        }
    }
}

struct Header {
    std::string version;
    char format = 0;
    std::string encoding;
};

inline Header parse_header(BinaryReader& r) {
    const std::string line = r.read_line();
    // Expected: "KyTea <version> <T|B> <encoding>"
    std::vector<std::string_view> tok;
    std::string_view sv{line};
    std::size_t pos = 0;
    while (pos < sv.size()) {
        const auto sp = sv.find(' ', pos);
        const auto end = (sp == std::string_view::npos) ? sv.size() : sp;
        if (end > pos) {
            tok.push_back(sv.substr(pos, end - pos));
        }
        pos = (sp == std::string_view::npos) ? sv.size() : sp + 1;
    }
    if (tok.size() != 4 || tok[0] != "KyTea" || tok[2].size() != 1) {
        throw ParseError("malformed model header");
    }
    return Header{std::string(tok[1]), tok[2][0], std::string(tok[3])};
}

}  // namespace detail

// A loaded KyTea model, holding exactly what word segmentation needs. Tag /
// reading / language-model sections of the file are parsed only far enough to
// advance past them (they are not retained). Immutable once loaded.
class Model {
public:
    static Expected<Model, Error> load_from_bytes(Span<const std::byte> data) {
        try {
            bytes::BinaryReader r(data);
            return Model(parse(r));
        } catch (const bytes::ParseError&) {
            return Unexpected(Error{ErrorCode::MalformedModel, "malformed KyTea model"});
        }
    }

    static Expected<Model, Error> load(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        if (!in) {
            return Unexpected(Error{ErrorCode::IoError, "could not open model file"});
        }
        std::vector<std::byte> buf;
        in.seekg(0, std::ios::end);
        const auto size = in.tellg();
        if (size < 0) {
            return Unexpected(Error{ErrorCode::IoError, "could not read model file"});
        }
        buf.resize(static_cast<std::size_t>(size));
        in.seekg(0, std::ios::beg);
        in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
        if (!in) {
            return Unexpected(Error{ErrorCode::IoError, "could not read model file"});
        }
        return load_from_bytes(buf);
    }

    [[nodiscard]] const Config& config() const noexcept { return parts_.config; }
    [[nodiscard]] const CharTable& chars() const noexcept { return parts_.chars; }
    [[nodiscard]] double multiplier() const noexcept { return parts_.multiplier; }

    [[nodiscard]] const Automaton<FeatVec>& char_dict() const noexcept { return parts_.char_dict; }
    [[nodiscard]] const Automaton<FeatVec>& type_dict() const noexcept { return parts_.type_dict; }
    [[nodiscard]] const FeatVec& dict_vector() const noexcept { return parts_.dict_vector; }
    [[nodiscard]] const FeatVec& biases() const noexcept { return parts_.biases; }
    [[nodiscard]] const Automaton<WordEntry>& word_dict() const noexcept { return parts_.word_dict; }
    [[nodiscard]] std::uint8_t num_dicts() const noexcept { return parts_.word_dict.num_dicts(); }

private:
    struct Parts {
        Config config;
        CharTable chars;
        double multiplier = 1.0;
        Automaton<FeatVec> char_dict;
        Automaton<FeatVec> type_dict;
        FeatVec dict_vector;
        FeatVec biases;
        Automaton<WordEntry> word_dict;
    };

    explicit Model(Parts&& parts) : parts_(std::move(parts)) {}

    static Parts parse(bytes::BinaryReader& r) {
        using namespace detail;

        const Header header = parse_header(r);
        if (header.format != 'B') {
            throw ParseError("only binary KyTea models are supported");
        }
        if (header.version != "0.4.0") {
            // "0.4.0NQ" (non-quantized, DISABLE_QUANTIZE builds) and other versions
            // are intentionally unsupported: distributed models are the quantized
            // "0.4.0" build, so we reject anything else rather than risk misreading.
            throw ParseError("unsupported KyTea model version");
        }
        if (header.encoding != "utf8") {
            throw ParseError("only utf8 KyTea models are supported");
        }

        Config cfg;
        cfg.do_ws = r.read_bool();
        cfg.do_tags = r.read_bool();
        cfg.num_tags = static_cast<int>(r.read<std::uint32_t>());
        cfg.char_window = r.read<std::uint8_t>();
        cfg.char_n = r.read<std::uint8_t>();
        cfg.type_window = r.read<std::uint8_t>();
        cfg.type_n = r.read<std::uint8_t>();
        cfg.dict_n = r.read<std::uint8_t>();
        discard_bool(r);     // bias flag (config-level; the model's own flag is used)
        discard<double>(r);  // epsilon
        cfg.solver = r.read<std::uint8_t>();

        CharTable chars(r.read_cstring());

        // --- wsModel (KyteaModel) ---
        const auto num_classes = r.read<std::int32_t>();
        if (num_classes < 2) {
            throw ParseError("model has no word-segmentation classifier");
        }
        discard<std::uint8_t>(r);  // solver
        r.skip(std::size_t{static_cast<std::uint32_t>(num_classes)} *
               sizeof(std::int32_t));  // labels
        discard_bool(r);               // bias flag
        const double multiplier = r.read<double>();

        // --- FeatureLookup (keep the WS parts, skip the tag parts) ---
        if (r.read<std::uint8_t>() == 0) {
            throw ParseError("word-segmentation model has no feature lookup");
        }
        Automaton<FeatVec> char_dict = Automaton<FeatVec>::read(r, read_featvec);
        Automaton<FeatVec> type_dict = Automaton<FeatVec>::read(r, read_featvec);
        skip_dictionary(r, skip_featvec);  // selfDict
        FeatVec dict_vector = read_featvec(r);
        FeatVec biases = read_featvec(r);
        skip_featvec(r);  // tagDictVector
        skip_featvec(r);  // tagUnkVector

        // --- global tag models: numTags x (word list + KyteaModel), skipped ---
        for (int i = 0; i < cfg.num_tags; ++i) {
            const auto n_words = r.read<std::uint32_t>();  // tag word list
            for (std::uint32_t j = 0; j < n_words; ++j) {
                skip_kytea_string(r);
            }
            skip_tag_model(r);
        }

        // --- word dictionary (needed for the D features) ---
        const int num_tags = cfg.num_tags;
        Automaton<WordEntry> word_dict =
            Automaton<WordEntry>::read(r, [num_tags](bytes::BinaryReader& rr) {
                return read_word_entry(rr, num_tags);
            });

        // --- subword dictionary (ProbTagEntry) + per-level reading LMs, skipped ---
        skip_dictionary(r, [num_tags](bytes::BinaryReader& rr) {
            skip_prob_subword_entry(rr, num_tags);
        });
        for (int i = 0; i < num_tags; ++i) {
            skip_lm(r);
        }

        // Positional, not designated: designated initializers are C++20. The
        // order is Parts' declaration order (config, chars, multiplier,
        // char_dict, type_dict, dict_vector, biases, word_dict).
        return Model::Parts{
            cfg,
            std::move(chars),
            multiplier,
            std::move(char_dict),
            std::move(type_dict),
            std::move(dict_vector),
            std::move(biases),
            std::move(word_dict),
        };
    }

    Parts parts_;
};

}  // namespace segmentlib::kytea
