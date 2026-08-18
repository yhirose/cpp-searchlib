#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "segmentlib/kytea/kytea_backend.h"
#include "segmentlib/kytea/model.h"
#include "segmentlib/mlp/mlp_backend.h"
#include "segmentlib/mlp/model.h"
#include "segmentlib/support/expected.h"
#include "segmentlib/support/span.h"
#include "segmentlib/types.h"

namespace segmentlib {

namespace detail {

// Runs `fn` over every text, parallelized across `threads` (0 = hardware
// concurrency), returning results in input order. Workers claim contiguous
// chunks via an atomic cursor (dynamic scheduling balances the varying line
// lengths; the chunk amortizes the atomic). Each thread writes disjoint result
// slots, and the per-item functions are re-entrant (the model is immutable, its
// scratch buffers are thread_local), so no locking is needed.
template <class Result, class Fn>
std::vector<Expected<Result, Error>> run_batch(Span<const std::string_view> texts,
                                               unsigned threads, Fn fn) {
    std::vector<Expected<Result, Error>> out(texts.size());
    if (texts.empty()) {
        return out;
    }

    unsigned n = threads != 0 ? threads : std::thread::hardware_concurrency();
    if (n == 0) {
        n = 1;
    }
    n = static_cast<unsigned>(std::min<std::size_t>(n, texts.size()));

    if (n == 1) {
        for (std::size_t i = 0; i < texts.size(); ++i) {
            out[i] = fn(texts[i]);
        }
        return out;
    }

    std::atomic<std::size_t> cursor{0};
    constexpr std::size_t kChunk = 64;
    const auto worker = [&] {
        for (;;) {
            const std::size_t start = cursor.fetch_add(kChunk, std::memory_order_relaxed);
            if (start >= texts.size()) {
                break;
            }
            const std::size_t end = std::min(start + kChunk, texts.size());
            for (std::size_t i = start; i < end; ++i) {
                out[i] = fn(texts[i]);
            }
        }
    };

    std::vector<std::thread> pool;
    pool.reserve(n - 1);
    for (unsigned t = 0; t + 1 < n; ++t) {
        pool.emplace_back(worker);
    }
    worker();  // the calling thread participates too
    for (auto& th : pool) {
        th.join();
    }
    return out;
}

}  // namespace detail

// The public segmentation API. A Segmenter owns one backend and dispatches to
// it; the backend type is hidden from callers. KyTea-compatible and MLP
// backends exist today; Vaporetto will join the variant later.
//
// Thread safety: a loaded Segmenter is immutable, and the per-call scratch
// buffers are thread_local, so any number of threads may call tokenize() or
// tokenize_all() concurrently on the same const Segmenter. That is the intended
// way to share one: load once, then hand out `const Segmenter&`.
class Segmenter {
public:
    // Loads a model, auto-detecting its format: a "SegmentLibMLP " header
    // line selects the MLP backend (design.ja.md 4.7), anything else is
    // treated as a KyTea binary model.
    static Expected<Segmenter, Error> load(const std::filesystem::path& model_path) {
        // Auto-detection by the file's leading bytes: the MLP format opens with
        // its ASCII signature line (design.ja.md 4.7); KyTea models start with
        // "KyTea " and anything unrecognized falls through to the KyTea loader's
        // own diagnostics. (Vaporetto's zstd magic joins here later.)
        std::ifstream in(model_path, std::ios::binary);
        if (!in) {
            return Unexpected(Error{ErrorCode::IoError, "cannot open model file"});
        }
        std::string head(mlp::kModelSignature.size(), '\0');
        in.read(head.data(), static_cast<std::streamsize>(head.size()));
        if (in.gcount() == static_cast<std::streamsize>(head.size()) &&
            head == mlp::kModelSignature) {
            in.close();
            return load_mlp(model_path);
        }
        in.close();
        return load_kytea(model_path);
    }

    // Loads a model, forcing the KyTea backend.
    static Expected<Segmenter, Error> load_kytea(const std::filesystem::path& model_path) {
        auto model = kytea::Model::load(model_path);
        if (!model) {
            return Unexpected(model.error());
        }
        return Segmenter(kytea::KyteaBackend(std::move(*model)));
    }

    // Loads a model, forcing the MLP backend.
    static Expected<Segmenter, Error> load_mlp(const std::filesystem::path& model_path) {
        auto model = mlp::Model::load(model_path);
        if (!model) {
            return Unexpected(model.error());
        }
        return Segmenter(mlp::MlpBackend(std::move(*model)));
    }

    // Move-only. A Segmenter owns its model outright — 128 MB for the
    // distributed KyTea model — so an accidental copy (passing by value, or
    // `auto b = a;`) would duplicate all of it: measured at 10 ms and ~400 MB,
    // with no diagnostic and no visible symptom beyond the memory. Since the
    // loaded model is immutable and safe to use concurrently, sharing one
    // wants a `const Segmenter&`, or a shared_ptr where ownership must be
    // shared; both say so at the use site, which a silent deep copy does not.
    Segmenter(const Segmenter&) = delete;
    Segmenter& operator=(const Segmenter&) = delete;
    Segmenter(Segmenter&&) = default;
    Segmenter& operator=(Segmenter&&) = default;

    [[nodiscard]] Expected<Segments, Error> tokenize(std::string_view text) const {
        return std::visit(
            [&](const auto& b) -> Expected<Segments, Error> { return b.tokenize(text); },
            backend_);
    }

    // Tokenizes many inputs in parallel; result[i] corresponds to texts[i].
    // Inputs are independent and the model is immutable, so throughput scales
    // near-linearly with cores. `threads == 0` uses hardware_concurrency().
    [[nodiscard]] std::vector<Expected<Segments, Error>> tokenize_all(
        Span<const std::string_view> texts, unsigned threads = 0) const {
        return detail::run_batch<Segments>(
            texts, threads, [this](std::string_view t) { return tokenize(t); });
    }

private:
    using AnyBackend = std::variant<kytea::KyteaBackend, mlp::MlpBackend>;

    explicit Segmenter(AnyBackend backend) noexcept : backend_(std::move(backend)) {}

    AnyBackend backend_;
};

}  // namespace segmentlib
