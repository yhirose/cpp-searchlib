//
//  searchlib.h
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <istream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace searchlib {

//-----------------------------------------------------------------------------
// Serialization primitives (shared by the index save/load implementations)
//-----------------------------------------------------------------------------

namespace detail {

// The on-disk "plain" format (format_type 0) is a host-endian dump of
// fixed-width fields, as designed in docs/embedded_minimal_roadmap.ja.md 9.1.
// Every integer is normalized to a fixed 64-bit (or 32-bit for the header
// tags) width so that a 32-bit and a 64-bit build agree on layout; endianness
// is left host-native, which is acceptable because a plain-format index is
// expected to be loaded on the same platform that wrote it. A future
// format_type can add an endian-neutral or compressed layout without
// disturbing this one.
inline constexpr char kIndexMagic[4] = {'S', 'I', 'D', 'X'};
inline constexpr uint32_t kFormatTypePlain = 0;
inline constexpr uint32_t kSchemaVersion = 2;

template <typename T> inline void write_scalar(std::ostream &os, T value) {
  static_assert(std::is_trivially_copyable_v<T>);
  os.write(reinterpret_cast<const char *>(&value), sizeof(T));
}

template <typename T> inline T read_scalar(std::istream &is) {
  static_assert(std::is_trivially_copyable_v<T>);
  T value{};
  is.read(reinterpret_cast<char *>(&value), sizeof(T));
  if (!is) {
    throw std::runtime_error("searchlib: unexpected end of index stream");
  }
  return value;
}

inline void write_u32string(std::ostream &os, const std::u32string &s) {
  write_scalar<uint64_t>(os, s.size());
  for (char32_t c : s) {
    write_scalar<uint32_t>(os, static_cast<uint32_t>(c));
  }
}

inline std::u32string read_u32string(std::istream &is) {
  auto n = read_scalar<uint64_t>(is);
  std::u32string s;
  s.reserve(static_cast<size_t>(n));
  for (uint64_t i = 0; i < n; i++) {
    s.push_back(static_cast<char32_t>(read_scalar<uint32_t>(is)));
  }
  return s;
}

} // namespace detail

//-----------------------------------------------------------------------------
// Interface
//-----------------------------------------------------------------------------

class IPostings {
public:
  virtual ~IPostings() = 0;

  virtual size_t size() const = 0;

  // document_id is scoped to the IInvertedIndex instance that produced this
  // IPostings; it is not a globally unique identifier across indexes. Code
  // that fans out across multiple indexes (e.g. a future federated search
  // layer) must tag results with their originating index rather than
  // assuming document_id alone is enough to disambiguate them.
  virtual size_t document_id(size_t index) const = 0;
  virtual size_t search_hit_count(size_t index) const = 0;

  virtual size_t term_position(size_t index, size_t search_hit_index) const = 0;
  virtual size_t term_length(size_t index, size_t search_hit_index) const = 0;
  virtual bool is_term_position(size_t index, size_t term_pos) const = 0;
};

class IInvertedIndex {
public:
  virtual ~IInvertedIndex() = 0;

  virtual size_t document_count() const = 0;

  // document_id here (and everywhere else in this interface) is local to
  // this IInvertedIndex instance; see the note on IPostings::document_id.
  virtual size_t document_term_count(size_t document_id) const = 0;
  virtual double average_document_term_count() const = 0;

  virtual bool term_exists(const std::u32string &str) const = 0;
  virtual size_t term_count(const std::u32string &str) const = 0;
  virtual size_t term_count(const std::u32string &str,
                            size_t document_id) const = 0;

  virtual size_t df(const std::u32string &str) const = 0;
  virtual double tf(const std::u32string &str, size_t document_id) const = 0;

  virtual std::shared_ptr<const IPostings>
  postings(const std::u32string &str) const = 0;

  // Logical (tombstone) deletion support. Read-only indexes report no
  // removals; searches filter out removed document_ids via these hooks.
  // Overridden by indexes that support IMutableInvertedIndex::remove_document.
  virtual bool has_removed_documents() const { return false; }
  virtual bool is_document_removed(size_t document_id) const { return false; }
};

using Normalizer = std::function<std::u32string(const std::u32string &str)>;

template <typename T>
using TextRangeList =
    std::unordered_map<size_t /*document_id*/, std::vector<T>>;

template <typename T>
using Tokenizer =
    std::function<void(Normalizer normalizer,
                       std::function<void(const std::u32string &str,
                                          size_t term_pos, T text_range)>
                           callback)>;

template <typename T> class ITextRange {
public:
  virtual ~ITextRange(){};

  virtual T text_range(const IPostings &positions, size_t index,
                       size_t search_hit_index) const = 0;
};

template <typename T>
class IInvertedIndexWithTextRange : public IInvertedIndex, ITextRange<T> {
public:
  virtual ~IInvertedIndexWithTextRange(){};
};

//-----------------------------------------------------------------------------
// Search
//-----------------------------------------------------------------------------

enum class Operation { Term, And, Adjacent, Or, Near, Not };

struct Expression {
  Operation operation;
  std::u32string term_str;
  size_t near_operation_distance;
  std::vector<Expression> nodes;
};

std::optional<Expression> parse_query(Normalizer normalizer,
                                      std::string_view query);

std::shared_ptr<IPostings> perform_search(const IInvertedIndex &invidx,
                                          const Expression &expr);

size_t term_count_score(const IInvertedIndex &invidx, const Expression &expr,
                        const IPostings &postings, size_t index);

double tf_idf_score(const IInvertedIndex &invidx, const Expression &expr,
                    const IPostings &postings, size_t index);

double bm25_score(const IInvertedIndex &invidx, const Expression &expr,
                  const IPostings &postings, size_t index, double k1 = 1.2,
                  double b = 0.75);

//-----------------------------------------------------------------------------
// Federated Search
//-----------------------------------------------------------------------------

// A mutable counterpart to IInvertedIndex. Kept as a separate interface
// (rather than discovered via dynamic_pointer_cast<IMutableInvertedIndex>)
// so that callers who already know an index is writable at registration
// time can pass that fact along explicitly, with no RTTI involved.
class IMutableInvertedIndex {
public:
  virtual ~IMutableInvertedIndex() = 0;

  // document_id is local to the corresponding IInvertedIndex; see the note
  // on IPostings::document_id.
  virtual void remove_document(size_t document_id) = 0;
};

// One entry in a FederatedIndex. mutable_index is null for read-only
// members (e.g. an installed book) and non-null for members that support
// removal (e.g. a notes index), decided by the caller at registration time.
struct FederationMember {
  std::shared_ptr<IInvertedIndex> index;
  std::shared_ptr<IMutableInvertedIndex> mutable_index;
};

// One hit produced by perform_federated_search. document ids are only
// meaningful together with `index`, since each member has its own local id
// space (see the note on IPostings::document_id).
struct FederatedHit {
  std::shared_ptr<IInvertedIndex> index;
  std::shared_ptr<IPostings> postings;
  size_t index_in_postings;
};

// A dynamic collection of independently-built IInvertedIndex instances that
// can be searched as one. Members are added/removed by shared_ptr identity;
// snapshot() returns a copy of the member list so that a search in
// progress is unaffected by concurrent add()/remove() calls (the
// underlying indexes are kept alive by the shared_ptr in the snapshot).
//
// This class only guards its own member list; it does not make any
// individual IInvertedIndex/IMutableInvertedIndex thread-safe on its own.
class FederatedIndex {
public:
  void add(std::shared_ptr<IInvertedIndex> index,
           std::shared_ptr<IMutableInvertedIndex> mutable_index = nullptr);
  void remove(const std::shared_ptr<IInvertedIndex> &index);

  std::vector<FederationMember> snapshot() const;

private:
  mutable std::mutex mutex_;
  std::vector<FederationMember> members_;
};

// Evaluates `expr` independently against every member of `federation`
// (each member resolves its own And/Or/Near/Not cursors internally, see
// perform_search) and concatenates the results, tagged with the
// originating member. No cross-member score normalization or sorting is
// performed; callers that need a combined ranking must do so themselves.
std::vector<FederatedHit> perform_federated_search(const FederatedIndex &federation,
                                                   const Expression &expr);

//-----------------------------------------------------------------------------
// Indexers
//-----------------------------------------------------------------------------

template <typename T> class IIndexer {
public:
  virtual ~IIndexer(){};

  // document_id is chosen by the caller and is only meaningful within the
  // IInvertedIndex this indexer writes to; see the note on
  // IPostings::document_id.
  virtual void index_document(size_t document_id, Tokenizer<T> tokenizer) = 0;
};

//-----------------------------------------------------------------------------
// Text Ranges
//-----------------------------------------------------------------------------

struct TextRange {
  size_t position;
  size_t length;
};

//-----------------------------------------------------------------------------
// Tokenizers
//-----------------------------------------------------------------------------

class UTF8PlainTextTokenizer {
public:
  explicit UTF8PlainTextTokenizer(std::string_view text);

  void operator()(Normalizer normalizer,
                  std::function<void(const std::u32string &str, size_t term_pos,
                                     TextRange text_range)>
                      callback);

private:
  std::string_view text_;
};

//-----------------------------------------------------------------------------

TextRange text_range(const TextRangeList<TextRange> &text_range_list,
                     const IPostings &positions, size_t index,
                     size_t search_hit_index);

class InMemoryInvertedIndexBase : public IInvertedIndex {
public:
  size_t document_count() const override;

  size_t document_term_count(size_t document_id) const override;
  double average_document_term_count() const override;

  bool term_exists(const std::u32string &str) const override;
  size_t term_count(const std::u32string &str) const override;
  size_t term_count(const std::u32string &str,
                    size_t document_id) const override;

  size_t df(const std::u32string &str) const override;
  double tf(const std::u32string &str, size_t document_id) const override;

  std::shared_ptr<const IPostings>
  postings(const std::u32string &str) const override;

  bool has_removed_documents() const override;
  bool is_document_removed(size_t document_id) const override;

  // Logical deletion: mark a document_id as removed. The postings and term
  // statistics are left intact (no physical compaction); searches exclude
  // removed document_ids at their output. Re-indexing the same document_id
  // via InMemoryIndexer clears the tombstone.
  void remove_document(size_t document_id);

  // Serialize/deserialize the T-independent part of the index (documents_
  // and term_dictionary_, including postings). The templated
  // InMemoryInvertedIndex<T> wraps this with the file header and the
  // T-dependent text-range section.
  void save(std::ostream &os) const;
  void load(std::istream &is);

  class Postings : public IPostings {
  public:
    size_t size() const override;

    size_t document_id(size_t index) const override;
    size_t search_hit_count(size_t index) const override;

    size_t term_position(size_t index, size_t search_hit_index) const override;
    size_t term_length(size_t index, size_t search_hit_index) const override;
    bool is_term_position(size_t index, size_t term_pos) const override;

    void add_term_position(size_t document_id, size_t term_pos);

    void save(std::ostream &os) const;
    void load(std::istream &is);

  private:
    // Kept sorted by document_id ascending so that document_id(index) is
    // O(1) and lookups by document_id can binary-search.
    using Entry =
        std::pair<size_t /*document_id*/, std::vector<size_t /*position*/>>;
    std::vector<Entry> positions_;
  };

  struct Document {
    size_t term_count;
  };

  struct Term {
    std::u32string str;
    size_t term_count;
    Postings postings;
  };

  std::unordered_map<size_t /*document_id*/, Document> documents_;
  std::unordered_map<std::u32string /*str*/, Term> term_dictionary_;
  std::unordered_set<size_t /*document_id*/> removed_document_ids_;
};

template <typename T>
class InMemoryInvertedIndex : public IInvertedIndexWithTextRange<T>,
                             public IMutableInvertedIndex {
public:
  size_t document_count() const override { return base_.document_count(); }

  size_t document_term_count(size_t document_id) const override {
    return base_.document_term_count(document_id);
  }

  double average_document_term_count() const override {
    return base_.average_document_term_count();
  }

  bool term_exists(const std::u32string &str) const override {
    return base_.term_exists(str);
  }

  size_t term_count(const std::u32string &str) const override {
    return base_.term_count(str);
  }

  size_t term_count(const std::u32string &str,
                    size_t document_id) const override {
    return base_.term_count(str, document_id);
  }

  size_t df(const std::u32string &str) const override { return base_.df(str); }

  double tf(const std::u32string &str, size_t document_id) const override {
    return base_.tf(str, document_id);
  }

  std::shared_ptr<const IPostings>
  postings(const std::u32string &str) const override {
    return base_.postings(str);
  }

  bool has_removed_documents() const override {
    return base_.has_removed_documents();
  }

  bool is_document_removed(size_t document_id) const override {
    return base_.is_document_removed(document_id);
  }

  void remove_document(size_t document_id) override {
    base_.remove_document(document_id);
  }

  T text_range(const IPostings &positions, size_t index,
               size_t search_hit_index) const override {
    return searchlib::text_range(text_range_list_, positions, index,
                                 search_hit_index);
  }

  // Callbacks for serializing the text-range value type T. When T is
  // trivially copyable (e.g. the built-in TextRange), these may be left
  // empty and a raw byte copy is used automatically; otherwise the caller
  // must supply both.
  using TextRangeSerializer = std::function<void(std::ostream &, const T &)>;
  using TextRangeDeserializer = std::function<T(std::istream &)>;

  void save(std::ostream &os,
            const TextRangeSerializer &serialize_value = {}) const {
    os.write(detail::kIndexMagic, sizeof(detail::kIndexMagic));
    detail::write_scalar<uint32_t>(os, detail::kFormatTypePlain);
    detail::write_scalar<uint32_t>(os, detail::kSchemaVersion);

    base_.save(os);

    // Text-range section, ordered by document_id for deterministic output.
    detail::write_scalar<uint64_t>(os, text_range_list_.size());
    std::vector<size_t> document_ids;
    document_ids.reserve(text_range_list_.size());
    for (const auto &[document_id, _] : text_range_list_) {
      document_ids.push_back(document_id);
    }
    std::sort(document_ids.begin(), document_ids.end());
    for (auto document_id : document_ids) {
      const auto &values = text_range_list_.at(document_id);
      detail::write_scalar<uint64_t>(os, document_id);
      detail::write_scalar<uint64_t>(os, values.size());
      for (const auto &value : values) {
        save_value_(os, value, serialize_value);
      }
    }
  }

  void load(std::istream &is,
            const TextRangeDeserializer &deserialize_value = {}) {
    char magic[sizeof(detail::kIndexMagic)];
    is.read(magic, sizeof(magic));
    if (!is || std::memcmp(magic, detail::kIndexMagic, sizeof(magic)) != 0) {
      throw std::runtime_error("searchlib: not a valid index file (bad magic)");
    }
    auto format_type = detail::read_scalar<uint32_t>(is);
    if (format_type != detail::kFormatTypePlain) {
      throw std::runtime_error("searchlib: unsupported index format_type");
    }
    auto schema_version = detail::read_scalar<uint32_t>(is);
    if (schema_version != detail::kSchemaVersion) {
      throw std::runtime_error("searchlib: unsupported index schema_version");
    }

    base_.load(is);

    text_range_list_.clear();
    auto document_count = detail::read_scalar<uint64_t>(is);
    for (uint64_t i = 0; i < document_count; i++) {
      auto document_id = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
      auto value_count = detail::read_scalar<uint64_t>(is);
      std::vector<T> values;
      values.reserve(static_cast<size_t>(value_count));
      for (uint64_t j = 0; j < value_count; j++) {
        values.push_back(load_value_(is, deserialize_value));
      }
      text_range_list_[document_id] = std::move(values);
    }
  }

  void save(const std::string &path,
            const TextRangeSerializer &serialize_value = {}) const {
    std::ofstream os(path, std::ios::binary);
    if (!os) {
      throw std::runtime_error(
          "searchlib: cannot open index file for writing: " + path);
    }
    save(os, serialize_value);
  }

  void load(const std::string &path,
            const TextRangeDeserializer &deserialize_value = {}) {
    std::ifstream is(path, std::ios::binary);
    if (!is) {
      throw std::runtime_error(
          "searchlib: cannot open index file for reading: " + path);
    }
    load(is, deserialize_value);
  }

private:
  template <typename> friend class InMemoryIndexer;

  static void save_value_(std::ostream &os, const T &value,
                          const TextRangeSerializer &serialize_value) {
    if (serialize_value) {
      serialize_value(os, value);
      return;
    }
    if constexpr (std::is_trivially_copyable_v<T>) {
      os.write(reinterpret_cast<const char *>(&value), sizeof(T));
    } else {
      throw std::runtime_error("searchlib: a text-range serializer is "
                               "required for this value type");
    }
  }

  static T load_value_(std::istream &is,
                       const TextRangeDeserializer &deserialize_value) {
    if (deserialize_value) {
      return deserialize_value(is);
    }
    if constexpr (std::is_trivially_copyable_v<T>) {
      T value{};
      is.read(reinterpret_cast<char *>(&value), sizeof(T));
      if (!is) {
        throw std::runtime_error("searchlib: unexpected end of index stream");
      }
      return value;
    } else {
      throw std::runtime_error("searchlib: a text-range deserializer is "
                               "required for this value type");
    }
  }

  InMemoryInvertedIndexBase base_;
  TextRangeList<T> text_range_list_;
};

template <typename T> class InMemoryIndexer : public IIndexer<T> {
public:
  InMemoryIndexer(InMemoryInvertedIndex<T> &index, Normalizer normalizer)
      : index_(index), normalizer_(normalizer) {}

  void index_document(size_t document_id, Tokenizer<T> tokenizer) override {
    // (Re)indexing a document clears any prior logical-deletion tombstone,
    // so that the "update = remove + re-index (same id)" pattern works.
    index_.base_.removed_document_ids_.erase(document_id);

    size_t term_count = 0;
    tokenizer(normalizer_, [&](const auto &str, auto term_pos,
                               auto text_range) {
      if (index_.base_.term_dictionary_.find(str) ==
          index_.base_.term_dictionary_.end()) {
        index_.base_.term_dictionary_[str] = {str, 0};
      }

      auto &term = index_.base_.term_dictionary_.at(str);
      term.term_count++;
      term.postings.add_term_position(document_id, term_pos);

      index_.text_range_list_[document_id].push_back(std::move(text_range));

      term_count++;
    });

    index_.base_.documents_[document_id] = {term_count};
  }

private:
  InMemoryInvertedIndex<T> &index_;
  Normalizer normalizer_;
};

} // namespace searchlib
