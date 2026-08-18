#pragma once

#include <optional>
#include <type_traits>
#include <utility>
#include <variant>

namespace segmentlib {

// A minimal stand-in for std::expected, which is C++23 and so out of reach for
// the C++17 baseline this library targets.
//
// The interface is deliberately the subset this codebase uses: construct from a
// value or from Unexpected, test with operator bool, read the value with
// operator* / operator->, read the failure with error(). None of the monadic
// operations (and_then, transform, or_else) appear anywhere here, so none are
// provided, and neither is value(): it would be a second spelling of operator*
// with different behaviour on failure in the standard, which is worse than not
// having it. Adding any of them later is easy; guessing at their semantics now
// is not.

// The tag type that turns a bare error into "this is the failure branch",
// mirroring std::unexpected. Everything that fails does so by returning
// Unexpected(some_error), which the converting constructor below picks up.
template <class E>
class Unexpected {
public:
    explicit constexpr Unexpected(E error) : error_(std::move(error)) {}

    [[nodiscard]] constexpr const E& error() const& noexcept { return error_; }
    [[nodiscard]] constexpr E&& error() && noexcept { return std::move(error_); }

private:
    E error_;
};

template <class E>
Unexpected(E) -> Unexpected<E>;

template <class T, class E>
class Expected {
public:
    using value_type = T;
    using error_type = E;

    // Default-constructs the value branch, which is what makes
    // `std::vector<Expected<T, E>> out(n)` work; the batch tokenizer fills a
    // result vector that way before the workers overwrite each slot.
    constexpr Expected() : storage_(std::in_place_index<0>) {}

    constexpr Expected(T value) : storage_(std::in_place_index<0>, std::move(value)) {}
    constexpr Expected(Unexpected<E> error)
        : storage_(std::in_place_index<1>, std::move(error).error()) {}

    [[nodiscard]] constexpr bool has_value() const noexcept {
        return storage_.index() == 0;
    }
    [[nodiscard]] explicit constexpr operator bool() const noexcept { return has_value(); }

    [[nodiscard]] constexpr T& operator*() & noexcept { return std::get<0>(storage_); }
    [[nodiscard]] constexpr const T& operator*() const& noexcept {
        return std::get<0>(storage_);
    }
    [[nodiscard]] constexpr T&& operator*() && noexcept {
        return std::move(std::get<0>(storage_));
    }

    [[nodiscard]] constexpr T* operator->() noexcept { return &std::get<0>(storage_); }
    [[nodiscard]] constexpr const T* operator->() const noexcept {
        return &std::get<0>(storage_);
    }

    [[nodiscard]] constexpr const E& error() const& noexcept {
        return std::get<1>(storage_);
    }

private:
    std::variant<T, E> storage_;
};

// The void branch has no value to hold, so it is a plain "error or nothing"
// rather than a variant. Kept as a partial specialization instead of an
// if constexpr inside the primary template because the two shapes genuinely
// differ: there is no operator-> and no value to dereference.
template <class E>
class Expected<void, E> {
public:
    using value_type = void;
    using error_type = E;

    constexpr Expected() noexcept = default;
    constexpr Expected(Unexpected<E> error) : error_(std::move(error).error()) {}

    [[nodiscard]] constexpr bool has_value() const noexcept { return !error_.has_value(); }
    [[nodiscard]] explicit constexpr operator bool() const noexcept { return has_value(); }

    constexpr void operator*() const noexcept {}

    [[nodiscard]] constexpr const E& error() const& noexcept { return *error_; }

private:
    std::optional<E> error_;
};

}  // namespace segmentlib
