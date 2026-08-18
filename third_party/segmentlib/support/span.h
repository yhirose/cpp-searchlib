#pragma once

#include <cstddef>
#include <iterator>
#include <type_traits>
#include <utility>

namespace segmentlib {

// A minimal stand-in for std::span, which is C++20 and so out of reach for the
// C++17 baseline this library targets (the point of that baseline is to be
// droppable into a C++17 consumer as headers alone).
//
// This is deliberately not a general reimplementation: it carries a pointer and
// a size, the only two things the code here ever asks of a span, and offers the
// subset of the interface that is actually used. There is no static extent —
// every span in this codebase is dynamically sized.
template <class T>
class Span {
public:
    using element_type = T;
    using value_type = std::remove_cv_t<T>;
    using pointer = T*;
    using reference = T&;
    using iterator = T*;
    using size_type = std::size_t;

    constexpr Span() noexcept = default;
    constexpr Span(T* data, std::size_t size) noexcept : data_(data), size_(size) {}

    // Converting constructor from any contiguous container (vector, array,
    // string, std::span itself). The `(*)[]` conversion test is the same one
    // std::span uses: it admits the qualification conversion T -> const T while
    // rejecting unrelated or derived-to-base element types, which a plain
    // is_convertible on the pointers would let through.
    //
    // This takes a forwarding reference so that a temporary view (the
    // std::span the still-C++23 training code hands to the inference API, for
    // instance) binds without a copy. As with std::span, a Span outlives
    // nothing: handing it an rvalue container that owns its storage dangles.
    template <class C,
              class = std::enable_if_t<
                  !std::is_same_v<std::remove_cv_t<std::remove_reference_t<C>>, Span> &&
                  std::is_convertible_v<
                      std::remove_pointer_t<decltype(std::declval<C&&>().data())> (*)[],
                      T (*)[]>>>
    constexpr Span(C&& c) noexcept : data_(c.data()), size_(c.size()) {}

    [[nodiscard]] constexpr T* data() const noexcept { return data_; }
    [[nodiscard]] constexpr std::size_t size() const noexcept { return size_; }
    [[nodiscard]] constexpr std::size_t size_bytes() const noexcept {
        return size_ * sizeof(T);
    }
    [[nodiscard]] constexpr bool empty() const noexcept { return size_ == 0; }

    [[nodiscard]] constexpr T& operator[](std::size_t i) const noexcept { return data_[i]; }

    [[nodiscard]] constexpr T* begin() const noexcept { return data_; }
    [[nodiscard]] constexpr T* end() const noexcept { return data_ + size_; }

private:
    T* data_ = nullptr;
    std::size_t size_ = 0;
};

// Reinterprets a span as the raw bytes behind it, the way std::as_bytes does.
// The binary reader and writer speak in bytes, so every model-parsing path
// funnels through this.
template <class T>
[[nodiscard]] constexpr Span<const std::byte> as_bytes(Span<T> s) noexcept {
    return Span<const std::byte>(reinterpret_cast<const std::byte*>(s.data()), s.size_bytes());
}

}  // namespace segmentlib
