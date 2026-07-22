#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <vector>

namespace miniserve {

// A bounded, thread-safe FIFO for opaque request identifiers.
//
// Python owns request payloads and futures. The C++ queue stores only integer
// request IDs, so no Python reference-count operation occurs without the GIL.
// All operations are linearizable under one mutex. Admission and insertion are
// one atomic operation: try_push() either inserts the ID or reports full.
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity);

    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;
    BoundedQueue(BoundedQueue&&) = delete;
    BoundedQueue& operator=(BoundedQueue&&) = delete;

    [[nodiscard]] bool try_push(std::uint64_t request_id);
    [[nodiscard]] std::optional<std::uint64_t> try_pop();
    [[nodiscard]] std::vector<std::uint64_t> pop_batch(std::size_t max_items);

    [[nodiscard]] std::size_t size() const;
    [[nodiscard]] std::size_t capacity() const noexcept;
    [[nodiscard]] bool empty() const;
    [[nodiscard]] bool full() const;

private:
    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::deque<std::uint64_t> queue_;
};

}  // namespace miniserve
