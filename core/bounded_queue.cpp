#include "bounded_queue.hpp"

#include <algorithm>
#include <stdexcept>

namespace miniserve {

BoundedQueue::BoundedQueue(const std::size_t capacity) : capacity_(capacity) {
    if (capacity_ == 0) {
        throw std::invalid_argument("BoundedQueue capacity must be positive");
    }
}

bool BoundedQueue::try_push(const std::uint64_t request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.size() >= capacity_) {
        return false;
    }
    queue_.push_back(request_id);
    return true;
}

std::optional<std::uint64_t> BoundedQueue::try_pop() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.empty()) {
        return std::nullopt;
    }

    const auto request_id = queue_.front();
    queue_.pop_front();
    return request_id;
}

std::vector<std::uint64_t> BoundedQueue::pop_batch(const std::size_t max_items) {
    std::lock_guard<std::mutex> lock(mutex_);

    const auto count = std::min(max_items, queue_.size());
    std::vector<std::uint64_t> batch;
    batch.reserve(count);

    for (std::size_t i = 0; i < count; ++i) {
        batch.push_back(queue_.front());
        queue_.pop_front();
    }
    return batch;
}

std::size_t BoundedQueue::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
}

std::size_t BoundedQueue::capacity() const noexcept {
    return capacity_;
}

bool BoundedQueue::empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
}

bool BoundedQueue::full() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size() >= capacity_;
}

}  // namespace miniserve
