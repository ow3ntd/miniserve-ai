#include "bounded_queue.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

#define CHECK(condition)                                                        \
    do {                                                                        \
        if (!(condition)) {                                                     \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__     \
                      << ": " #condition << '\n';                              \
            return false;                                                       \
        }                                                                       \
    } while (false)

bool test_rejects_zero_capacity() {
    try {
        miniserve::BoundedQueue queue(0);
        (void)queue;
    } catch (const std::invalid_argument&) {
        return true;
    }
    return false;
}

bool test_capacity_and_fifo() {
    miniserve::BoundedQueue queue(2);

    CHECK(queue.capacity() == 2);
    CHECK(queue.empty());
    CHECK(queue.try_push(11));
    CHECK(queue.try_push(22));
    CHECK(!queue.try_push(33));
    CHECK(queue.full());
    CHECK(queue.size() == 2);

    const auto first = queue.try_pop();
    const auto second = queue.try_pop();
    const auto third = queue.try_pop();

    CHECK(first.has_value() && *first == 11);
    CHECK(second.has_value() && *second == 22);
    CHECK(!third.has_value());
    CHECK(queue.empty());
    return true;
}

bool test_pop_batch_is_bounded_and_fifo() {
    miniserve::BoundedQueue queue(8);
    for (std::uint64_t id = 1; id <= 6; ++id) {
        CHECK(queue.try_push(id));
    }

    CHECK(queue.pop_batch(0).empty());
    CHECK(queue.size() == 6);

    const auto first = queue.pop_batch(4);
    CHECK((first == std::vector<std::uint64_t>{1, 2, 3, 4}));
    CHECK(queue.size() == 2);

    const auto second = queue.pop_batch(99);
    CHECK((second == std::vector<std::uint64_t>{5, 6}));
    CHECK(queue.empty());
    return true;
}

bool test_multiple_producers_consumers_no_loss_or_duplicates() {
    constexpr std::size_t producer_count = 4;
    constexpr std::size_t consumer_count = 4;
    constexpr std::size_t items_per_producer = 5'000;
    constexpr std::size_t total_items = producer_count * items_per_producer;

    miniserve::BoundedQueue queue(128);
    std::atomic<bool> start{false};
    std::atomic<bool> failed{false};
    std::atomic<std::size_t> consumed{0};
    std::mutex output_mutex;
    std::vector<std::uint64_t> output;
    output.reserve(total_items);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);

    std::vector<std::thread> producers;
    for (std::size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            const std::uint64_t base = producer * items_per_producer;
            for (std::size_t offset = 0; offset < items_per_producer; ++offset) {
                const auto id = base + offset;
                while (!queue.try_push(id)) {
                    if (std::chrono::steady_clock::now() >= deadline) {
                        failed.store(true, std::memory_order_release);
                        return;
                    }
                    std::this_thread::yield();
                }
            }
        });
    }

    std::vector<std::thread> consumers;
    for (std::size_t consumer = 0; consumer < consumer_count; ++consumer) {
        consumers.emplace_back([&] {
            std::vector<std::uint64_t> local;
            local.reserve(total_items / consumer_count);

            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            while (consumed.load(std::memory_order_acquire) < total_items) {
                auto batch = queue.pop_batch(32);
                if (batch.empty()) {
                    if (std::chrono::steady_clock::now() >= deadline) {
                        failed.store(true, std::memory_order_release);
                        break;
                    }
                    std::this_thread::yield();
                    continue;
                }

                consumed.fetch_add(batch.size(), std::memory_order_acq_rel);
                local.insert(local.end(), batch.begin(), batch.end());
            }

            std::lock_guard<std::mutex> lock(output_mutex);
            output.insert(output.end(), local.begin(), local.end());
        });
    }

    start.store(true, std::memory_order_release);

    for (auto& thread : producers) {
        thread.join();
    }
    for (auto& thread : consumers) {
        thread.join();
    }

    CHECK(!failed.load(std::memory_order_acquire));
    CHECK(consumed.load(std::memory_order_acquire) == total_items);
    CHECK(output.size() == total_items);
    CHECK(queue.empty());

    std::vector<unsigned char> seen(total_items, 0);
    for (const auto id : output) {
        CHECK(id < total_items);
        CHECK(seen[id] == 0);
        seen[id] = 1;
    }
    for (const auto count : seen) {
        CHECK(count == 1);
    }
    return true;
}

}  // namespace

int main() {
    const struct {
        const char* name;
        bool (*run)();
    } tests[] = {
        {"rejects zero capacity", test_rejects_zero_capacity},
        {"capacity and FIFO", test_capacity_and_fifo},
        {"bounded FIFO batch pop", test_pop_batch_is_bounded_and_fifo},
        {"multi-producer/multi-consumer stress",
         test_multiple_producers_consumers_no_loss_or_duplicates},
    };

    for (const auto& test : tests) {
        try {
            if (!test.run()) {
                std::cerr << "FAILED: " << test.name << '\n';
                return 1;
            }
            std::cout << "PASS: " << test.name << '\n';
        } catch (const std::exception& exc) {
            std::cerr << "FAILED: " << test.name << " threw " << exc.what() << '\n';
            return 1;
        } catch (...) {
            std::cerr << "FAILED: " << test.name << " threw unknown exception\n";
            return 1;
        }
    }

    return 0;
}
