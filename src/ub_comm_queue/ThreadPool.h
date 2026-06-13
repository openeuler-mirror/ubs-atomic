/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

 * rmrs is licensed under the Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *      http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

namespace ub_comm_queue {

class ThreadPool {
public:
    explicit ThreadPool(size_t threadCount);
    template <class F, class... Args>
    auto enqueue(F &&f, Args &&...args) -> std::future<std::invoke_result_t<F, Args...>>;

    ~ThreadPool();

private:
    void runWorkerLoop();
    bool waitForTask(std::unique_lock<std::mutex> &lock);
    bool tryDequeueTask(std::function<void()> &task);
    void executeDrainPhase();

    std::vector<std::thread> threads_;
    std::queue<std::function<void()>> taskQueue_;
    std::mutex mtx_;
    std::condition_variable taskAvailable_;
    std::atomic<bool> shuttingDown_;
};

inline ThreadPool::ThreadPool(size_t threadCount) : shuttingDown_(false)
{
    for (size_t i = 0; i < threadCount; ++i) {
        threads_.emplace_back(&ThreadPool::runWorkerLoop, this);
    }
}

inline bool ThreadPool::waitForTask(std::unique_lock<std::mutex> &lock)
{
    taskAvailable_.wait(lock, [this] { return shuttingDown_.load(std::memory_order_acquire) || !taskQueue_.empty(); });
    return !taskQueue_.empty();
}

inline bool ThreadPool::tryDequeueTask(std::function<void()> &task)
{
    std::unique_lock<std::mutex> lock(mtx_);
    if (taskQueue_.empty() && !waitForTask(lock)) {
        return false;
    }
    task = std::move(taskQueue_.front());
    taskQueue_.pop();
    return true;
}

inline void ThreadPool::executeDrainPhase()
{
    std::unique_lock<std::mutex> lock(mtx_);
    while (!taskQueue_.empty()) {
        auto task = std::move(taskQueue_.front());
        taskQueue_.pop();
        lock.unlock();
        task();
        lock.lock();
    }
}

inline void ThreadPool::runWorkerLoop()
{
    // Phase 1: Normal operation — process tasks until shutdown signal
    while (!shuttingDown_.load(std::memory_order_acquire)) {
        std::function<void()> task;
        if (!tryDequeueTask(task)) {
            break;
        }
        task();
    }

    // Phase 2: Drain — execute any tasks still queued after shutdown signal
    executeDrainPhase();
}

template <class F, class... Args>
auto ThreadPool::enqueue(F &&f, Args &&...args) -> std::future<std::invoke_result_t<F, Args...>>
{
    using return_type = std::invoke_result_t<F, Args...>;
    using task_type = std::packaged_task<return_type()>;

    auto task = std::make_shared<task_type>(
        [captured_f = std::forward<F>(f),
         captured_args = std::make_tuple(std::forward<Args>(args)...)]() mutable -> return_type {
            return std::apply(std::move(captured_f), std::move(captured_args));
        });

    std::future<return_type> result = task->get_future();
    {
        std::unique_lock<std::mutex> lock(mtx_);
        if (shuttingDown_.load(std::memory_order_acquire)) {
            throw std::runtime_error("pool is shutting down");
        }
        if (threads_.empty()) {
            throw std::runtime_error("no worker threads available");
        }
        taskQueue_.emplace([task]() { (*task)(); });
    }
    taskAvailable_.notify_one();
    return result;
}

inline ThreadPool::~ThreadPool()
{
    // Signal shutdown via atomic flag (no mutex needed for atomic store)
    shuttingDown_.store(true, std::memory_order_release);
    // Wake all blocked workers so they re-check the shutdown flag
    taskAvailable_.notify_all();
    // Wait for all workers to finish drain and exit
    for (std::thread &t : threads_) {
        t.join();
    }
}

} // namespace ub_comm_queue

#endif // THREAD_POOL_H