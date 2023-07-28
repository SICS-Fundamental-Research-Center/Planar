#ifndef CORE_COMMON_MESSAGE_QUEUE_H_
#define CORE_COMMON_MESSAGE_QUEUE_H_

#include <queue>
#include <mutex>


namespace sics::graph::core::common {

// A simple queue type that will block `pop()` operation until some element is
// pushed into it.
template<typename T>
class BlockingQueue {
 public:
  BlockingQueue() = default;

  // Push() is a non-blocking operation.
  void Push(T t) {
    {
      std::lock_guard<std::mutex> grd(mtx_);
      q_.push(t);
    }
    cv_.notify_one();
  }

  // PopOrWait() will return immediately if the queue is not empty; otherwise,
  // it will block until some element is pushed into the queue.
  T PopOrWait() {
    std::unique_lock<std::mutex> lck(mtx_);
    while (q_.empty()) {
      cv_.wait(lck);
    }
    T t = q_.front();
    q_.pop();
    return t;
  }

  [[nodiscard]]
  size_t Size() {
    std::lock_guard<std::mutex> grd(mtx_);
    return q_.size();
  }

 private:
  std::queue<T> q_;

  std::mutex mtx_;

  std::condition_variable cv_;
};

}  // namespace sics::graph::core::common

#endif  // CORE_COMMON_MESSAGE_QUEUE_H_
