#ifndef CORE_COMMON_MESSAGE_QUEUE_H_
#define CORE_COMMON_MESSAGE_QUEUE_H_

#include <queue>
#include <mutex>


namespace sics::graph::core::common {

template<typename T>
class MessageQueue {
 public:
  MessageQueue() = default;

  void Push(T message) {
    {
      std::lock_guard<std::mutex> grd(mtx_);
      q_.push(message);
    }
    cv_.notify_one();
  }

  T PopOrWait() {
    std::unique_lock<std::mutex> lck(mtx_);
    while (q_.empty()) {
      cv_.wait(lck);
    }
    return q_.pop();
  }

 private:
  std::queue<T> q_;

  std::mutex mtx_;

  std::condition_variable cv_;
};


} // namespace sics::graph::core::common

#endif  // CORE_COMMON_MESSAGE_QUEUE_H_
