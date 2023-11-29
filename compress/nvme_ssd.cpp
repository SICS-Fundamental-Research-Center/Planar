#include <gflags/gflags.h>

#include <fstream>
#include <vector>

#include "core/common/multithreading/task.h"
#include "core/common/multithreading/thread_pool.h"
#include "util/logging.h"

DEFINE_string(i, "0.bin", "compress file name");
DEFINE_uint32(p, 8, "thread pool size");
DEFINE_string(type, "normal", "normal read time mode");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto file_path = FLAGS_i;
  auto parallelism = FLAGS_p;
  auto mode = FLAGS_type;

  if (mode == "normal") {
    auto time_b = std::chrono::system_clock::now();
    // read src data
    std::ifstream src_file(file_path, std::ios::binary);

    src_file.seekg(0, std::ios::end);
    size_t file_size = src_file.tellg();
    src_file.seekg(0, std::ios::beg);
    auto src_data = (char*)malloc(file_size);
    src_file.read(src_data, file_size);

    auto time_e = std::chrono::system_clock::now();
    LOGF_INFO("time used for normal read: {}",
              std::chrono::duration<double>(time_e - time_b).count());
    free(src_data);
  } else {
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);
    std::atomic<size_t> pending_packages(parallelism);

    auto thread_pool = xyz::graph::core::common::ThreadPool(parallelism);

    // read src data size
    std::ifstream src_file(file_path, std::ios::binary);
    src_file.seekg(0, std::ios::end);
    size_t file_size = src_file.tellg();
    src_file.seekg(0, std::ios::beg);

    auto buffer = (char*)malloc(file_size);

    std::vector<size_t> offsets(parallelism);
    std::vector<size_t> read_sizes(parallelism);
    size_t step = ceil((double)file_size / parallelism);
    for (int i = 0; i < parallelism; i++) {
      offsets[i] = i * step;
      read_sizes[i] = step;
      if (i == parallelism - 1) {
        read_sizes[i] = file_size - i * step;
      }
    }

    auto time_begin = std::chrono::system_clock::now();
    for (int i = 0; i < parallelism; i++) {
      auto offset = offsets[i];
      auto read_size = read_sizes[i];
      thread_pool.SubmitSync([i, buffer, offset, read_size, file_path,
                              &pending_packages, &finish_cv]() {
        std::ifstream src_file(file_path, std::ios::binary);
        src_file.seekg(offset, std::ios::beg);
        src_file.read(buffer + offset, read_size);
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
    auto time_end = std::chrono::system_clock::now();

    LOGF_INFO("time used for normal read: {}",
              std::chrono::duration<double>(time_end - time_begin).count());
  }
}