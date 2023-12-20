#include <gflags/gflags.h>
#include <lz4.h>
#include <lz4hc.h>
#include <stdio.h>
#include <stdlib.h>

#include <boost/chrono.hpp>
#include <boost/chrono/thread_clock.hpp>
#include <boost/thread.hpp>
#include <fstream>

#include "core/common/multithreading/task.h"
#include "core/common/multithreading/thread_pool.h"
#include "util/logging.h"

#define LZ4_MEMORY_USAGE 20

DEFINE_uint32(type, 0, "normal read time mode");
DEFINE_string(dir, "/Users/liuyang/sics/refactor/graph-systems/testfile/",
              "compress directory");
DEFINE_string(file, "0.bin", "compress file name");
DEFINE_uint32(p, 8, "thread pool size");
DEFINE_uint32(block_size, 4096, "block size");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto dir = FLAGS_dir;
  auto file = FLAGS_file;
  auto parallelism = FLAGS_p;
  auto block_size = FLAGS_block_size;
  // compress first
  std::string src_path = dir + file;
  std::string path = dir + "compress/";

  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(parallelism);

  auto thread_pool = xyz::graph::core::common::ThreadPool(parallelism);

  auto time_b = std::chrono::system_clock::now();
  // read src data
  std::ifstream src_file(src_path, std::ios::binary);
  if (!src_file) {
    LOG_FATAL("Error opening bin file: ", src_path.c_str());
  }

  src_file.seekg(0, std::ios::end);
  size_t file_size = src_file.tellg();
  src_file.seekg(0, std::ios::beg);
  auto src_data = (char*)malloc(file_size);
  src_file.read(src_data, file_size);

  auto time_e = std::chrono::system_clock::now();

  LOGF_INFO("time used for normal read: {}",
            std::chrono::duration<double>(time_e - time_b).count());

  std::vector<char*> src_data_address(parallelism);
  std::vector<size_t> src_data_size(parallelism);
  std::vector<size_t> dst_data_size(parallelism);
  size_t step = ceil((double)file_size / parallelism);
  for (int i = 0; i < parallelism; i++) {
    src_data_address[i] = src_data + i * step;
    src_data_size[i] = step;
    if (i == parallelism - 1) {
      src_data_size[i] = file_size - i * step;
    }
  }

  // num_block
  size_t num_block = ceil((double)file_size / block_size);
  LOGF_INFO("num block: {} = file_size: {} / block_size: {}", num_block,
            file_size, block_size);

  for (size_t i = 0; i < parallelism; i++) {
    auto input_size = src_data_size[i];
    auto input_data = src_data_address[i];

    thread_pool.SubmitSync([i, input_size, input_data, path, parallelism,
                            num_block, &dst_data_size, &pending_packages,
                            &finish_cv]() {
      for (int idx = i; idx < num_block; idx += parallelism) {
        // compress one split file
        size_t output_size = LZ4_compressBound(input_size);
        char* output_data = (char*)malloc(output_size);
        LOGF_INFO("compressed data size: {} raw size: {}", output_size,
                  input_size);
        auto ret = LZ4_compress_HC(input_data, output_data, input_size,
                                   output_size, 6);
        dst_data_size.at(i) = ret;
        if (ret != 0) {
          LOGF_INFO("compress {} success, before size {}M after size {}M", i,
                    input_size, ret);
          // write back to disk
          std::ofstream file(path + std::to_string(i) + ".lz4.bin",
                             std::ios::binary);
          file.write(output_data, ret);
        } else {
          LOG_INFO("压缩失败！");
        }
        free(output_data);
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
  size_t compress_file_size = 0;
  for (int i = 0; i < parallelism; i++) {
    compress_file_size += dst_data_size[i];
  }
  LOGF_INFO("compress ratio: {}",
            (double)compress_file_size / (double)file_size);
}
