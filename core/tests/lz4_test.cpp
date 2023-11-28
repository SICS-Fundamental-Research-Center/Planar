#include <gtest/gtest.h>
#include <lz4.h>
#include <lz4hc.h>
#include <stdio.h>
#include <stdlib.h>

#include <fstream>

#include "core/common/multithreading/task.h"
#include "core/common/multithreading/thread_pool.h"
#include "util/logging.h"

#define LZ4_MEMORY_USAGE 20

namespace xyz::graph::core::tests {

class Lz4Test : public ::testing::Test {
 protected:
  Lz4Test() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }

  std::string data_dir = TEST_DATA_DIR;
  std::string compress_file = data_dir + "/compress.txt";
};

TEST_F(Lz4Test, SimpleTest) {
  FILE* file = fopen(compress_file.c_str(), "r");
  fseek(file, 0, SEEK_END);
  long size = ftell(file);
  rewind(file);
  char* input_data = (char*)malloc(size);
  size_t read_count = fread(input_data, 1, size, file);
  printf("read count: %d\n", read_count);
  //  char* input_data = "Hello, World!";
  //  size_t input_size = strlen(input_data);
  size_t input_size = read_count;
  size_t output_size = LZ4_COMPRESSBOUND(input_size);
  char dst[output_size];
  //    auto ret = LZ4_compress_default(input_data, dst, input_size,
  //    output_size);
  //  auto ret = LZ4_compress_fast(input_data, dst, input_size, output_size, 0);
  auto level = LZ4HC_CLEVEL_MAX;
  auto ret = LZ4_compress_HC(input_data, dst, input_size, output_size, 6);
  LOGF_INFO("compress size: {}", ret);

  // write back to file
  std::ofstream out_file(compress_file + ".lz4", std::ios::binary);
  out_file.write(dst, ret);
  out_file.close();
  //    LZ4_decompress_safe();
}

TEST_F(Lz4Test, TimeForNormalRead) {
  std::string src_path = data_dir + "/0.bin";
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
}

TEST_F(Lz4Test, Compress) {
  size_t task_num = 8;
  std::string src_path = data_dir + "/2.bin";
  std::string path = data_dir + "/compress/";

  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(task_num);

  auto thread_pool = core::common::ThreadPool(task_num);

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

  std::vector<char*> src_data_address(task_num);
  std::vector<size_t> src_data_size(task_num);
  size_t step = ceil((double)file_size / task_num);
  for (int i = 0; i < task_num; i++) {
    src_data_address[i] = src_data + i * step;
    src_data_size[i] = step;
    if (i == task_num - 1) {
      src_data_size[i] = file_size - i * step;
    }
  }

  for (size_t i = 0; i < task_num; i++) {
    auto input_size = src_data_size[i];
    auto input_data = src_data_address[i];
    thread_pool.SubmitSync([i, input_size, input_data, path, &pending_packages,
                            &finish_cv]() {
      // compress one split file
      size_t output_size = LZ4_compressBound(input_size);
      char* output_data = (char*)malloc(output_size);
      LOGF_INFO("compressed data size: {} raw size: {}", output_size,
                input_size);

      //          auto ret = LZ4_compress_default(input_data, output_data,
      //          input_size,
      //                                          output_size);
      auto ret =
          LZ4_compress_HC(input_data, output_data, input_size, output_size, 6);

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
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
}

TEST_F(Lz4Test, Decompress) {
  size_t task_num = 8;
  std::string path = data_dir + "/compress/";

  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(task_num);
  auto thread_pool = core::common::ThreadPool(task_num);
  auto time_begin = std::chrono::system_clock::now();

  std::vector<char*> output_datas(task_num);
  std::vector<size_t> output_sizes(task_num);
  for (int i = 0; i < task_num; i++) {
    std::ifstream file(path + std::to_string(i) + ".lz4.bin", std::ios::binary);
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);
    auto src_data = (char*)malloc(file_size);
    file.read(src_data, file_size);
    output_datas[i] = src_data;
    output_sizes[i] = file_size;
  }

  auto time_mid = std::chrono::system_clock::now();
  LOGF_INFO("start decompress, Load time: {}",
            std::chrono::duration<double>(time_mid - time_begin).count());
  pending_packages.store(task_num);
  for (size_t i = 0; i < task_num; i++) {
    auto output_size = output_sizes[i];
    auto output_data = output_datas[i];
    auto input_size = 1775592509;
    thread_pool.SubmitSync([i, path, output_data, output_size, input_size,
                            &finish_cv, &pending_packages]() {
      auto input_data = (char*)malloc(input_size);
      auto ret =
          LZ4_decompress_safe(output_data, input_data, output_size, input_size);
      LOGF_INFO("{} decompress success, before size {}M after size {}M", i,
                output_size, input_size);
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  auto time_end = std::chrono::system_clock::now();

  LOGF_INFO("Decompress time with {} cores : {}", task_num,
            std::chrono::duration<double>(time_end - time_mid).count());
  LOGF_INFO("Total time used: {}",
            std::chrono::duration<double>(time_end - time_begin).count());
}

}  // namespace xyz::graph::core::tests