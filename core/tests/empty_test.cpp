#include <gtest/gtest.h>
#include <lzma.h>
#include <stdio.h>
#include <stdlib.h>

#include <fstream>

#include "core/common/multithreading/task.h"
#include "core/common/multithreading/thread_pool.h"
#include "util/logging.h"

namespace xyz::graph::core::tests {

class SampleTest : public ::testing::Test {
 protected:
  SampleTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }

  std::string data_dir = TEST_DATA_DIR;
  std::string compress_file = data_dir + "/compress.txt";
};

TEST_F(SampleTest, SomeValuesAreAlwaysEqual) { EXPECT_EQ(1, 1); }

TEST_F(SampleTest, XzTest) {
  // 原始数据
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

  // 分配压缩后的数据缓冲区
  size_t output_size = lzma_stream_buffer_bound(input_size);
  char* output_data = (char*)malloc(output_size);
  printf("compressed data size: %d raw size: %d\n", output_size, input_size);

  // 初始化压缩和解压缩器
  lzma_stream strm = LZMA_STREAM_INIT;
  lzma_ret ret = lzma_easy_encoder(&strm, 6, LZMA_CHECK_CRC64);

  // 压缩数据
  strm.next_in = (uint8_t*)input_data;
  strm.avail_in = input_size;
  strm.next_out = (uint8_t*)output_data;
  strm.avail_out = output_size;
  ret = lzma_code(&strm, LZMA_FINISH);
  printf("ret: %d\n", ret);
  // 检查压缩结果
  if (ret == LZMA_OK || ret == LZMA_SYNC_FLUSH) {
    printf("压缩成功！\n");
    printf("压缩前大小：%zu\n", input_size);
    printf("压缩后大小：%zu\n", strm.total_out);
  } else {
    printf("压缩失败！\n");
  }

  // 重置压缩和解压缩器
  lzma_end(&strm);
  strm = LZMA_STREAM_INIT;
  ret = lzma_stream_decoder(&strm, UINT64_MAX, LZMA_CONCATENATED);

  // 解压缩数据
  strm.next_in = (uint8_t*)output_data;
  strm.avail_in = strm.total_out;
  strm.next_out = (uint8_t*)input_data;
  strm.avail_out = input_size;
  ret = lzma_code(&strm, LZMA_RUN);

  // 检查解压缩结果
  if (ret == LZMA_OK) {
    printf("解压缩成功！\n");
    printf("解压缩后数据：%s\n", input_data);
  } else {
    printf("解压缩失败！\n");
  }

  // 清理资源
  free(output_data);
}

TEST_F(SampleTest, XzLargeFileCompress) {
  size_t task_num = 8;
  std::string src_path = data_dir + "/0.bin";
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
    LOG_FATAL("Error opening bin file: ", path.c_str());
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
      size_t output_size = lzma_stream_buffer_bound(input_size);
      char* output_data = (char*)malloc(output_size);
      printf("compressed data size: %d raw size: %d\n", output_size,
             input_size);

      lzma_stream strm = LZMA_STREAM_INIT;
      lzma_ret ret = lzma_easy_encoder(&strm, 6, LZMA_CHECK_CRC64);
      strm.next_in = (uint8_t*)input_data;
      strm.avail_in = input_size;
      strm.next_out = (uint8_t*)output_data;
      strm.avail_out = output_size;
      LOGF_INFO("begin compress {}", i);
      ret = lzma_code(&strm, LZMA_FINISH);
      if (ret == LZMA_OK || ret == LZMA_SYNC_FLUSH) {
        LOGF_INFO("compress {} success, before size {}M after size {}M", i,
                  input_size, strm.total_out);
        // write back to disk
        std::ofstream file(path + std::to_string(i) + ".bin", std::ios::binary);
        file.write(output_data, strm.total_out);
      } else {
        printf("压缩失败！\n");
      }
      free(output_data);
      lzma_end(&strm);
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  // finish compress

  // start decompress
  //  auto time_begin = std::chrono::system_clock::now();
  //  pending_packages.store(task_num);
  //  for (size_t i = 0; i < task_num; i++) {
  //  }
  //  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
  //
  //  auto time_end = std::chrono::system_clock::now();
  //
  //  LOGF_INFO("Decompress time with {} cores : {}", task_num,
  //            std::chrono::duration<double>(time_end - time_begin).count());
}

TEST_F(SampleTest, XzLargeFileDecompress) {
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
    std::ifstream file(path + std::to_string(i) + ".bin", std::ios::binary);
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);
    auto src_data = (char*)malloc(file_size);
    file.read(src_data, file_size);
    output_datas[i] = src_data;
    output_sizes[i] = file_size;
  }

  LOG_INFO("start decompress");
  pending_packages.store(task_num);
  for (size_t i = 0; i < task_num; i++) {
    auto output_size = output_sizes[i];
    auto output_data = output_datas[i];
    thread_pool.SubmitSync([i, path, output_data, output_size, &finish_cv,
                            &pending_packages]() {
      lzma_stream strm = LZMA_STREAM_INIT;
      lzma_ret ret = lzma_stream_decoder(&strm, UINT64_MAX, LZMA_CONCATENATED);
      auto input_size = 1353157548;
      auto input_data = (char*)malloc(sizeof(char) * input_size);

      strm.next_in = (uint8_t*)output_data;
      strm.avail_in = output_size;
      strm.next_out = (uint8_t*)input_data;
      strm.avail_out = input_size;
      ret = lzma_code(&strm, LZMA_RUN);
      if (ret == LZMA_OK) {
        LOGF_INFO("{} decompress success, before size {}M after size {}M", i,
                  output_size, strm.total_out);
      } else {
        LOG_INFO("decompress failed");
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  auto time_end = std::chrono::system_clock::now();

  LOGF_INFO("Decompress time with {} cores : {}", task_num,
            std::chrono::duration<double>(time_end - time_begin).count());
}

}  // namespace xyz::graph::core::tests
