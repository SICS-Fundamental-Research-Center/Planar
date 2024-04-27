#include <snappy.h>
#include <stdlib.h>

#include <fstream>

#include "core/common/multithreading/task.h"
#include "core/common/multithreading/thread_pool.h"
#include "util/logging.h"

DEFINE_uint32(type, 0, "normal read time mode");
DEFINE_string(dir, "/Users/liuyang/sics/refactor/graph-systems/testfile/",
              "compress directory");
DEFINE_string(file, "0.bin", "compress file name");
DEFINE_uint32(p, 8, "thread pool size");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto dir = FLAGS_dir;
  auto file = FLAGS_file;
  auto task_num = FLAGS_p;

  if (FLAGS_type == 0) {
    std::string src_path = dir + file;
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
  } else if (FLAGS_type == 1) {
    // compress first
    std::string src_path = dir + file;
    std::string path = dir + "compress/";

    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);
    std::atomic<size_t> pending_packages(task_num);

    auto thread_pool = sics::graph::core::common::ThreadPool(task_num);

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
    std::vector<size_t> dst_data_size(task_num);
    size_t step = ceil((double)file_size / task_num);
    for (uint32_t i = 0; i < task_num; i++) {
      src_data_address[i] = src_data + i * step;
      src_data_size[i] = step;
      if (i == task_num - 1) {
        src_data_size[i] = file_size - i * step;
      }
    }

    // compress
    for (size_t i = 0; i < task_num; i++) {
      auto input_size = src_data_size[i];
      auto input_data = src_data_address[i];
      thread_pool.SubmitSync([i, input_size, input_data, path, &dst_data_size,
                              &pending_packages, &finish_cv]() {
        // compress one split file
        size_t output_size = snappy::MaxCompressedLength(input_size);
        char* output_data = (char*)malloc(output_size);
        LOGF_INFO("compressed data size: {} raw size: {}", output_size,
                  input_size);

        snappy::RawCompress(input_data, input_size, output_data, &output_size);
        dst_data_size[i] = output_size;
        LOGF_INFO("compress {} success, before size {}M after size {}M", i,
                  input_size, output_size);
        // write back to disk
        std::ofstream file(path + std::to_string(i) + ".snappy.bin",
                           std::ios::binary);
        file.write(output_data, output_size);
        free(output_data);
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
    size_t compress_file_size = 0;
    for (uint32_t i = 0; i < task_num; i++) {
      compress_file_size += dst_data_size[i];
    }
    LOGF_INFO("compress ratio: {}",
              (double)compress_file_size / (double)file_size);
  } else {
    // decompress and compute time
    std::string path = dir + "compress/";
    auto src_path = dir + file;
    std::ifstream src_file(src_path, std::ios::binary);
    if (!src_file) {
      LOG_FATAL("Error opening bin file: ", src_path.c_str());
    }
    src_file.seekg(0, std::ios::end);
    size_t file_size = src_file.tellg();
    src_file.seekg(0, std::ios::beg);
    src_file.close();
    size_t step = ceil((double)file_size / task_num);

    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);
    std::atomic<size_t> pending_packages(task_num);
    auto thread_pool = sics::graph::core::common::ThreadPool(task_num);
    auto time_begin = std::chrono::system_clock::now();

    std::vector<char*> output_datas(task_num);
    std::vector<size_t> output_sizes(task_num);
    for (uint32_t i = 0; i < task_num; i++) {
      std::ifstream file(path + std::to_string(i) + ".snappy.bin",
                         std::ios::binary);
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
      auto input_size = step;
      thread_pool.SubmitSync([i, path, output_data, output_size, input_size,
                              &finish_cv, &pending_packages]() {
        auto input_data = (char*)malloc(input_size);
        snappy::RawUncompress(output_data, output_size, input_data);
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
}