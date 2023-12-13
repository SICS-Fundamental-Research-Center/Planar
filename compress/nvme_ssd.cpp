#include <gflags/gflags.h>
#include <lz4.h>
#include <lz4hc.h>

#include <boost/chrono.hpp>
#include <boost/chrono/thread_clock.hpp>
#include <boost/thread.hpp>
#include <boost/thread/future.hpp>
#include <fstream>
#include <vector>

#include "core/common/multithreading/task.h"
#include "core/common/multithreading/thread_pool.h"
#include "util/logging.h"
#define LZ4_MEMORY_USAGE 20

DEFINE_string(i, "0.bin", "compress file name");
DEFINE_uint32(p, 8, "thread pool size");
DEFINE_string(type, "normal", "normal read time mode");
DEFINE_string(dir, "/Users/liuyang/sics/refactor/graph-systems/testfile/",
              "compress directory");
DEFINE_uint32(compress_parallelism, 32, "compress parallelism");
DEFINE_uint32(ssd_parallelism, 1, "ssd parallelism");
DEFINE_uint32(decompress_parallelism, 32, "decompress parallelism");

DEFINE_string(op, "xor", "operation type");

using namespace boost::chrono;

size_t read_block(int i, char* buffer, size_t offset, size_t read_size,
                  std::string file_path) {
  thread_clock::time_point start = thread_clock::now();
  std::ifstream src_file(file_path, std::ios::binary);
  src_file.seekg(offset, std::ios::beg);
  src_file.read(buffer + offset, read_size);
  thread_clock::time_point end = thread_clock::now();
  boost::chrono::nanoseconds duration = end - start;
  boost::chrono::milliseconds milli =
      boost::chrono::duration_cast<boost::chrono::milliseconds>(duration);
  //  LOGF_INFO("{} boost time used for nvme read: {} {}", i, milli.count(),
  //            duration.count());
  return duration.count();
}

size_t read_block_with_lz4decompress(int i, char* buffer, size_t step,
                                     size_t decompress_size, std::string dir,
                                     int parallelism, int max = 32) {
  thread_clock::time_point start = thread_clock::now();
  for (int idx = i; idx < max; idx += parallelism) {
    std::string file_path =
        dir + "compress/" + std::to_string(idx) + ".lz4.bin";
    std::ifstream src_file(file_path, std::ios::binary);
    src_file.seekg(0, std::ios::end);
    size_t read_size = src_file.tellg();
    src_file.seekg(0, std::ios::beg);
    auto src = (char*)malloc(read_size);
    src_file.read(src, read_size);

    auto offset = idx * step;
    auto ret =
        LZ4_decompress_safe(src, buffer + offset, read_size, decompress_size);
    LOGF_INFO("{} decompress success, before size {} M after size {} M -- {} ",
              idx, read_size, ret, decompress_size);
  }
  thread_clock::time_point end = thread_clock::now();
  boost::chrono::nanoseconds duration = end - start;
  boost::chrono::milliseconds milli =
      boost::chrono::duration_cast<boost::chrono::milliseconds>(duration);
  LOGF_INFO("{} boost time used for nvme read: {} {}", i, milli.count(),
            duration.count());
  return duration.count();
}

size_t decompress_block(int i, char* buffer, char* dst, size_t offset,
                        size_t compress_size, size_t decompress_size) {
  thread_clock::time_point start = thread_clock::now();
  auto ret =
      LZ4_decompress_safe(buffer, dst + offset, compress_size, decompress_size);
  //  LOGF_INFO("{} decompress success, before size {} M after size {} M -- {}
  //  ", i,
  //            compress_size, ret, decompress_size);

  thread_clock::time_point end = thread_clock::now();
  boost::chrono::nanoseconds duration = end - start;
  boost::chrono::milliseconds milli =
      boost::chrono::duration_cast<boost::chrono::milliseconds>(duration);
  //  LOGF_INFO("{} boost time used for nvme read: {} {}", i, milli.count(),
  //            duration.count());
  return duration.count();
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto file_path = FLAGS_i;
  auto parallelism = FLAGS_p;
  auto mode = FLAGS_type;
  auto dir = FLAGS_dir;
  auto ssd_p = FLAGS_ssd_parallelism;
  auto decompress_p = FLAGS_decompress_parallelism;
  auto compress_p = FLAGS_compress_parallelism;

  auto op = FLAGS_op;

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
  } else if (mode == "lz4") {
    auto path = dir + "0.bin";
    std::ifstream src_file(path, std::ios::binary);
    src_file.seekg(0, std::ios::end);
    size_t file_size = src_file.tellg();
    src_file.seekg(0, std::ios::beg);
    auto buffer = (char*)malloc(file_size);

    auto time_b = std::chrono::system_clock::now();
    size_t step = ceil((double)file_size / compress_p);
    std::vector<size_t> decompress_sizes(compress_p);
    for (int i = 0; i < compress_p; i++) {
      decompress_sizes[i] = step;
      if (i == compress_p - 1) {
        decompress_sizes[i] = file_size - i * step;
      }
    }

    std::vector<boost::thread> threads;
    std::vector<boost::unique_future<size_t>> futs;

    for (int i = 0; i < parallelism; i++) {
      auto decompress_size = decompress_sizes[i];
      boost::packaged_task<size_t> pt(
          boost::bind(read_block_with_lz4decompress, i, buffer, step,
                      decompress_size, dir, parallelism, compress_p));
      auto fut = pt.get_future();
      futs.push_back(boost::move(fut));
      threads.push_back(
          boost::thread(boost::move(pt)));  // boost::move is necessary
    }
    for (auto& t : threads) {
      t.join();
    }
    size_t res = 0;
    for (auto& fut : futs) {
      res += fut.get();
    }
    LOGF_INFO("total time used for nvme read: {} {}", res,
              res / double(1000000000));

    auto time_e = std::chrono::system_clock::now();
    LOGF_INFO("time used for normal read: {}",
              std::chrono::duration<double>(time_e - time_b).count());

  } else if (mode == "lz42") {
    auto time_b = std::chrono::system_clock::now();
    size_t time_load = 0;
    std::vector<char*> buffers(compress_p);
    std::vector<size_t> compress_sizes(compress_p);
    for (int i = 0; i < compress_p; i++) {
      std::string file = dir + "compress/" + std::to_string(i) + ".lz4.bin";
      std::ifstream src_file(file, std::ios::binary);
      src_file.seekg(0, std::ios::end);
      size_t file_size = src_file.tellg();
      src_file.seekg(0, std::ios::beg);
      buffers[i] = (char*)malloc(file_size);
      compress_sizes[i] = file_size;

      std::vector<size_t> offsets(ssd_p);
      std::vector<size_t> sizes(ssd_p);
      size_t step = ceil((double)file_size / ssd_p);
      for (int k = 0; k < ssd_p; k++) {
        offsets[k] = k * step;
        sizes[k] = step;
        if (k == ssd_p - 1) {
          sizes[k] = file_size - k * step;
        }
      }

      std::vector<boost::thread> threads;
      std::vector<boost::unique_future<size_t>> futs;

      for (int j = 0; j < ssd_p; j++) {
        auto offset = offsets[j];
        auto size = sizes[j];
        boost::packaged_task<size_t> pt(
            boost::bind(read_block, i, buffers[i], offset, size, file));
        auto fut = pt.get_future();
        futs.push_back(boost::move(fut));
        threads.push_back(boost::thread(boost::move(pt)));
      }
      // join and count time
      for (auto& t : threads) {
        t.join();
      }
      for (auto& fut : futs) {
        time_load += fut.get();
      }
    }
    auto time_m = std::chrono::system_clock::now();
    LOGF_INFO("cpu time for load {} s: {} ns", time_load / double(1000000000),
              time_load);
    LOGF_INFO("normal time used for load {} s",
              std::chrono::duration<double>(time_m - time_b).count());

    // for decompress
    std::ifstream graph_file(file_path, std::ios::binary);
    graph_file.seekg(0, std::ios::end);
    size_t graph_size = graph_file.tellg();
    graph_file.seekg(0, std::ios::beg);

    auto graph = (char*)malloc(graph_size);

    size_t step = ceil((double)graph_size / compress_p);
    std::vector<size_t> graph_offsets(compress_p);
    std::vector<size_t> graph_sizes(compress_p);
    for (int i = 0; i < compress_p; i++) {
      graph_offsets[i] = i * step;
      graph_sizes[i] = step;
      if (i == compress_p - 1) {
        graph_sizes[i] = graph_size - i * step;
      }
    }

    size_t time_decompress = 0;
    std::vector<boost::thread> threads;
    std::vector<boost::unique_future<size_t>> futs;
    for (int i = 0; i < decompress_p; i++) {
      auto buffer = buffers[i];
      auto compress_size = compress_sizes[i];
      auto decompress_offset = graph_offsets[i];
      auto decompress_size = graph_sizes[i];
      boost::packaged_task<size_t> pt(
          boost::bind(decompress_block, i, buffer, graph, decompress_offset,
                      compress_size, decompress_size));
      auto fut = pt.get_future();
      futs.push_back(boost::move(fut));
      threads.push_back(boost::thread(boost::move(pt)));
    }
    for (auto& t : threads) {
      t.join();
    }
    for (auto& fut : futs) {
      time_decompress += fut.get();
    }
    LOGF_INFO("cpu time for decompress {} s: {} ns",
              time_decompress / double(1000000000), time_decompress);
    LOGF_INFO("cpu time for all: {} {}", time_decompress + time_load,
              (time_decompress + time_load) / double(1000000000));
    auto time_e = std::chrono::system_clock::now();
    LOGF_INFO("normal time used for decompress: {}",
              std::chrono::duration<double>(time_e - time_m).count());
    LOGF_INFO("normal time used total {}",
              std::chrono::duration<double>(time_e - time_b).count());

  } else if (mode == "linear_scan") {
    auto time_begin = std::chrono::system_clock::now();
    std::ifstream src_file(file_path, std::ios::binary);
    src_file.seekg(0, std::ios::end);
    size_t file_size = src_file.tellg();
    src_file.seekg(0, std::ios::beg);
    auto buffer = (char*)malloc(file_size);
    src_file.read(buffer, file_size);
    auto time_mid = std::chrono::system_clock::now();

    auto buffer_new = (char*)malloc(file_size);

    auto time_b2 = std::chrono::system_clock::now();
    if (op == "xor") {
      buffer_new[0] = buffer[0];
      for (int i = 1; i < file_size; i++) {
        buffer_new[i] = buffer[i - 1] ^ buffer[i];
      }
    } else {
      auto size = file_size / 4;
      uint32_t* src = (uint32_t*)buffer;
      uint32_t* dst = (uint32_t*)buffer_new;
      for (int i = 0; i < size; i++) {
        auto a = src[i];
        char b = a;
        if (i + b < size) {
          dst[i + b] = a;
        } else {
          dst[i] = a;
        }
      }
    }

    auto time_end = std::chrono::system_clock::now();
    std::ofstream dst_file(file_path + ".linear_scan", std::ios::binary);
    dst_file.write(buffer_new, file_size);
    LOGF_INFO("normal time used for loading {}",
              std::chrono::duration<double>(time_mid - time_begin).count());
    LOGF_INFO("normal time used for linear scan: {}",
              std::chrono::duration<double>(time_end - time_b2).count());

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

    auto time_begin = std::chrono::system_clock::now();

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

    std::vector<boost::thread> threads;
    std::vector<boost::unique_future<size_t>> futs;

    for (int i = 0; i < parallelism; i++) {
      auto offset = offsets[i];
      auto read_size = read_sizes[i];

      boost::packaged_task<size_t> pt(
          boost::bind(read_block, i, buffer, offset, read_size, file_path));
      auto fut = pt.get_future();
      futs.push_back(boost::move(fut));
      threads.push_back(
          boost::thread(boost::move(pt)));  // boost::move is necessary
      //      thread_pool.SubmitSync([i, buffer, offset, read_size, file_path,
      //                              &pending_packages, &finish_cv]() {
      //        std::ifstream src_file(file_path, std::ios::binary);
      //        src_file.seekg(offset, std::ios::beg);
      //        src_file.read(buffer + offset, read_size);
      //        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      //        return;
      //      });
    }
    //    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
    for (auto& t : threads) {
      t.join();
    }
    size_t res = 0;
    for (auto& fut : futs) {
      res += fut.get();
    }
    LOGF_INFO("total time used for nvme read: {} {}", res,
              res / double(1000000000));
    auto time_end = std::chrono::system_clock::now();

    LOGF_INFO("time used for normal read: {}",
              std::chrono::duration<double>(time_end - time_begin).count());
  }
}