#include <gflags/gflags.h>
#include <stdlib.h>
#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>

#include "core/common/bitmap.h"
#include "core/common/multithreading/task.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph_metadata.h"
#include "util/logging.h"

DEFINE_uint32(type, 0, "normal read time mode");
DEFINE_string(dir, "/Users/liuyang/sics/refactor/graph-systems/testfile/",
              "compress directory");
DEFINE_string(file, "0.bin", "compress file name");
DEFINE_uint32(p, 8, "thread pool size");

using VertexID = sics::graph::core::common::VertexID;
using VertexIndex = sics::graph::core::common::VertexIndex;
using EdgeIndex = sics::graph::core::common::EdgeIndex;
using Bitmap = sics::graph::core::common::Bitmap;

EdgeIndex compressFirstEdge(char* base, EdgeIndex offset, VertexID first,
                            VertexID second) {
  // char* saveStart = base;
  // long saveOffset = offset;

  int bytesUsed = 0;
  char firstByte = 0;
  VertexID toCompress = 0;
  if (first > second) {
    toCompress = second - first;
  } else
    toCompress = first - second;

  firstByte = toCompress & 0x3f;  // 0011|1111
  if (first > second) {
    firstByte |= 0x40;  // 0100|0000 means second is smaller
  } else {
    firstByte |= 0x80;  // 1000|0000 means first is smaller
  }
  toCompress = toCompress >> 6;
  base[offset] = firstByte;
  offset++;

  char curByte = toCompress & 0x7f;
  while ((curByte > 0) || (toCompress > 0)) {
    bytesUsed++;
    char toWrite = curByte;
    toCompress = toCompress >> 7;
    // Check to see if there's any bits left to represent
    curByte = toCompress & 0x7f;
    if (toCompress > 0) {
      toWrite |= 0x80;
    }
    base[offset] = toWrite;
    offset++;
  }
  return offset;
}

EdgeIndex compressEdge(char* base, EdgeIndex offset, VertexID e) {
  char curByte = e & 0x7f;
  int bytesUsed = 0;
  while ((curByte > 0) || (e > 0)) {
    bytesUsed++;
    char toWrite = curByte;
    e = e >> 7;
    // Check to see if there's any bits left to represent
    curByte = e & 0x7f;
    if (e > 0) {
      toWrite |= 0x80;
    }
    base[offset] = toWrite;
    offset++;
  }
  return offset;
}

char* byte_encode(VertexIndex idx, VertexID src, const uint32_t* out_edges,
                  int degree, VertexID* offset_new) {
  auto out_edges_copy = (VertexID*)malloc(degree * sizeof(VertexID));
  auto dst = (char*)malloc(degree * sizeof(VertexID));
  // sort out edges of src
  memcpy(out_edges_copy, out_edges, degree * sizeof(VertexID));
  std::sort(out_edges_copy, out_edges_copy + degree);
  std::vector<EdgeIndex> offset(degree);
  // compress first edge
  offset[0] = compressFirstEdge(dst, 0, src, out_edges_copy[0]);
  for (int i = 1; i < degree; i++) {
    if (out_edges_copy[i] < out_edges_copy[i - 1]) {
      LOG_FATAL("compress error! order is not right");
    }
    offset[i] = compressEdge(dst, offset[i - 1],
                             out_edges_copy[i] - out_edges_copy[i - 1]);
  }
  return dst;
}

void encode(std::string dir, int parallelism) {
  // read meta info
  YAML::Node metadata_node = YAML::LoadFile(dir + "metadata.yaml");
  sics::graph::core::data_structures::GraphMetadata metadata =
      metadata_node["GraphMetadata"]
          .as<sics::graph::core::data_structures::GraphMetadata>();
  auto subgraph = metadata.GetSubgraphMetadata(0);

  auto graph_path = dir + "graphs/" + std::to_string(subgraph.gid) + ".bin";
  std::ifstream file(graph_path, std::ios::binary);
  if (!file) {
    LOG_FATAL("Error opening bin file: ", graph_path.c_str());
  }
  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  file.seekg(0, std::ios::beg);
  auto src_data = (char*)malloc(file_size);
  file.read(src_data, file_size);

  // find the ptr of subgraph structure

  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(parallelism);
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);

  std::vector<char*> address(parallelism);
  std::vector<EdgeIndex> offsets(parallelism);
  auto step = ceil(subgraph.num_vertices / parallelism);
  uint32_t begin = 0, end = 0;
  for (int i = 0; i < parallelism; i++) {
    begin = end;
    end += step;
    if (i == parallelism - 1) {
      end = subgraph.num_vertices;
    }
    thread_pool.SubmitSync(
        [i, offsets, address, begin, end, &pending_packages, &finish_cv]() {
          for (VertexID i = begin; i < end; i++) {

          }
          if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
          return;
        });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
}

void decode() {}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto dir = FLAGS_dir;
  auto file = FLAGS_file;
  // auto task_num = FLAGS_p;

  //  int arr[] = {5, 3, 7, 1, 9};
  auto arr = (int*)malloc(5 * sizeof(int));
  arr[0] = 5;
  arr[1] = 3;
  arr[2] = 7;
  arr[3] = 1;
  arr[4] = 9;

  // Get the size of the array
  int n = 5;
  // Use std::sort to sort the array in ascending order
  std::sort(arr, arr + n);
  // Print the elements of the array
  for (int i = 0; i < n; ++i) {
    std::cout << arr[i] << ' ';
  }
}