// This file belongs to the SICS graph-systems project, a C++ library for
// exploiting parallelism graph computing. It is response for preprocessing
// graph.

#include <cstdint>
#include <fstream>
#include <iostream>

#include "yaml-cpp/yaml.h"

#include "common/bitmap.h"
#include "common/multithreading/thread_pool.h"
#include "gflags/gflags.h"
#include "util/atomic.h"
#include "util/logging.h"

#define vid_t uint32_t

DEFINE_string(convert_mode, "\0", "Conversion mode");
DEFINE_string(i, "", "input path.");
DEFINE_string(o, "", "output path.");
DEFINE_string(sep, "", "seperator to splite csv file.");
DEFINE_bool(read_head, false, "whether to read header of csv.");

inline std::pair<vid_t, vid_t> SplitEdge(const std::string& str,
                                         const std::string sep) {
  char* strc = new char[strlen(str.c_str()) + 1];
  strcpy(strc, str.c_str());
  char* tmpStr = strtok(strc, sep.c_str());
  vid_t out[2];
  for (size_t i = 0; i < 2; i++) {
    out[i] = atoll(tmpStr);
    tmpStr = strtok(NULL, sep.c_str());
  }
  delete[] strc;
  return std::make_pair(out[0], out[1]);
};

void ConvertEdgelist(const std::string input_path,
                     const std::string output_path, const std::string sep,
                     const bool read_head) {
  // auto thread_pool = sics::graph::core::common::ThreadPool(
  //     std::thread::hardware_concurrency()); To update.
  std::ifstream in_file(input_path);
  std::ofstream out_data_file(output_path + "edgelist.bin");
  std::ofstream out_meta_file(output_path + "meta.yaml");

  std::vector<std::pair<vid_t, vid_t>> edges_vec;
  edges_vec.reserve(65536);

  vid_t max_vid = 0;
  std::string line;
  if (in_file) {
    while (getline(in_file, line)) {
      auto out = SplitEdge(line, sep);
      edges_vec.push_back(out);
      sics::graph::core::util::atomic::WriteMax(&max_vid, (vid_t)out.first);
      sics::graph::core::util::atomic::WriteMax(&max_vid, (vid_t)out.second);
    }
  }

  auto bitmap = sics::graph::core::common::Bitmap(max_vid);
  auto buffer_edges = (vid_t*)malloc(sizeof(vid_t) * edges_vec.size() * 2);
  memset(buffer_edges, 0, sizeof(vid_t) * edges_vec.size() * 2);

  size_t i = 0;
  for (auto iter = edges_vec.begin(); iter != edges_vec.end(); iter++) {
    buffer_edges[i * 2] = iter->first;
    buffer_edges[i * 2 + 1] = iter->second;
    bitmap.SetBit(buffer_edges[i * 2]);
    bitmap.SetBit(buffer_edges[i * 2 + 1]);
    i++;
  }

  // Write binary edgelist
  out_data_file.write((char*)buffer_edges,
                      sizeof(vid_t) * 2 * edges_vec.size());

  // Write Meta date.
  YAML::Node node;
  node["edgelist_bin"]["num_vertices"] = bitmap.Count();
  node["edgelist_bin"]["num_edges"] = edges_vec.size();
  node["edgelist_bin"]["max_vid"] = max_vid;
  LOG_INFO(bitmap.Count(), " ", edges_vec.size());
  out_meta_file << node << std::endl;

  delete buffer_edges;
  in_file.close();
  out_data_file.close();
  out_meta_file.close();

  return;
}

int main(int argc, char** argv) {
  gflags::SetUsageMessage(
      "\n \tUSAGE: graph-convert --convert_mode=[options] -i <input file path> "
      "-o <output file path> --sep=[options] \n"
      "General options:\n"
      "\t\t edgelistcsv2edgelistbin:  - Convert edge list txt to binary edge "
      "list\n"
      "\t\t edgelistcsv2csr:   - Convert edge list of txt format to binary "
      "csr\n"
      "\t\t edgelistbin2csr:   - Convert edge list of bin format to binary "
      "csr\n");

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  assert(FLAGS_i != "" && FLAGS_o != "" && FLAGS_convert_mode != "\0");

  std::cout << FLAGS_convert_mode << std::endl;
  if (FLAGS_convert_mode == "edgelistcsv2edgelistbin") {
    assert(FLAGS_sep != "");
    ConvertEdgelist(FLAGS_i, FLAGS_o, FLAGS_sep, FLAGS_read_head);
  } else if (FLAGS_convert_mode == "edgelistcsv2csr") {
    // TO ADD.
  } else if (FLAGS_convert_mode == "edgelistbin2csr") {
    // TO ADD.
  } else {
    std::cout << "Error convert mode." << std::endl;
  }

  gflags::ShutDownCommandLineFlags();
  return 0;
}