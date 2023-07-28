// This file belongs to the SICS graph-systems project, a C++ library for
// exploiting parallelism graph computing. It is response for preprocessing
// graph, e.g. convert a graph from Edgelist to CSR.

#include <fstream>
#include <iostream>
#include <type_traits>

#include "yaml-cpp/yaml.h"
#include "gflags/gflags.h"

#include "common/bitmap.h"
#include "common/multithreading/thread_pool.h"
#include "common/types.h"
#include "util/atomic.h"
#include "util/logging.h"

#define vid_t sics::graph::core::common::GraphIDType

// Define convert mode.
#define CONVERTMODE(F) \
  F(edgelistcsv2edgelistbin), F(edgelistcsv2csrbin), F(edgelistbin2csrbin)

#define F(e) e
enum ConvertMode { CONVERTMODE(F), NumConvertMode };
#undef F

#define F(s) #s
std::string ConvertModeName[] = {CONVERTMODE(F)};
#undef F

ConvertMode String2Enmu(std::string s) {
  return ConvertMode(
      std::find(ConvertModeName, ConvertModeName + NumConvertMode, s) -
      ConvertModeName);
}

DEFINE_string(convert_mode, "", "Conversion mode");
DEFINE_string(i, "", "input path.");
DEFINE_string(o, "", "output path.");
DEFINE_string(sep, "", "seperator to splite csv file.");
DEFINE_bool(read_head, false, "whether to read header of csv.");

void ConvertEdgelist(const std::string& input_path,
                     const std::string& output_path, const std::string& sep,
                     const bool& read_head) {
  // TODO(haisaoko):  auto thread_pool = sics::graph::core::common::ThreadPool(
  //     std::thread::hardware_concurrency());
  std::ifstream in_file(input_path);
  std::ofstream out_data_file(output_path + "edgelist.bin");
  std::ofstream out_meta_file(output_path + "meta.yaml");

  std::vector<vid_t> edges_vec;
  edges_vec.reserve(65536);

  vid_t max_vid = 0;
  std::string line, vid_str;
  if (in_file) {
    while (getline(in_file, line)) {
      std::stringstream ss(line);
      while (getline(ss, vid_str, *sep.c_str())) {
        vid_t vid = stoll(vid_str);
        edges_vec.push_back(vid);
        sics::graph::core::util::atomic::WriteMax(&max_vid, vid);
      }
    }
  }

  auto bitmap = sics::graph::core::common::Bitmap(max_vid);
  auto buffer_edges = (vid_t*)malloc(sizeof(vid_t) * edges_vec.size() * 2);
  memset(buffer_edges, 0, sizeof(vid_t) * edges_vec.size() * 2);

  size_t i = 0;
  for (auto iter = edges_vec.begin(); iter != edges_vec.end(); iter++) {
    buffer_edges[i++] = *iter;
    bitmap.SetBit(*iter);
  }

  // Write binary edgelist
  out_data_file.write((char*)buffer_edges,
                      sizeof(vid_t) * 2 * edges_vec.size());

  // Write Meta date.
  YAML::Node node;
  node["edgelist_bin"]["num_vertices"] = bitmap.Count();
  node["edgelist_bin"]["num_edges"] = edges_vec.size();
  node["edgelist_bin"]["max_vid"] = max_vid;
  out_meta_file << node << std::endl;

  delete buffer_edges;
  in_file.close();
  out_data_file.close();
  out_meta_file.close();
}

int main(int argc, char** argv) {
  gflags::SetUsageMessage(
      "\n USAGE: graph-convert --convert_mode=[options] -i <input file path> "
      "-o <output file path> --sep=[separator] \n"
      " General options:\n"
      "\t edgelistcsv2edgelistbin:  - Convert edge list txt to binary edge "
      "list\n"
      "\t edgelistcsv2csrbin:   - Convert edge list of txt format to binary "
      "csr\n"
      "\t edgelistbin2csrbin:   - Convert edge list of bin format to binary "
      "csr\n");

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_i == "" || FLAGS_o == "")
    LOG_ERROR("Input (output) path is empty.");

  switch (String2Enmu(FLAGS_convert_mode)) {
    case edgelistcsv2edgelistbin:
      assert(FLAGS_sep != "");
      if (FLAGS_sep == "")
        LOG_ERROR("CSV separator is not empty. Use -sep [e.g. \",\"].");
      ConvertEdgelist(FLAGS_i, FLAGS_o, FLAGS_sep, FLAGS_read_head);
      break;
    case edgelistcsv2csrbin:
      // TO ADD (hsiaoko)
      break;
    case edgelistbin2csrbin:
      // TO ADD (hsiaoko)
      break;
    default:
      LOG_INFO("Error convert mode.");
      break;
  }

  gflags::ShutDownCommandLineFlags();
  return 0;
}