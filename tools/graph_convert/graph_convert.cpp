// This file belongs to the SICS graph-systems project, a C++ library for
// exploiting parallelism graph computing.
//
// We store graphs in a binary format. Other formats such as edge-list can be
// converted to our format with graph_convert tool. You can use graph_convert as
// follows: e.g. convert a graph from edgelist CSV to edgelist bin file.
// USAGE: graph-convert --convert_mode=[options] -i <input file path> -o <output
// file path> --sep=[separator]

#include <fstream>
#include <iostream>
#include <type_traits>

#include <gflags/gflags.h>
#include <yaml-cpp/yaml.h>

#include "common/bitmap.h"
#include "common/multithreading/thread_pool.h"
#include "common/types.h"
#include "util/atomic.h"
#include "util/logging.h"

using sics::graph::core::common::VertexID;

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
                     const std::string& output_path,
                     const std::string& sep,
                     bool read_head) {
  // TO ADD(hsiaoko): auto thread_pool = sics::graph::core::common::ThreadPool(
  //     std::thread::hardware_concurrency());
  std::ifstream in_file(input_path);
  std::ofstream out_data_file(output_path + "edgelist.bin");
  std::ofstream out_meta_file(output_path + "meta.yaml");

  std::vector<VertexID> edges_vec;
  edges_vec.reserve(65536);

  VertexID max_vid = 0;
  std::string line, vid_str;
  if (in_file) {
    if (read_head) getline(in_file, line);
    while (getline(in_file, line)) {
      std::stringstream ss(line);
      while (getline(ss, vid_str, *sep.c_str())) {
        VertexID vid = stoll(vid_str);
        edges_vec.push_back(vid);
        sics::graph::core::util::atomic::WriteMax(&max_vid, vid);
      }
    }
  }

  auto bitmap = sics::graph::core::common::Bitmap(max_vid);
  auto buffer_edges =
      (VertexID*)malloc(sizeof(VertexID) * edges_vec.size() * 2);
  memset(buffer_edges, 0, sizeof(VertexID) * edges_vec.size() * 2);

  size_t i = 0;
  for (auto iter = edges_vec.begin(); iter != edges_vec.end(); iter++) {
    buffer_edges[i++] = *iter;
    bitmap.SetBit(*iter);
  }

  // Write binary edgelist
  out_data_file.write((char*) buffer_edges,
                      sizeof(VertexID) * 2 * edges_vec.size());

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
  if (FLAGS_i == "" || FLAGS_o == "") {
    LOG_ERROR("Input (output) path is empty.");
    return -1;
  }

  switch (String2Enmu(FLAGS_convert_mode)) {
    case edgelistcsv2edgelistbin:
      if (FLAGS_sep == "") {
        LOG_ERROR("CSV separator is not empty. Use -sep [e.g. \",\"].");
        return -1;
      }
      ConvertEdgelist(FLAGS_i, FLAGS_o, FLAGS_sep, FLAGS_read_head);
      break;
    case edgelistcsv2csrbin:
      // TO ADD (hsiaoko): to add edgelist csv 2 csr bin function.
      break;
    case edgelistbin2csrbin:
      // TO ADD (hsiaoko): to add edgelist bin 2 csr bin function.
      break;
    default:
      LOG_INFO("Error convert mode.");
      break;
  }

  gflags::ShutDownCommandLineFlags();
  return 0;
}