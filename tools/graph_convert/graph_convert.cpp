// This file belongs to the SICS graph-systems project, a C++ library for
// exploiting parallelism graph computing.
//
// We store graphs in a binary format. Other formats such as edge-list can be
// converted to our format with graph_convert tool. You can use graph_convert as
// follows: e.g. convert a graph from edgelist CSV to edgelist bin file.
// USAGE: graph-convert --convert_mode=[options] -i <input file path> -o <output
// file path> --sep=[separator]

#include <filesystem>
#include <fstream>
#include <iostream>
#include <type_traits>

#include <gflags/gflags.h>
#include <yaml-cpp/yaml.h>

#include "common/bitmap.h"
#include "common/multithreading/thread_pool.h"
#include "common/types.h"
#include "util/atomic.h"
#include "util/file.h"
#include "util/logging.h"

using sics::graph::core::common::Bitmap;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::file::Exist;
using sics::graph::core::util::file::MakeDirectory;
using std::filesystem::create_directory;
using std::filesystem::exists;

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

// @DESCRIPTION: convert a edgelist graph from csv file to binery file. Here the
// compression operations is default in ConvertEdgelist.
// @PARAMETER: input_path and output_path indicates the input and output path
// respectively, sep determines the separator for the csv file, read_head
// indicates whether to read head.
void ConvertEdgelist(const std::string& input_path,
                     const std::string& output_path,
                     const std::string& sep,
                     bool read_head) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(parallelism);

  if (!exists(output_path)) create_directory(output_path);
  std::ifstream in_file(input_path);
  std::ofstream out_data_file(output_path + "edgelist.bin");
  std::ofstream out_meta_file(output_path + "meta.yaml");

  // Read edgelist graph.
  std::vector<VertexID> edges_vec;
  edges_vec.reserve(1048576);
  VertexID max_vid = 0, compressed_vid = 0;
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

  // Compute the mapping between origin vid to compressed vid.
  auto aligned_max_vid = ((max_vid >> 6) << 6) + 64;
  auto bitmap = Bitmap(aligned_max_vid);
  bitmap.Clear();
  auto buffer_edges =
      (VertexID*)malloc(sizeof(VertexID) * edges_vec.size() * 2);
  memset(buffer_edges, 0, sizeof(VertexID) * edges_vec.size() * 2);
  auto vid_map = (VertexID*)malloc(sizeof(VertexID) * aligned_max_vid);
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &bitmap, &edges_vec, &compressed_vid,
                           &vid_map, &pending_packages, &finish_cv, &mtx]() {
      for (size_t j = i; j < edges_vec.size(); j += parallelism) {
        if (!bitmap.GetBit(edges_vec.at(j))) {
          auto local_vid = __sync_fetch_and_add(&compressed_vid, 1);
          bitmap.SetBit(edges_vec.at(j));
          vid_map[edges_vec.at(j)] = local_vid;
        }
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
    thread_pool.SubmitAsync(task);
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  // Compress vid and buffer graph.
  pending_packages.store(parallelism);
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &buffer_edges, &edges_vec, &vid_map,
                           &pending_packages, &finish_cv]() {
      for (size_t j = i; j < edges_vec.size(); j += parallelism)
        buffer_edges[j] = vid_map[edges_vec.at(j)];

      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
    thread_pool.SubmitAsync(task);
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  // Write binary edgelist
  out_data_file.write((char*) buffer_edges,
                      sizeof(VertexID) * 2 * edges_vec.size());

  // Write Meta date.
  YAML::Node node;
  node["edgelist_bin"]["num_vertices"] = bitmap.Count();
  node["edgelist_bin"]["num_edges"] = edges_vec.size() / 2;
  node["edgelist_bin"]["max_vid"] = compressed_vid - 1;
  out_meta_file << node << std::endl;

  delete buffer_edges;
  delete vid_map;
  in_file.close();
  out_data_file.close();
  out_meta_file.close();
}

// @DESCRIPTION: convert a binary edgelist graph to binery CSR.
// @PARAMETER: input_path and output_path indicates the input and output path
// respectively.
void ConvertEdgelistBin2CSRBin(const std::string& input_path,
                               const std::string& output_path) {

  // TODO(hsiaoko): not finished yet.
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
      // TODO(hsiaoko): to add edgelist csv 2 csr bin function.
      break;
    case edgelistbin2csrbin:
      ConvertEdgelistBin2CSRBin(FLAGS_i, FLAGS_o);
      break;
    default:
      LOG_INFO("Error convert mode.");
      break;
  }

  gflags::ShutDownCommandLineFlags();
  return 0;
}
