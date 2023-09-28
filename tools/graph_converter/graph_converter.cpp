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

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/common/types.h"
#include "core/data_structures/graph_metadata.h"
#include "core/util/atomic.h"
#include "core/util/logging.h"
#include "tools/common/data_structures.h"
#include "tools/common/io.h"
#include "tools/common/yaml_config.h"

using sics::graph::core::common::Bitmap;
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexLabel;
using sics::graph::core::data_structures::GraphMetadata;
using sics::graph::core::data_structures::SubgraphMetadata;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::tools::common::Edge;
using sics::graph::tools::common::Edges;
using std::filesystem::create_directory;
using std::filesystem::exists;
using namespace sics::graph::tools::common;

DEFINE_string(partitioner, "", "partitioner type.");
DEFINE_string(i, "", "input path.");
DEFINE_string(o, "", "output path.");
DEFINE_uint64(n_partitions, 1, "the number of partitions");
DEFINE_uint64(n_edges, UINT64_MAX, "the number of edges to read");
DEFINE_string(store_strategy, "unconstrained",
              "graph-systems adopted three strategies to store edges: "
              "kUnconstrained, incoming, and outgoing.");
DEFINE_string(convert_mode, "", "Conversion mode");
DEFINE_string(sep, "", "separator to split a line of csv file.");
DEFINE_bool(read_head, false, "whether to read header of csv.");
DEFINE_bool(biggraph, false, "for big graphs.");

// @DESCRIPTION: convert a edgelist graph from csv file to binary file. Here the
// compression operations is default in ConvertEdgelist.
// @PARAMETER: input_path and output_path indicates the input and output path
// respectively, sep determines the separator for the csv file, read_head
// indicates whether to read head.
void ConvertEdgelistCSV2EdgelistBin(const std::string& input_path,
                                    const std::string& output_path,
                                    const std::string& sep, bool read_head) {
  LOG_INFO("ConvertEdgelistCSV2EdgelistBin");
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();
  task_package.reserve(parallelism);

  if (!exists(output_path)) create_directory(output_path);
  std::ifstream in_file(input_path);
  if (!in_file) LOG_FATAL("File not found!");
  std::ofstream out_data_file(output_path + "edgelist.bin");
  std::ofstream out_meta_file(output_path + "meta.yaml");

  in_file.seekg(0, std::ios::end);
  size_t length = in_file.tellg();
  in_file.seekg(0, std::ios::beg);

  char* buff = new char[length]();
  in_file.read(buff, length);
  std::string content(buff, length);

  EdgeIndex n_edges = count(content.begin(), content.end(), '\n');
  auto buffer_edges = new VertexID[n_edges * 2]();
  std::stringstream ss(content);
  delete[] buff;

  EdgeIndex index = 0;
  VertexID max_vid = 0, compressed_vid = 0;
  std::string line, vid_str;
  if (read_head) getline(ss, line, '\n');
  while (getline(ss, line, '\n')) {
    if (*line.c_str() == '\0') break;
    std::stringstream ss_line(line);
    while (getline(ss_line, vid_str, *sep.c_str())) {
      VertexID vid = stoll(vid_str);
      sics::graph::core::util::atomic::WriteMax(&max_vid, vid);
      buffer_edges[index++] = vid;
    }
  }
  content.clear();
  in_file.close();

  auto aligned_max_vid = (((max_vid + 1) >> 6) << 6) + 64;
  auto vid_map = new VertexID[aligned_max_vid]();
  Bitmap bitmap(aligned_max_vid);

  // Compute the mapping between origin vid to compressed vid.
  for (EdgeIndex index = 0; index < n_edges * 2; index++) {
    if (!bitmap.GetBit(buffer_edges[index])) {
      bitmap.SetBit(buffer_edges[index]);
      vid_map[buffer_edges[index]] = compressed_vid++;
    }
  }

  auto compressed_buffer_edges = new VertexID[n_edges * 2]();

  // Compress vid and buffer graph.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (EdgeIndex j = i; j < n_edges * 2; j += parallelism)
        compressed_buffer_edges[j] = vid_map[buffer_edges[j]];
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete[] buffer_edges;
  delete[] vid_map;

  // Write binary edgelist
  out_data_file.write(reinterpret_cast<char*>(compressed_buffer_edges),
                      sizeof(VertexID) * 2 * n_edges);
  delete[] compressed_buffer_edges;

  // Write Meta date.
  YAML::Node node;
  node["EdgelistBin"]["num_vertices"] = bitmap.Count();
  node["EdgelistBin"]["num_edges"] = n_edges;
  node["EdgelistBin"]["max_vid"] = compressed_vid - 1;
  out_meta_file << node << std::endl;

  out_data_file.close();
  out_meta_file.close();
}

// @DESCRIPTION: convert a edgelist graph from csv file to binary file. Here the
// compression operations is default in ConvertEdgelist. We strongly recommend
// users to used this function if they know how many edges to read.
// @PARAMETER: input_path and output_path indicates the input and output path
// respectively, sep determines the separator for the csv file, read_head
// indicates whether to read head.
void BigGraphConvertEdgelistCSV2EdgelistBin(const std::string& input_path,
                                            const std::string& output_path,
                                            const std::string& sep,
                                            EdgeIndex n_edges, bool read_head) {
  if (n_edges == UINT64_MAX) LOG_FATAL("n_edges should be specified.");
  LOG_INFO("Read ", n_edges, " edges.", n_edges * 2);

  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();

  if (!exists(output_path)) create_directory(output_path);
  std::ifstream in_file(input_path);
  if (!in_file) LOG_FATAL("File not found!");

  std::ofstream out_data_file(output_path + "edgelist.bin");
  std::ofstream out_meta_file(output_path + "meta.yaml");

  // Read edgelist graph.
  VertexID max_vid = 0, compressed_vid = 0;
  std::string line, vid_str;
  auto buffer_edges = new VertexID[n_edges * 2]();
  EdgeIndex index = 0;
  if (read_head) getline(in_file, line);
  while (getline(in_file, line)) {
    std::stringstream ss(line);
    if (index > (n_edges - 1) * 2) break;
    while (getline(ss, vid_str, *sep.c_str())) {
      VertexID vid = stoll(vid_str);
      sics::graph::core::util::atomic::WriteMax(&max_vid, vid);
      *(buffer_edges + index++) = vid;
    }
  }
  in_file.close();

  if (index < n_edges) n_edges = index;

  auto aligned_max_vid = (((max_vid + 1) >> 6) << 6) + 64;
  auto vid_map = new VertexID[aligned_max_vid]();
  auto bitmap = new Bitmap(aligned_max_vid);

  for (index = 0; index < n_edges * 2; index++) {
    if (!bitmap->GetBit(buffer_edges[index])) {
      bitmap->SetBit(buffer_edges[index]);
      vid_map[buffer_edges[index]] = compressed_vid++;
    }
  }

  auto compressed_buffer_edges = new VertexID[n_edges * 2]();
  // Compress vid and buffer graph.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i, parallelism]() {
      for (EdgeIndex j = i; j < n_edges * 2; j += parallelism) {
        compressed_buffer_edges[j] = vid_map[buffer_edges[j]];
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete[] buffer_edges;
  delete[] vid_map;

  // Write binary edgelist
  out_data_file.write(reinterpret_cast<char*>(compressed_buffer_edges),
                      sizeof(VertexID) * 2 * n_edges);
  delete[] compressed_buffer_edges;

  // Write Meta date.
  YAML::Node node;
  node["EdgelistBin"]["num_vertices"] = bitmap->Count();
  node["EdgelistBin"]["num_edges"] = n_edges;
  node["EdgelistBin"]["max_vid"] = compressed_vid - 1;
  out_meta_file << node << std::endl;

  delete bitmap;
  out_data_file.close();
  out_meta_file.close();
}

// @DESCRIPTION: convert a binary edgelist graph to binary CSR.
// @PARAMETER: input_path and output_path indicates the input and output path
// respectively. store_strategy indicates different store strategy where
// unconstrained store both outgoing and incoming edges of a vertex,
// incoming_only store incoming edges only, and outgoing_only store outgoing
// edges of a vertex only. By default, the graph is store in unconstrained
// manner.
void ConvertEdgelistBin2CSRBin(const std::string& input_path,
                               const std::string& output_path,
                               const StoreStrategy store_strategy) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);

  YAML::Node node = YAML::LoadFile(input_path + "meta.yaml");
  LOG_INFO(input_path + "meta.yaml");

  EdgelistMetadata edgelist_metadata = {
      node["EdgelistBin"]["num_vertices"].as<VertexID>(),
      node["EdgelistBin"]["num_edges"].as<VertexID>(),
      node["EdgelistBin"]["max_vid"].as<VertexID>()};

  auto buffer_edges = new Edge[edgelist_metadata.num_edges]();

  std::ifstream in_file(input_path + "edgelist.bin");
  if (!in_file) LOG_FATAL("Open file failed: " + input_path + "edgelist.bin");
  in_file.read(reinterpret_cast<char*>(buffer_edges),
               sizeof(Edge) * edgelist_metadata.num_edges);

  Edges edges(edgelist_metadata, buffer_edges);
  edges.SortBySrc();

  GraphMetadata graph_metadata;
  graph_metadata.set_num_vertices(edgelist_metadata.num_vertices);
  graph_metadata.set_num_edges(edgelist_metadata.num_edges);
  graph_metadata.set_max_vid(edgelist_metadata.max_vid);
  graph_metadata.set_min_vid(0);
  graph_metadata.set_num_subgraphs(1);

  // Write the csr graph to disk
  GraphFormatConverter graph_format_converter(output_path);
  std::vector<Edges> edge_buckets;
  edge_buckets.push_back(edges);
  graph_format_converter.WriteSubgraph(edge_buckets, graph_metadata,
                                       store_strategy);
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
    LOG_FATAL("Input (output) path is empty.");

  switch (ConvertMode2Enum(FLAGS_convert_mode)) {
    case kEdgelistCSV2EdgelistBin:
      if (FLAGS_sep == "")
        LOG_FATAL("CSV separator is not empty. Use -sep [e.g. \",\"].");
      if (FLAGS_biggraph)
        BigGraphConvertEdgelistCSV2EdgelistBin(FLAGS_i, FLAGS_o, FLAGS_sep,
                                               FLAGS_n_edges, FLAGS_read_head);
      else
        ConvertEdgelistCSV2EdgelistBin(FLAGS_i, FLAGS_o, FLAGS_sep,
                                       FLAGS_read_head);
      break;
    case kEdgelistCSV2CSRBin:
      // TODO(hsiaoko): to add edgelist csv 2 csr bin function.
      break;
    case kEdgelistBin2CSRBin:
      ConvertEdgelistBin2CSRBin(FLAGS_i, FLAGS_o,
                                StoreStrategy2Enum(FLAGS_store_strategy));
      break;
    default:
      LOG_FATAL("Error convert mode.");
  }

  gflags::ShutDownCommandLineFlags();
  return 0;
}