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
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexLabel;
using sics::graph::core::data_structures::GraphMetadata;
using sics::graph::core::data_structures::SubgraphMetadata;
using sics::graph::core::util::atomic::WriteAdd;
using std::filesystem::create_directory;
using std::filesystem::exists;
using namespace sics::graph::tools::common;

DEFINE_string(partitioner, "", "partitioner type.");
DEFINE_string(i, "", "input path.");
DEFINE_string(o, "", "output path.");
DEFINE_uint64(n_partitions, 1, "the number of partitions");
DEFINE_string(store_strategy, "unconstrained",
              "graph-systems adopted three strategies to store edges: "
              "kUnconstrained, incoming, and outgoing.");
DEFINE_string(convert_mode, "", "Conversion mode");
DEFINE_string(sep, "", "separator to split csv file.");
DEFINE_bool(read_head, false, "whether to read header of csv.");

// @DESCRIPTION: convert a edgelist graph from csv file to binary file. Here the
// compression operations is default in ConvertEdgelist.
// @PARAMETER: input_path and output_path indicates the input and output path
// respectively, sep determines the separator for the csv file, read_head
// indicates whether to read head.
void ConvertEdgelistCSV2EdgelistBin(const std::string& input_path,
                                    const std::string& output_path,
                                    const std::string& sep,
                                    bool read_head) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();
  std::mutex mtx;

  if (!exists(output_path)) create_directory(output_path);
  std::ifstream in_file(input_path);
  if (!in_file) throw std::runtime_error("Open file failed: " + input_path);
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
                           &vid_map, &buffer_edges, &mtx]() {
      for (VertexID j = i; j < edges_vec.size(); j += parallelism) {
        buffer_edges[j] = edges_vec.at(j);
        std::lock_guard<std::mutex> lck(mtx);
        if (!bitmap.GetBit(edges_vec.at(j))) {
          auto local_vid = compressed_vid++;
          bitmap.SetBit(edges_vec.at(j));
          vid_map[edges_vec.at(j)] = local_vid;
        }
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  // Compress vid and buffer graph.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task =
        std::bind([i, parallelism, &buffer_edges, &edges_vec, &vid_map]() {
          for (VertexID j = i; j < edges_vec.size(); j += parallelism)
            buffer_edges[j] = vid_map[edges_vec.at(j)];
        });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  // Write binary edgelist
  out_data_file.write((char*)buffer_edges,
                      sizeof(VertexID) * 2 * edges_vec.size());

  // Write Meta date.
  YAML::Node node;
  node["EdgelistBin"]["num_vertices"] = bitmap.Count();
  node["EdgelistBin"]["num_edges"] = edges_vec.size() / 2;
  node["EdgelistBin"]["max_vid"] = compressed_vid - 1;
  out_meta_file << node << std::endl;

  delete buffer_edges;
  delete vid_map;
  in_file.close();
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
bool ConvertEdgelistBin2CSRBin(const std::string& input_path,
                               const std::string& output_path,
                               const StoreStrategy store_strategy) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);

  YAML::Node node = YAML::LoadFile(input_path + "meta.yaml");
  LOG_INFO(input_path + "meta.yaml");
  auto num_vertices = node["EdgelistBin"]["num_vertices"].as<VertexID>();
  auto num_edges = node["EdgelistBin"]["num_edges"].as<VertexID>();
  auto max_vid = node["EdgelistBin"]["max_vid"].as<VertexID>();
  auto aligned_max_vid = ((max_vid >> 6) << 6) + 64;

  auto buffer_edges = (VertexID*)malloc(sizeof(VertexID) * num_edges * 2);
  std::ifstream in_file(input_path + "edgelist.bin");
  if (!in_file)
    throw std::runtime_error("Open file failed: " + input_path +
                             "edgelist.bin");
  in_file.read((char*)buffer_edges, sizeof(VertexID) * 2 * num_edges);

  auto num_inedges_by_vid =
      (VertexID*)malloc(sizeof(VertexID) * aligned_max_vid);
  auto num_outedges_by_vid =
      (VertexID*)malloc(sizeof(VertexID) * aligned_max_vid);
  memset(num_inedges_by_vid, 0, sizeof(VertexID) * aligned_max_vid);
  memset(num_outedges_by_vid, 0, sizeof(VertexID) * aligned_max_vid);
  auto visited = Bitmap(aligned_max_vid);
  visited.Clear();

  // Traversal edges to get the num_in_edges and num_out_edges respectively
  auto task_package = TaskPackage();
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task =
        std::bind([i, parallelism, &num_edges, &buffer_edges,
                   &num_inedges_by_vid, &num_outedges_by_vid, &visited]() {
          for (VertexID j = i; j < num_edges; j += parallelism) {
            auto src = buffer_edges[j * 2];
            auto dst = buffer_edges[j * 2 + 1];
            visited.SetBit(src);
            visited.SetBit(dst);
            WriteAdd(num_inedges_by_vid + dst, (VertexID)1);
            WriteAdd(num_outedges_by_vid + src, (VertexID)1);
          }
        });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  auto edge_bucket = (VertexID**)malloc(sizeof(VertexID*));
  *edge_bucket = buffer_edges;

  GraphMetadata graph_metadata;
  graph_metadata.set_num_vertices(num_vertices);
  graph_metadata.set_num_edges(num_edges);
  graph_metadata.set_max_vid(max_vid);
  graph_metadata.set_min_vid(0);
  graph_metadata.set_num_subgraphs(1);

  std::vector<EdgelistMetadata> edgelist_metadata_vec;
  edgelist_metadata_vec.push_back({num_vertices, num_edges, max_vid});

  // Write the csr graph to disk
  GraphFormatConverter graph_format_converter(output_path);
  graph_format_converter.WriteSubgraph(edge_bucket,
                                       graph_metadata,
                                       edgelist_metadata_vec,
                                       store_strategy);
  return 0;
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