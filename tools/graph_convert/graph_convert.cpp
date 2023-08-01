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
#include "util/logging.h"

using sics::graph::core::common::Bitmap;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexLabel;
using sics::graph::core::util::atomic::WriteAdd;
using std::filesystem::create_directory;
using std::filesystem::exists;

enum StoreStrategy {
  kUnconstrained,  // default
  kIncomingOnly,
  kOutgoingOnly,
  kUndefinedStrategy
};

inline StoreStrategy String2EnumStoreStrategy(const std::string& s) {
  if (s == "incoming_only")
    return kIncomingOnly;
  else if (s == "outgoing_only")
    return kOutgoingOnly;
  else if (s == "unconstrained")
    return kUnconstrained;
  return kUndefinedStrategy;
};

enum ConvertMode {
  kEdgelistCSV2EdgelistBin,
  kEdgelistCSV2CSRBin,
  kEdgelistBin2CSRBin,
  kUndefinedMode
};

inline ConvertMode String2EnumConvertMode(const std::string& s) {
  if (s == "edgelistcsv2edgelistbin")
    return kEdgelistCSV2EdgelistBin;
  else if (s == "edgelistbin2csrbin")
    return kEdgelistBin2CSRBin;
  else if (s == "edgelistcsv2bin")
    return kEdgelistCSV2CSRBin;
  return kUndefinedMode;
};

DEFINE_string(convert_mode, "", "Conversion mode");
DEFINE_string(i, "", "input path.");
DEFINE_string(o, "", "output path.");
DEFINE_string(sep, "", "seperator to splite csv file.");
DEFINE_string(store_strategy, "kUnconstrained",
              "graph-systems adopted three strategies to store edges: "
              "kUnconstrained, incomming, and outgoing.");
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
  auto task_package = TaskPackage();

  if (!exists(output_path)) create_directory(output_path);
  std::ifstream in_file(input_path);
  if (!in_file)
    throw std::runtime_error("Open file failed: " + input_path);
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
    auto task = std::bind(
        [i, parallelism, &bitmap, &edges_vec, &compressed_vid, &vid_map]() {
          for (size_t j = i; j < edges_vec.size(); j += parallelism) {
            if (!bitmap.GetBit(edges_vec.at(j))) {
              auto local_vid = __sync_fetch_and_add(&compressed_vid, 1);
              bitmap.SetBit(edges_vec.at(j));
              vid_map[edges_vec.at(j)] = local_vid;
            }
          }
          return;
        });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  // Compress vid and buffer graph.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task =
        std::bind([i, parallelism, &buffer_edges, &edges_vec, &vid_map]() {
          for (size_t j = i; j < edges_vec.size(); j += parallelism)
            buffer_edges[j] = vid_map[edges_vec.at(j)];
          return;
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
bool ConvertEdgelistBin2CSRBin(const std::string& input_path,
                               const std::string& output_path,
                               const StoreStrategy store_strategy) {
  struct TMPCSRVertex {
    VertexID vid;
    size_t indegree = 0;
    size_t outdegree = 0;
    VertexID* in_edges = nullptr;
    VertexID* out_edges = nullptr;
  };

  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);

  YAML::Node node = YAML::LoadFile(input_path + "meta.yaml");
  auto num_vertices = node["edgelist_bin"]["num_vertices"].as<size_t>();
  auto num_edges = node["edgelist_bin"]["num_edges"].as<size_t>();
  auto max_vid = node["edgelist_bin"]["max_vid"].as<VertexID>();
  auto aligned_max_vid = ((max_vid >> 6) << 6) + 64;

  auto buffer_edges = (VertexID*)malloc(sizeof(VertexID) * num_edges * 2);
  std::ifstream in_file(input_path + "edgelist.bin");
  if (!in_file)
    throw std::runtime_error("Open file failed: " + input_path + "edgelist.bin");
  in_file.read((char*)buffer_edges, sizeof(VertexID) * 2 * num_edges);
  if (!exists(output_path)) create_directory(output_path);
  if (!exists(output_path + "graphs")) create_directory(output_path + "graphs");
  std::ofstream out_data_file(output_path + "graphs/0.bin");
  std::ofstream out_meta_file(output_path + "meta.yaml");

  auto num_inedges_by_vid = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  auto num_outedges_by_vid = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  memset(num_inedges_by_vid, 0, sizeof(size_t) * aligned_max_vid);
  memset(num_outedges_by_vid, 0, sizeof(size_t) * aligned_max_vid);
  auto visited = Bitmap(aligned_max_vid);
  visited.Clear();

  // Traversal edges to get the num_in_edges and num_out_edges respectively
  auto task_package = TaskPackage();
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task =
        std::bind([i, parallelism, &num_edges, &buffer_edges,
                   &num_inedges_by_vid, &num_outedges_by_vid, &visited]() {
          for (size_t j = i; j < num_edges; j += parallelism) {
            auto src = buffer_edges[j * 2];
            auto dst = buffer_edges[j * 2 + 1];
            visited.SetBit(src);
            visited.SetBit(dst);
            WriteAdd(num_inedges_by_vid + src, (size_t)1);
            WriteAdd(num_outedges_by_vid + dst, (size_t)1);
          }
          return;
        });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  TMPCSRVertex* buffer_csr_vertices =
      (TMPCSRVertex*)malloc(sizeof(TMPCSRVertex) * aligned_max_vid);
  size_t count_in_edges = 0, count_out_edges = 0;

  // malloc space for each vertex.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &aligned_max_vid, &buffer_edges,
                           &num_inedges_by_vid, &num_outedges_by_vid,
                           &buffer_csr_vertices, &count_in_edges,
                           &count_out_edges, &visited]() {
      for (size_t j = i; j < aligned_max_vid; j += parallelism) {
        if (!visited.GetBit(j)) continue;
        auto u = TMPCSRVertex();
        u.vid = j;
        u.indegree = num_inedges_by_vid[j];
        u.outdegree = num_outedges_by_vid[j];
        u.in_edges = (VertexID*)malloc(sizeof(VertexID) * u.indegree);
        u.out_edges = (VertexID*)malloc(sizeof(VertexID) * u.outdegree);
        WriteAdd(&count_in_edges, u.indegree);
        WriteAdd(&count_out_edges, u.outdegree);
        buffer_csr_vertices[j] = u;
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete num_inedges_by_vid;
  delete num_outedges_by_vid;

  // Fill edges.
  size_t* offset_in_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  size_t* offset_out_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  memset(offset_in_edges, 0, sizeof(size_t) * aligned_max_vid);
  memset(offset_out_edges, 0, sizeof(size_t) * aligned_max_vid);
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task =
        std::bind([i, parallelism, &num_edges, &buffer_edges, &offset_in_edges,
                   &offset_out_edges, &buffer_csr_vertices]() {
          for (size_t j = i; j < num_edges; j += parallelism) {
            auto src = buffer_edges[j * 2];
            auto dst = buffer_edges[j * 2 + 1];
            auto offset_out = __sync_fetch_and_add(offset_out_edges + src, 1);
            auto offset_in = __sync_fetch_and_add(offset_in_edges + dst, 1);
            buffer_csr_vertices[src].out_edges[offset_out] = dst;
            buffer_csr_vertices[dst].in_edges[offset_in] = src;
          }
          return;
        });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  // Construct CSR graph.
  auto buffer_globalid = (VertexID*)malloc(sizeof(VertexID) * num_vertices);
  auto buffer_indegree = (size_t*)malloc(sizeof(size_t) * num_vertices);
  auto buffer_outdegree = (size_t*)malloc(sizeof(size_t) * num_vertices);

  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &num_vertices, &buffer_globalid,
                           &buffer_indegree, &buffer_outdegree,
                           &buffer_csr_vertices]() {
      for (size_t j = i; j < num_vertices; j += parallelism) {
        buffer_globalid[j] = buffer_csr_vertices[j].vid;
        buffer_indegree[j] = buffer_csr_vertices[j].indegree;
        buffer_outdegree[j] = buffer_csr_vertices[j].outdegree;
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  // Write globalid_by_index, indegree and outdgree buffer.
  out_data_file.write((char*)buffer_globalid, sizeof(VertexID) * num_vertices);
  delete buffer_globalid;

  auto buffer_vdata = (VertexLabel*)malloc(sizeof(VertexLabel) * num_vertices);
  memset(buffer_vdata, 0, sizeof(VertexLabel) * num_vertices);
  out_data_file.write((char*)buffer_vdata, sizeof(VertexLabel) * num_vertices);
  delete buffer_vdata;

  switch (store_strategy) {
    case kOutgoingOnly:
      out_data_file.write((char*)buffer_outdegree,
                          sizeof(size_t) * num_vertices);
      break;
    case kIncomingOnly:
      out_data_file.write((char*)buffer_indegree,
                          sizeof(size_t) * num_vertices);
      break;
    case kUnconstrained:
      out_data_file.write((char*)buffer_indegree,
                          sizeof(size_t) * num_vertices);
      out_data_file.write((char*)buffer_outdegree,
                          sizeof(size_t) * num_vertices);
      break;
    default:
      LOG_ERROR("Undefined store strategy.");
      return -1;
  }

  auto buffer_in_offset = (size_t*)malloc(sizeof(size_t) * num_vertices);
  auto buffer_out_offset = (size_t*)malloc(sizeof(size_t) * num_vertices);
  memset(buffer_in_offset, 0, sizeof(size_t) * num_vertices);
  memset(buffer_out_offset, 0, sizeof(size_t) * num_vertices);

  // Compute offset for each vertex.
  for (size_t i = 1; i < num_vertices; i++) {
    buffer_in_offset[i] = buffer_in_offset[i - 1] + buffer_indegree[i - 1];
    buffer_out_offset[i] = buffer_out_offset[i - 1] + buffer_outdegree[i - 1];
  }

  delete buffer_indegree;
  delete buffer_outdegree;

  // Write offset buffer.
  switch (store_strategy) {
    case kOutgoingOnly:
      out_data_file.write((char*)buffer_out_offset,
                          sizeof(size_t) * num_vertices);
      break;
    case kIncomingOnly:
      out_data_file.write((char*)buffer_in_offset,
                          sizeof(size_t) * num_vertices);
      break;
    case kUnconstrained:
      out_data_file.write((char*)buffer_in_offset,
                          sizeof(size_t) * num_vertices);
      out_data_file.write((char*)buffer_out_offset,
                          sizeof(size_t) * num_vertices);
      break;
    default:
      LOG_ERROR("Undefined store strategy.");
      return -1;
  }

  // Buffer compacted edges.
  auto buffer_in_edges = (VertexID*)malloc(sizeof(VertexID) * count_in_edges);
  auto buffer_out_edges = (VertexID*)malloc(sizeof(VertexID) * count_out_edges);
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([i, parallelism, &num_vertices, &buffer_in_edges,
                           &buffer_out_edges, &buffer_in_offset,
                           &buffer_out_offset, &buffer_csr_vertices]() {
      for (size_t j = i; j < num_vertices; j += parallelism) {
        memcpy(buffer_in_edges + buffer_in_offset[j],
               buffer_csr_vertices[j].in_edges,
               buffer_csr_vertices[j].indegree * sizeof(VertexID));
        memcpy(buffer_out_edges + buffer_out_offset[j],
               buffer_csr_vertices[j].out_edges,
               buffer_csr_vertices[j].outdegree * sizeof(VertexID));
      }
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete buffer_in_offset;
  delete buffer_out_offset;

  // Write edges buffers.
  switch (store_strategy) {
    case kOutgoingOnly:
      out_data_file.write((char*)buffer_out_edges,
                          sizeof(VertexID) * count_out_edges);
      break;
    case kIncomingOnly:
      out_data_file.write((char*)buffer_in_edges,
                          sizeof(VertexID) * count_in_edges);
      break;
    case kUnconstrained:
      out_data_file.write((char*)buffer_out_edges,
                          sizeof(VertexID) * count_out_edges);
      out_data_file.write((char*)buffer_in_edges,
                          sizeof(VertexID) * count_in_edges);
      break;
    default:
      LOG_ERROR("Undefined store strategy.");
      return -1;
  }
  delete buffer_in_edges;
  delete buffer_out_edges;

  // Write Meta date.
  YAML::Node out_node;
  out_node["csr_bin"]["num_vertices"] = num_vertices;
  switch (store_strategy) {
    case kOutgoingOnly:
      out_node["csr_bin"]["num_incomming_edges"] = 0;
      out_node["csr_bin"]["num_outgoing_edges"] = count_out_edges;
      break;
    case kIncomingOnly:
      out_node["csr_bin"]["num_outgoing_edges"] = 0;
      out_node["csr_bin"]["num_incomming_edges"] = count_in_edges;
      break;
    case kUnconstrained:
      out_node["csr_bin"]["num_outgoing_edges"] = count_out_edges;
      out_node["csr_bin"]["num_incomming_edges"] = count_in_edges;
      break;
    default:
      LOG_ERROR("Undefined store strategy.");
      return -1;
  }
  out_node["csr_bin"]["max_vid"] = max_vid;
  out_node["csr_bin"]["num_subgraphs"] = 1;
  out_meta_file << out_node << std::endl;

  YAML::Node subgraph_node;
  subgraph_node["subgraph"]["gid"] = 0;
  subgraph_node["subgraph"]["num_vertices"] = num_vertices;
  switch (store_strategy) {
    case kOutgoingOnly:
      subgraph_node["subgraph"]["num_incomming_edges"] = 0;
      subgraph_node["subgraph"]["num_outgoing_edges"] = count_out_edges;
      break;
    case kIncomingOnly:
      subgraph_node["subgraph"]["num_outgoing_edges"] = 0;
      subgraph_node["subgraph"]["num_incomming_edges"] = count_in_edges;
      break;
    case kUnconstrained:
      subgraph_node["subgraph"]["num_outgoing_edges"] = count_out_edges;
      subgraph_node["subgraph"]["num_incomming_edges"] = count_in_edges;
      break;
    default:
      LOG_ERROR("Undefined store strategy.");
      return -1;
  }
  subgraph_node["subgraph"]["max_vid"] = max_vid;
  out_meta_file << subgraph_node << std::endl;

  in_file.close();
  out_meta_file.close();
  out_data_file.close();
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
  if (FLAGS_i == "" || FLAGS_o == "") {
    LOG_ERROR("Input (output) path is empty.");
    return -1;
  }

  switch (String2EnumConvertMode(FLAGS_convert_mode)) {
    case kEdgelistCSV2EdgelistBin:
      if (FLAGS_sep == "") {
        LOG_ERROR("CSV separator is not empty. Use -sep [e.g. \",\"].");
        return -1;
      }
      ConvertEdgelist(FLAGS_i, FLAGS_o, FLAGS_sep, FLAGS_read_head);
      break;
    case kEdgelistCSV2CSRBin:
      // TODO(hsiaoko): to add edgelist csv 2 csr bin function.
      break;
    case kEdgelistBin2CSRBin:
      ConvertEdgelistBin2CSRBin(FLAGS_i, FLAGS_o,
                                String2EnumStoreStrategy(FLAGS_store_strategy));
      break;
    default:
      LOG_INFO("Error convert mode.");
      break;
  }

  gflags::ShutDownCommandLineFlags();
  return 0;
}