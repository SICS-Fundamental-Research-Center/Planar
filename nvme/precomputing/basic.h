#ifndef GRAPH_SYSTEMS_NVME_PRECOMPUTING_BASIC_H_
#define GRAPH_SYSTEMS_NVME_PRECOMPUTING_BASIC_H_

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "core/data_structures/graph_metadata.h"
#include "nvme/data_structures/graph/block_csr_graph.h"

namespace sics::graph::nvme::precomputing {

using sics::graph::core::common::BlockID;
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::VertexDegree;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexIndex;

namespace fs = std::filesystem;

struct BlockWithHopInfo {
  BlockWithHopInfo(std::string root_path,
                   core::data_structures::BlockMetadata* meta) {
    graph_.Init(root_path, meta);
  }

  void Read(const std::string& path) {
    // init two_hop_neighbors_
    graph_.ReadAllSubBlock();
    auto num_vertices = graph_.GetVerticesNum();
    min_one_hop_neighbor_ = new VertexID[num_vertices];
    max_one_hop_neighbor_ = new VertexID[num_vertices];
    min_two_hop_neighbor_ = new VertexID[num_vertices];
    max_two_hop_neighbor_ = new VertexID[num_vertices];
    has_two_hop_neighbor_.resize(num_vertices);
    for (VertexIndex i = 0; i < num_vertices; i++) {
      min_one_hop_neighbor_[i] = MAX_VERTEX_ID;
      max_one_hop_neighbor_[i] = i;
      min_two_hop_neighbor_[i] = MAX_VERTEX_ID;
      max_two_hop_neighbor_[i] = i;
      has_two_hop_neighbor_[i] = false;
    }
  }

  void Write(const std::string& root_path) {
    auto num_vertices = graph_.GetVerticesNum();
    for (VertexIndex i = 0; i < num_vertices; i++) {
      if (graph_.GetOutDegree(i) == 0) {
        min_one_hop_neighbor_[i] = i;
        max_one_hop_neighbor_[i] = i;
      }
      if (!has_two_hop_neighbor_[i]) {
        min_two_hop_neighbor_[i] = i;
        max_two_hop_neighbor_[i] = i;
      }
    }

    std::ofstream min_one_hop_file(
        root_path + "precomputing/one_hop_min_id.bin", std::ios::binary);
    min_one_hop_file
        .write((char*)min_one_hop_neighbor_, num_vertices * sizeof(VertexID))
        .flush();
    min_one_hop_file.close();
    std::ofstream max_one_hop_file(
        root_path + "precomputing/one_hop_max_id.bin", std::ios::binary);
    max_one_hop_file
        .write((char*)max_one_hop_neighbor_, num_vertices * sizeof(VertexID))
        .flush();
    max_one_hop_file.close();
    std::ofstream min_two_hop_file(
        root_path + "precomputing/two_hop_min_id.bin", std::ios::binary);
    min_two_hop_file
        .write((char*)min_two_hop_neighbor_, num_vertices * sizeof(VertexID))
        .flush();
    min_two_hop_file.close();
    std::ofstream max_two_hop_file(
        root_path + "precomputing/two_hop_max_id.bin", std::ios::binary);
    max_two_hop_file
        .write((char*)max_two_hop_neighbor_, num_vertices * sizeof(VertexID))
        .flush();
    max_two_hop_file.close();

    delete[] min_one_hop_neighbor_;
    min_one_hop_neighbor_ = nullptr;
    delete[] max_one_hop_neighbor_;
    max_one_hop_neighbor_ = nullptr;
    delete[] min_two_hop_neighbor_;
    min_two_hop_neighbor_ = nullptr;
    delete[] max_two_hop_neighbor_;
    max_two_hop_neighbor_ = nullptr;
  }

  ~BlockWithHopInfo() {
    if (min_one_hop_neighbor_) delete[] min_one_hop_neighbor_;
    if (max_one_hop_neighbor_) delete[] max_one_hop_neighbor_;
    if (min_two_hop_neighbor_) delete[] min_two_hop_neighbor_;
    if (max_two_hop_neighbor_) delete[] max_two_hop_neighbor_;
  }

  void AddNeighbor(VertexID id, VertexID neighbor) {}

  void UpdateOneHopInfo(VertexID id, VertexID one_hop_neighbor_id) {
    min_one_hop_neighbor_[id] =
        std::min(min_one_hop_neighbor_[id], one_hop_neighbor_id);
    max_one_hop_neighbor_[id] =
        std::max(max_one_hop_neighbor_[id], one_hop_neighbor_id);
  }

  void UpdateTwoHopInfo(VertexID id, VertexID two_hop_nerighbor_id) {
    //    if (two_hop_nerighbor_id == id) return;
    //    if (CheckIsOneHop(id, two_hop_nerighbor_id)) return;
    min_two_hop_neighbor_[id] =
        std::min(min_two_hop_neighbor_[id], two_hop_nerighbor_id);
    max_two_hop_neighbor_[id] =
        std::max(max_two_hop_neighbor_[id], two_hop_nerighbor_id);
    has_two_hop_neighbor_[id] = true;
  }

  bool CheckIsOneHop(VertexID id, VertexID neighbor) {
    auto degree = graph_.GetOutDegree(id);
    if (degree == 0) return false;
    auto edges = graph_.GetOutEdges(id);
    for (VertexDegree i = 0; i < degree; i++) {
      if (edges[i] == neighbor) return true;
    }
    return false;
  }

 public:
  data_structures::graph::BlockCSRGraph graph_;
  // two hop info
  VertexID* min_one_hop_neighbor_ = nullptr;
  VertexID* max_one_hop_neighbor_ = nullptr;
  VertexID* min_two_hop_neighbor_ = nullptr;
  VertexID* max_two_hop_neighbor_ = nullptr;
  std::vector<bool> has_two_hop_neighbor_;
};

}  // namespace sics::graph::nvme::precomputing

namespace YAML {

using GraphID = sics::graph::core::common::GraphID;
using BlockID = sics::graph::core::common::BlockID;
using VertexID = sics::graph::core::common::VertexID;
using EdgeIndex = sics::graph::core::common::EdgeIndex;

}  // namespace YAML

#endif  // GRAPH_SYSTEMS_NVME_PRECOMPUTING_BASIC_H_
