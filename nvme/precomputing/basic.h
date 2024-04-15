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

namespace sics::graph::nvme::precomputing {

using sics::graph::core::common::BlockID;
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::VertexDegree;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexIndex;

namespace fs = std::filesystem;

struct Block {
  Block(uint32_t num_vertices, EdgeIndex num_edges, VertexID bid, VertexID eid)
      : num_vertices_(num_vertices),
        num_edges_(num_edges),
        bid_(bid),
        eid_(eid) {}
  void Read(const std::string& path) {
    if (isRead_) return;
    degree_ = new VertexID[num_vertices_];
    offset_ = new EdgeIndex[num_vertices_];
    edges_ = new VertexID[num_edges_];
    std::ifstream datafile(path, std::ios::binary);
    datafile.read(reinterpret_cast<char*>(degree_),
                  num_vertices_ * sizeof(VertexID));
    datafile.read(reinterpret_cast<char*>(offset_),
                  num_vertices_ * sizeof(EdgeIndex));
    datafile.read(reinterpret_cast<char*>(edges_),
                  num_edges_ * sizeof(VertexID));
    // init two_hop_neighbors_
    min_one_hop_neighbor_ = new VertexID[num_vertices_];
    max_one_hop_neighbor_ = new VertexID[num_vertices_];
    min_two_hop_neighbor_ = new VertexID[num_vertices_];
    max_two_hop_neighbor_ = new VertexID[num_vertices_];
    has_two_hop_neighbor_.resize(num_vertices_);
    for (auto i = 0; i < num_vertices_; i++) {
      min_one_hop_neighbor_[i] = MAX_VERTEX_ID;
      max_one_hop_neighbor_[i] = 0;
      min_two_hop_neighbor_[i] = MAX_VERTEX_ID;
      max_two_hop_neighbor_[i] = 0;
      has_two_hop_neighbor_[i] = false;
    }
    isRead_ = true;
  }
  void Write(const std::string& root_path,
             core::common::ThreadPool* pool = nullptr) {
    for (auto i = 0; i < num_vertices_; i++) {
      if (GetDegree(i) == 0) {
        min_one_hop_neighbor_[i] = i;
        max_one_hop_neighbor_[i] = MAX_VERTEX_ID;
      }
      if (!has_two_hop_neighbor_[i]) {
        min_two_hop_neighbor_[i] = i;
        max_two_hop_neighbor_[i] = MAX_VERTEX_ID;
      }
    }

    std::ofstream min_one_hop_file(root_path + "precomputing/one_hop_min.bin",
                                   std::ios::binary);
    min_one_hop_file
        .write(reinterpret_cast<char*>(min_one_hop_neighbor_),
               num_vertices_ * sizeof(VertexID))
        .flush();
    min_one_hop_file.close();
    std::ofstream max_one_hop_file(root_path + "precomputing/one_hop_max.bin",
                                   std::ios::binary);
    max_one_hop_file
        .write(reinterpret_cast<char*>(max_one_hop_neighbor_),
               num_vertices_ * sizeof(VertexID))
        .flush();
    max_one_hop_file.close();
    std::ofstream min_two_hop_file(root_path + "precomputing/two_hop_min.bin",
                                   std::ios::binary);
    min_two_hop_file
        .write(reinterpret_cast<char*>(min_two_hop_neighbor_),
               num_vertices_ * sizeof(VertexID))
        .flush();
    min_two_hop_file.close();
    std::ofstream max_two_hop_file(root_path + "precomputing/two_hop_max.bin",
                                   std::ios::binary);
    max_two_hop_file
        .write(reinterpret_cast<char*>(max_two_hop_neighbor_),
               num_vertices_ * sizeof(VertexID))
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

  void Release() {
    if (degree_) {
      delete[] degree_;
      degree_ = nullptr;
    }
    if (offset_) {
      delete[] offset_;
      offset_ = nullptr;
    }
    if (edges_) {
      delete[] edges_;
      edges_ = nullptr;
    }
    isRead_ = false;
  }
  ~Block() {
    if (degree_) delete[] degree_;
    if (offset_) delete[] offset_;
    if (edges_) delete[] edges_;
    if (min_one_hop_neighbor_) delete[] min_one_hop_neighbor_;
    if (max_one_hop_neighbor_) delete[] max_one_hop_neighbor_;
    if (min_two_hop_neighbor_) delete[] min_two_hop_neighbor_;
    if (max_two_hop_neighbor_) delete[] max_two_hop_neighbor_;
  }

  VertexID* GetEdges(VertexID idx) { return edges_ + offset_[idx]; }
  VertexID* GetEdgesByID(VertexID id) {
    if (id < bid_ || id >= eid_) {
      LOG_FATAL("id is not in the block");
    }
    return edges_ + offset_[id - bid_];
  }
  VertexDegree GetDegree(VertexID idx) { return degree_[idx]; }
  VertexDegree GetDegreeByID(VertexID id) { return degree_[id - bid_]; }

  void AddNeighbor(VertexID id, VertexID neighbor) {}

  void UpdateOneHopInfo(VertexID id, VertexID one_hop_neighbor_id) {
    min_one_hop_neighbor_[id] =
        std::min(min_one_hop_neighbor_[id], one_hop_neighbor_id);
    max_one_hop_neighbor_[id] =
        std::max(max_one_hop_neighbor_[id], one_hop_neighbor_id);
  }

  void UpdateTwoHopInfo(VertexID id, VertexID two_hop_nerighbor_id) {
    if (two_hop_nerighbor_id == id) return;
    if (CheckIsOneHop(id, two_hop_nerighbor_id)) return;
    min_two_hop_neighbor_[id] =
        std::min(min_two_hop_neighbor_[id], two_hop_nerighbor_id);
    max_two_hop_neighbor_[id] =
        std::max(max_two_hop_neighbor_[id], two_hop_nerighbor_id);
    has_two_hop_neighbor_[id] = true;
  }

  bool CheckIsOneHop(VertexID id, VertexID neighbor) {
    auto degree = GetDegree(id);
    if (degree == 0) return false;
    auto edges = GetEdges(id);
    for (auto i = 0; i < degree; i++) {
      if (edges[i] == neighbor) return true;
    }
    return false;
  }

 public:
  VertexID* degree_ = nullptr;
  EdgeIndex* offset_ = nullptr;
  VertexID* edges_ = nullptr;
  uint32_t num_vertices_;
  EdgeIndex num_edges_;
  VertexID bid_;
  VertexID eid_;
  bool isRead_ = false;
  // two hop info
  VertexID* min_one_hop_neighbor_ = nullptr;
  VertexID* max_one_hop_neighbor_ = nullptr;
  VertexID* min_two_hop_neighbor_ = nullptr;
  VertexID* max_two_hop_neighbor_ = nullptr;
  std::vector<bool> has_two_hop_neighbor_;
};

struct Blocks {
  Blocks(core::data_structures::GraphMetadata& graph_metadata) {
    auto num_block = graph_metadata.get_num_blocks();
    blocks.reserve(num_block);
    for (uint32_t i = 0; i < num_block; i++) {
      auto block = graph_metadata.GetBlockMetadata(i);
      blocks.emplace_back(block.num_vertices, block.num_outgoing_edges,
                          block.begin_id, block.end_id);
    }
  }

  BlockID GetBlockID(VertexID id) {
    for (VertexID i = 0; i < blocks.size(); i++) {
      if (blocks[i].bid_ <= id && id < blocks[i].eid_) {
        return i;
      }
    }
    LOGF_INFO("Can't find block id for vertex {}", id);
    return -1;
  }

  std::vector<Block> blocks;
};

struct BlockTwoHopInfo {
  GraphID bid;
  EdgeIndex num_two_hop_edges;
};

struct TwoHopInfos {
  TwoHopInfos() = default;
  TwoHopInfos(const std::string& root_path) {
    YAML::Node node;
    try {
      node = YAML::LoadFile(root_path + "precomputing/two_hop_info.yaml");
      *this = node["TwoHopInfos"].as<TwoHopInfos>();
    } catch (YAML::BadFile e) {
      LOG_ERROR("two_hop_info.yaml file read failed! ", e.msg);
    }
  }
  std::vector<BlockTwoHopInfo> infos;
};

}  // namespace sics::graph::nvme::precomputing

namespace YAML {

using GraphID = sics::graph::core::common::GraphID;
using BlockID = sics::graph::core::common::BlockID;
using VertexID = sics::graph::core::common::VertexID;
using EdgeIndex = sics::graph::core::common::EdgeIndex;
using sics::graph::nvme::precomputing::BlockTwoHopInfo;
using sics::graph::nvme::precomputing::TwoHopInfos;

template <>
struct convert<BlockTwoHopInfo> {
  static Node encode(const BlockTwoHopInfo& info) {
    Node node;
    node["bid"] = info.bid;
    node["num_two_hop_edges"] = info.num_two_hop_edges;
    return node;
  }
  static bool decode(const Node& node, BlockTwoHopInfo& info) {
    info.bid = node["bid"].as<GraphID>();
    info.num_two_hop_edges = node["num_two_hop_edges"].as<EdgeIndex>();
    return true;
  }
};

template <>
struct convert<TwoHopInfos> {
  static Node encode(const TwoHopInfos& infos) {
    Node node;
    for (const auto& info : infos.infos) {
      node.push_back(info);
    }
    return node;
  }
  static bool decode(const Node& node, TwoHopInfos& infos) {
    for (const auto& info : node) {
      infos.infos.push_back(info.as<BlockTwoHopInfo>());
    }
    return true;
  }
};

}  // namespace YAML

#endif  // GRAPH_SYSTEMS_NVME_PRECOMPUTING_BASIC_H_
