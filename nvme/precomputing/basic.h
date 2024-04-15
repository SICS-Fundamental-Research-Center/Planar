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
    two_hop_neighbors_.resize(num_vertices_);
    //    for (VertexID i = 0; i < num_vertices_; i++) {
    //      two_hop_neighbors_
    //      two_hop_neighbors_[i] = std::unordered_set<VertexID>();
    //    }
    isRead_ = true;
  }
  void WriteTwoHopInfo(const std::string& path,
                       core::common::ThreadPool* pool = nullptr) {
    std::ofstream two_hop_file(path, std::ios::binary);
    auto degree = new VertexID[num_vertices_];
    auto offset = new EdgeIndex[num_vertices_];
    //    offset[0] = 0;
    //    degree[0] = two_hop_neighbors_[0].size();

    auto parallelism = pool->GetParallelism() * 10;
    auto task_size = (num_vertices_ + parallelism - 1) / parallelism;
    core::common::TaskPackage tasks;
    uint32_t task_num = 0;
    VertexID b1 = 0, e1 = 0;
    // First, compute offset in partition, offset of every partition begins at 0
    for (; b1 < num_vertices_;) {
      e1 = b1 + task_size < num_vertices_ ? b1 + task_size : num_vertices_;
      auto task = [degree, offset, this, b1, e1]() {
        degree[b1] = two_hop_neighbors_[b1].size();
        offset[b1] = 0;
        for (auto i = b1 + 1; i < e1; i++) {
          degree[i] = two_hop_neighbors_[i].size();
          offset[i] = offset[i - 1] + degree[i - 1];
        }
      };
      tasks.push_back(task);
      task_num++;
      b1 = e1;
    }
    LOGF_INFO("task num: {}", tasks.size());
    pool->SubmitSync(tasks);

    // Second, compute base offset in partition
    EdgeIndex accumulate_base = 0;
    for (VertexID i = 0; i < task_num; i++) {
      VertexIndex b = i * task_size;
      VertexIndex e =
          b + task_size < num_vertices_ ? b + task_size : num_vertices_;
      offset[b] += accumulate_base;
      accumulate_base += offset[e - 1];
      accumulate_base += degree[e - 1];
    }
    tasks.clear();
    b1 = 0;
    e1 = 0;
    for (; b1 < num_vertices_;) {
      e1 = b1 + task_size < num_vertices_ ? b1 + task_size : num_vertices_;
      auto task = [offset, b1, e1]() {
        for (VertexID i = b1 + 1; i < e1; i++) {
          offset[i] += offset[b1];
        }
      };
      tasks.push_back(task);
      b1 = e1;
    }
    LOGF_INFO("task num: {}", tasks.size());
    pool->SubmitSync(tasks);

    LOG_INFO("Finish counting offset");
    num_two_hop_edges_ = offset[num_vertices_ - 1] + degree[num_vertices_ - 1];
    auto edges = new VertexID[num_two_hop_edges_];
    // copy two_hop_neighbors_ to edges
    VertexID b2 = 0, e2 = 0;
    tasks.clear();
    for (; b2 < num_vertices_;) {
      e2 = b2 + task_size < num_vertices_ ? b2 + task_size : num_vertices_;
      auto task = [edges, offset, this, b2, e2]() {
        for (VertexID i = b2; i < e2; i++) {
          int idx = 0;
          for (auto& neighbor : two_hop_neighbors_[i]) {
            edges[offset[i] + idx] = neighbor;
            idx++;
          }
        }
      };
      tasks.push_back(task);
      b2 = e2;
    }
    LOGF_INFO("task num: {}", tasks.size());
    pool->SubmitSync(tasks);
    LOG_INFO("Finish copy two_hop_neighbors_ to edges. Begin writing files");

    // write
    two_hop_file.write(reinterpret_cast<char*>(degree),
                       num_vertices_ * sizeof(VertexID));
    two_hop_file.write(reinterpret_cast<char*>(offset),
                       num_vertices_ * sizeof(EdgeIndex));
    two_hop_file.write(reinterpret_cast<char*>(edges),
                       num_two_hop_edges_ * sizeof(VertexID));
    two_hop_file.close();
    LOG_INFO("Finish writing files.");
    delete[] degree;
    delete[] offset;
    delete[] edges;
    // clear two_hop_neighbors_
    two_hop_neighbors_.clear();
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

  void AddNeighbor(VertexID id, VertexID neighbor) {
    two_hop_neighbors_[id].insert(neighbor);
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
  EdgeIndex num_two_hop_edges_;
  //  std::unordered_map<VertexID, std::set<VertexID>> two_hop_neighbors_;
  std::vector<std::unordered_set<VertexID>> two_hop_neighbors_;
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
