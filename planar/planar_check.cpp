#include <gflags/gflags.h>

#include <filesystem>
#include <fstream>

#include "core/common/bitmap.h"
#include "core/data_structures/graph_metadata.h"
#include "core/planar_system.h"
#include "data_structures/graph/mutable_block_csr_graph.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_string(mode, "csr", "csr or block or pages");

using namespace sics::graph;
namespace fs = std::filesystem;

using sics::graph::core::common::BlockID;
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::EdgeIndexS;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::VertexID;

EdgeIndexS GetOffset(EdgeIndexS* offset, VertexID* degree, VertexID vid,
                     uint32_t ratio) {
  auto b = vid / ratio;
  auto res = offset[b];
  auto beg = b * ratio;
  while (beg < vid) {
    res += degree[beg++];
  }
  return res;
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string root_path = FLAGS_i;
  std::string mode = FLAGS_mode;

  core::data_structures::GraphMetadata graph_metadata(root_path);

  if (mode == "csr") {
    auto subgraph_num = graph_metadata.get_num_subgraphs();
    for (GraphID i = 0; i < subgraph_num; i++) {
      auto subgraph_metadata = graph_metadata.GetSubgraphMetadata(i);
      auto num_vertices = subgraph_metadata.num_vertices;
      auto num_edges = subgraph_metadata.num_outgoing_edges;

      std::ifstream subgraph_file(
          root_path + "graphs/" + std::to_string(i) + ".bin", std::ios::binary);
      if (!subgraph_file) {
        LOG_FATAL("Error opening bin file: ",
                  root_path + "graphs/" + std::to_string(i) + ".bin");
      }
      auto index = new core::common::VertexID[num_vertices];
      auto degree = new core::common::VertexDegree[num_vertices];
      auto offset = new core::common::EdgeIndex[num_vertices];
      auto edges = new core::common::VertexID[num_edges];
      subgraph_file.read((char*)index,
                         num_vertices * sizeof(core::common::VertexID));
      subgraph_file.read((char*)degree,
                         num_vertices * sizeof(core::common::VertexDegree));
      subgraph_file.read((char*)offset,
                         num_vertices * sizeof(core::common::EdgeIndex));
      subgraph_file.read((char*)edges,
                         num_edges * sizeof(core::common::VertexID));

      LOG_INFO("SubgraphID: ", i, ", VertexNum: ", num_vertices,
               ", EdgeNum: ", num_edges);
      for (VertexID idx = 0; idx < num_vertices; idx++) {
        std::string tmp = "Index: " + std::to_string(idx) +
                          ", VertexID: " + std::to_string(index[idx]) + ", ";
        tmp += "Degree: " + std::to_string(degree[idx]) + ", ";
        tmp += "Offset: " + std::to_string(offset[idx]) + ", Edges: ";
        auto d = degree[idx];
        auto off = offset[idx];
        for (EdgeIndex j = 0; j < d; j++) {
          tmp += std::to_string(edges[off + j]) + ", ";
        }
        LOG_INFO(tmp);
      }
      delete[] index;
      delete[] degree;
      delete[] offset;
      delete[] edges;
    }
  } else if (mode == "block") {
    auto block_num = graph_metadata.get_num_blocks();
    for (BlockID i = 0; i < block_num; i++) {
      auto block_metadata = graph_metadata.GetBlockMetadata(i);
      auto bid = block_metadata.begin_id;
      auto eid = block_metadata.end_id;
      auto num_vertex = block_metadata.num_vertices;
      auto num_edge = block_metadata.num_outgoing_edges;

      std::ifstream block_file(
          root_path + "graphs/" + std::to_string(i) + ".bin", std::ios::binary);
      if (!block_file) {
        LOG_FATAL("Error opening bin file: ",
                  root_path + "graphs/" + std::to_string(i) + ".bin");
      }
      auto degree = new core::common::VertexDegree[num_vertex];
      auto offset = new core::common::EdgeIndex[num_vertex];
      auto edges = new core::common::VertexID[num_edge];
      block_file.read((char*)degree,
                      num_vertex * sizeof(core::common::VertexDegree));
      block_file.read((char*)offset,
                      num_vertex * sizeof(core::common::EdgeIndex));
      block_file.read((char*)edges, num_edge * sizeof(core::common::VertexID));

      LOG_INFO("BlockID: ", i, ", VertexNum: ", num_vertex,
               ", EdgeNum: ", num_edge);
      for (VertexID id = bid; id < eid; id++) {
        std::string tmp = "VertexID: " + std::to_string(id) + ", ";
        tmp += "Degree: " + std::to_string(degree[id - bid]) + ", ";
        tmp += "Offset: " + std::to_string(offset[id - bid]) + ", Edges: ";
        auto d = degree[id - bid];
        auto off = offset[id - bid];
        for (EdgeIndex j = 0; j < d; j++) {
          tmp += std::to_string(edges[off + j]) + ", ";
        }
        LOG_INFO(tmp);
      }
      delete[] degree;
      delete[] offset;
      delete[] edges;
    }
  } else if (mode == "block_del") {
    auto block_num = graph_metadata.get_num_blocks();
    auto block_metadata = graph_metadata.GetBlockMetadata(0);
    auto bid = block_metadata.begin_id;
    auto eid = block_metadata.end_id;
    auto num_vertex = block_metadata.num_vertices;
    auto num_edge = block_metadata.num_outgoing_edges;

    std::ifstream block_file(root_path + "graphs/" + std::to_string(0) + ".bin",
                             std::ios::binary);
    if (!block_file) {
      LOG_FATAL("Error opening bin file: ",
                root_path + "graphs/" + std::to_string(0) + ".bin");
    }
    auto degree = new core::common::VertexDegree[num_vertex];
    auto offset = new core::common::EdgeIndex[num_vertex];
    auto edges = new core::common::VertexID[num_edge];
    block_file.read((char*)degree,
                    num_vertex * sizeof(core::common::VertexDegree));
    block_file.read((char*)offset,
                    num_vertex * sizeof(core::common::EdgeIndex));
    block_file.read((char*)edges, num_edge * sizeof(core::common::VertexID));

    LOG_INFO("BlockID: ", 0, ", VertexNum: ", num_vertex,
             ", EdgeNum: ", num_edge);

    auto degree_new = new core::common::VertexDegree[num_vertex];
    auto offset_new = new core::common::EdgeIndex[num_vertex];
    memcpy(degree_new, degree, sizeof(core::common::VertexDegree) * num_vertex);

    size_t num = 0;
    VertexID max_id = 0;
    for (VertexID id = bid; id < eid; id++) {
      auto d = degree[id - bid];
      auto off = offset[id - bid];
      for (EdgeIndex j = 0; j < d; j++) {
        auto dst = edges[off + j];
        if (dst == id) {
          degree_new[id]--;
          num++;
          max_id = max_id < id ? id : max_id;
        }
      }
    }
    LOGF_INFO("edge {}, maxId: {}", num, max_id);

    size_t num_edges_new = 0;
    for (VertexID id = bid; id < eid; id++) {
      auto idx = id - bid;
      num_edges_new += degree_new[idx];
    }
    assert(num_edges_new == block_metadata.num_outgoing_edges - num);
    LOG_INFO("edges left: ", num_edges_new);
    auto edges_new = new core::common::VertexID[num_edges_new];
    core::common::EdgeIndex idx = 0;
    for (VertexID id = bid; id < eid; id++) {
      auto d = degree[id - bid];
      auto off = offset[id - bid];
      for (EdgeIndex j = 0; j < d; j++) {
        auto dst = edges[off + j];
        if (dst != id) {
          edges_new[idx] = dst;
          idx++;
        }
      }
    }
    assert(idx == num_edges_new);
    offset_new[0] = 0;
    for (VertexID id = bid + 1; id < eid; id++) {
      auto i = id - bid;
      offset_new[i] = offset_new[i - 1] + degree_new[i - 1];
    }

    std::ofstream file_new(root_path + "graphs/0.bin.new", std::ios::binary);
    file_new.write((char*)degree_new,
                   num_vertex * sizeof(core::common::VertexDegree));
    if (!file_new) {
      LOG_FATAL("Error writing degree data file");
    }
    file_new.write((char*)offset_new,
                   num_vertex * sizeof(core::common::EdgeIndex));
    if (!file_new) {
      LOG_FATAL("Error writing offset data file");
    }
    file_new.write((char*)edges_new,
                   num_edges_new * sizeof(core::common::VertexID));
    if (!file_new) {
      LOG_FATAL("Error writing edges data file");
    }
    LOG_INFO("Write new file finished!");
    file_new.close();
    delete[] degree;
    delete[] offset;
    delete[] edges;
    delete[] degree_new;
    delete[] offset_new;
    delete[] edges_new;
  } else if (mode == "subBlock") {
    core::data_structures::TwoDMetadata metadata(root_path);
    auto block_num = metadata.num_blocks;

    for (BlockID i = 0; i < block_num; i++) {
      auto block_meta = metadata.blocks.at(i);
      LOGF_INFO(
          "SubGraph ID: {}, num_vertices: {}, num_edges: {}, num_sub_blocks: "
          "{}",
          i, block_meta.num_vertices, block_meta.num_edges,
          block_meta.num_sub_blocks);
      auto num_blocks = block_meta.num_sub_blocks;
      auto num_vertices = block_meta.num_vertices;
      auto num_edges = block_meta.num_edges;

      auto dir = root_path + "graphs/" + std::to_string(i) + "_blocks/";
      auto index_path = dir + "index.bin";
      std::ifstream index_file(index_path, std::ios::binary);
      if (!index_file) {
        LOG_FATAL("Error opening bin file: ", index_path);
      }

      auto ratio = block_meta.offset_ratio;
      auto num_offsets = ((num_vertices - 1) / ratio) + 1;
      auto offset = new core::common::EdgeIndexS[num_offsets];
      auto degree = new core::common::VertexDegree[num_vertices];
      index_file.read((char*)offset,
                      num_offsets * sizeof(core::common::EdgeIndexS));
      index_file.read((char*)degree,
                      num_vertices * sizeof(core::common::VertexDegree));
      index_file.close();

      for (VertexID i = 0; i < block_meta.num_vertices; i++) {
        LOGF_INFO("ID: {}, Offset: {}, Degree: {}", i + block_meta.begin_id,
                  GetOffset(offset, degree, i, block_meta.offset_ratio),
                  degree[i]);
      }

      std::stringstream tmp;
      for (uint32_t j = 0; j < num_blocks; j++) {
        auto sub_block_meta = block_meta.sub_blocks.at(j);
        auto block_path = dir + std::to_string(j) + ".bin";
        std::ifstream block_file(block_path, std::ios::binary);
        if (!block_file) {
          LOG_FATAL("Error opening bin file: ", block_path);
        }
        auto edges = new core::common::VertexID[sub_block_meta.num_edges];
        block_file.read((char*)edges, sizeof(core::common::VertexID) *
                                          sub_block_meta.num_edges);
        for (int k = 0; k < sub_block_meta.num_edges; k++) {
          tmp << edges[k] << " ";
        }
        delete[] edges;
        LOGF_INFO(
            "BlockID: {}, BeginID: {}, EndID: {}, NumVertices: {}, "
            "NumEdges: {}, Edges: {}",
            j, sub_block_meta.begin_id, sub_block_meta.end_id,
            sub_block_meta.num_vertices, sub_block_meta.num_edges, tmp.str());
        tmp.str("");
      }

      delete[] offset;
      delete[] degree;
    }
  } else if (mode == "first") {
    core::data_structures::TwoDMetadata metadata(root_path);
    std::vector<core::data_structures::graph::MutableBlockCSRGraph> blocks(
        metadata.num_blocks);
    blocks.at(0).Init(root_path, &metadata.blocks.at(0));
    auto meta = metadata.blocks.at(0);
    auto sub_meta = meta.sub_blocks.at(0);
    auto path = root_path + "graphs/" + std::to_string(0) + "_blocks/0.bin";
    std::ifstream file(path, std::ios::binary);
    if (!file) {
      LOG_FATAL("Error opening bin file: ", path);
    }
    auto num_edges = meta.sub_blocks.at(0).num_edges;
    auto edges = new VertexID[num_edges];
    file.read((char*)edges, num_edges * sizeof(VertexID));
    blocks.at(0).SetSubBlock(0, edges);

    size_t num = 0;
    size_t num_v = 0;
    VertexID b = sub_meta.begin_id, e = sub_meta.end_id;
    auto& block = blocks.at(0);
    for (VertexID i = b; i < e; i++) {
      auto degree = block.GetOutDegree(i);
      bool flag = false;
      if (degree != 0) {
        auto edges = block.GetOutEdges(i);
        for (VertexID j = 0; j < degree; j++) {
          auto dst = edges[j];
          if (dst == i) {
            num++;
            flag = true;
          }
        }
      }
      if (flag) {
        num_v++;
      }
    }
    LOGF_INFO("redundant edges: num: {}, block edges: {}, vertex {}", num,
              sub_meta.num_edges, num_v);
  } else if (mode == "delete") {
    core::data_structures::TwoDMetadata metadata(root_path);
    std::vector<core::data_structures::graph::MutableBlockCSRGraph> blocks(
        metadata.num_blocks);
    blocks.at(0).Init(root_path, &metadata.blocks.at(0));
    auto meta = metadata.blocks.at(0);
    auto sub_meta = meta.sub_blocks.at(0);
    auto path = root_path + "graphs/" + std::to_string(0) + "_blocks/0.bin";
    std::ifstream file(path, std::ios::binary);
    if (!file) {
      LOG_FATAL("Error opening bin file: ", path);
    }
    auto num_edges = meta.sub_blocks.at(0).num_edges;
    auto edges = new VertexID[num_edges];
    file.read((char*)edges, num_edges * sizeof(VertexID));
    blocks.at(0).SetSubBlock(0, edges);

    size_t num = 0;
    size_t num_v = 0;
    VertexID b = sub_meta.begin_id, e = sub_meta.end_id;
    auto& block = blocks.at(0);
    for (VertexID i = b; i < e; i++) {
      auto degree = block.GetOutDegree(i);
      bool flag = false;
      if (degree != 0) {
        auto edges = block.GetOutEdges(i);
        for (VertexID j = 0; j < degree; j++) {
          auto dst = edges[j];
          if (dst == i) {
            num++;
            flag = true;
          }
        }
      }
      if (flag) {
        num_v++;
      }
    }
    LOGF_INFO("redundant edges: num: {}, block edges: {}, vertex {}", num,
              sub_meta.num_edges, num_v);
  } else {
    core::data_structures::TwoDMetadata metadata(root_path);
    std::vector<core::data_structures::graph::MutableBlockCSRGraph> blocks(
        metadata.num_blocks);
    for (int i = 0; i < metadata.num_blocks; i++) {
      blocks.at(i).Init(root_path, &metadata.blocks.at(i));
      auto meta = metadata.blocks.at(i);
      for (int j = 0; j < meta.num_sub_blocks; j++) {
        auto path = root_path + "graphs/" + std::to_string(i) + "_blocks/" +
                    std::to_string(j) + ".bin";
        std::ifstream file(path, std::ios::binary);
        if (!file) {
          LOG_FATAL("Error opening bin file: ", path);
        }
        auto num_edges = meta.sub_blocks.at(j).num_edges;
        auto edges = new VertexID[num_edges];
        file.read((char*)edges, num_edges * sizeof(VertexID));
        blocks.at(i).SetSubBlock(j, edges);
      }
      blocks.at(i).LogGraphInfo();
    }
  }

  return 0;
}
