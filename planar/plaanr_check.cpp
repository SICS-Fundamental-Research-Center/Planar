#include <gflags/gflags.h>

#include <filesystem>
#include <fstream>

#include "core/common/bitmap.h"
#include "core/data_structures/graph_metadata.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_string(mode, "csr", "csr or block or pages");

using namespace sics::graph;
namespace fs = std::filesystem;

using sics::graph::core::common::BlockID;
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::VertexID;

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
  } else {
    auto block_num = graph_metadata.get_num_blocks();
    for (BlockID i = 0; i < block_num; i++) {
      auto dir = root_path + "graphs/" + std::to_string(i) + "_blocks/";
      auto block_meta_file = dir + "blocks.yaml";
      sics::graph::core::data_structures::Blocks blocks_meta(block_meta_file);
      auto num_blocks = blocks_meta.num_blocks_;
      auto num_vertices = blocks_meta.num_vertices_;
      auto num_edges = blocks_meta.num_edges_;

      auto index_path =
          root_path + "graphs/" + std::to_string(i) + "_blocks/index.bin";
      std::ifstream index_file(index_path, std::ios::binary);
      if (!index_file) {
        LOG_FATAL("Error opening bin file: ", index_path);
      }

      auto ratio = blocks_meta.offset_ratio_;
      auto num_offsets = ((num_vertices - 1) / ratio) + 1;
      auto offset = new core::common::EdgeIndexS[num_offsets];
      auto degree = new core::common::VertexDegree[num_vertices];
      index_file.read((char*)offset,
                      num_offsets * sizeof(core::common::EdgeIndexS));
      index_file.read((char*)degree,
                      num_vertices * sizeof(core::common::VertexDegree));
      index_file.close();

      std::stringstream tmp;
      for (uint32_t j = 0; j < num_blocks; j++) {
        auto block_meta = blocks_meta.blocks_[j];
        auto block_path = dir + std::to_string(j) + ".bin";
        std::ifstream block_file(block_path, std::ios::binary);
        if (!block_file) {
          LOG_FATAL("Error opening bin file: ", block_path);
        }
        auto edges = new core::common::VertexID[block_meta.num_edges];
        block_file.read((char*)edges,
                        sizeof(core::common::VertexID) * block_meta.num_edges);
        for (int k = 0; k < block_meta.num_edges; k++) {
          tmp << edges[k] << " ";
        }
        delete[] edges;
        LOGF_INFO(
            "BlockID: {}, BeginID: {}, EndID: {}, NumVertices: {}, "
            "NumEdges: {}, Edges: {}",
            j, block_meta.begin_id, block_meta.end_id, block_meta.num_vertices,
            block_meta.num_edges, tmp.str());
        tmp.str("");
      }

      delete[] offset;
      delete[] degree;
    }
  }

  return 0;
}
