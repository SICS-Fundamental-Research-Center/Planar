#include <gflags/gflags.h>

#include <filesystem>
#include <fstream>

#include "core/common/bitmap.h"
#include "core/data_structures/graph_metadata.h"
#include "core/planar_system.h"
#include "data_structures/graph/mutable_block_csr_graph.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_string(o, "/output", "output files root path");
DEFINE_uint32(p, 1, "parallelism");
DEFINE_uint32(cut_v, 500000, "block vertex number");
DEFINE_uint32(offset_ratio, 64, "offset compress ratio");

using namespace sics::graph;
namespace fs = std::filesystem;
using core::common::BlockID;
using core::common::EdgeIndex;
using core::common::GraphID;
using core::common::VertexDegree;
using core::common::VertexID;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string root_path = FLAGS_i;
  std::string out_dir = FLAGS_o;
  uint32_t partition = FLAGS_p;
  uint32_t ratio = FLAGS_offset_ratio;
  uint32_t cut_v = FLAGS_cut_v;
  {
    fs::path dir = out_dir + "graphs";
    if (!fs::exists(dir)) {
      if (!fs::create_directories(dir)) {
        LOGF_FATAL("Failed creating directory: {}", dir.c_str());
      }
    }

    core::data_structures::GraphMetadata graph_metadata(root_path);

    auto subgraph = graph_metadata.GetSubgraphMetadata(0);
    std::string meta_path = root_path + "graphs/" + std::to_string(0) + ".bin";
    std::ifstream meta_file(meta_path, std::ios::binary);
    if (!meta_file) {
      LOG_FATAL("Error opening bin file: ", meta_path.c_str());
    }

    auto vid_by_index = new core::common::VertexID[subgraph.num_vertices];
    auto degree = new core::common::VertexDegree[subgraph.num_vertices];
    auto offset = new core::common::EdgeIndex[subgraph.num_vertices];
    auto edges = new core::common::VertexID[subgraph.num_outgoing_edges];
    meta_file.read((char*)(vid_by_index),
                   subgraph.num_vertices * sizeof(core::common::VertexID));
    meta_file.read((char*)(degree),
                   subgraph.num_vertices * sizeof(core::common::VertexDegree));
    meta_file.read((char*)(offset),
                   subgraph.num_vertices * sizeof(core::common::EdgeIndex));
    meta_file.read((char*)(edges), subgraph.num_outgoing_edges *
                                       sizeof(core::common::VertexID));

    core::common::EdgeIndex size =
        (subgraph.num_outgoing_edges + partition - 1) / partition;

    std::vector<VertexID> block_bids;
    std::vector<VertexID> block_eids;
    std::vector<size_t> block_num_edges;
    std::vector<size_t> block_num_vertexes;
    auto num_vertices = subgraph.num_vertices;

    BlockID block_num = 0;
    VertexID bid = 0, eid = 0;
    LOG_INFO("Begin scanning edges");
    for (; bid < num_vertices;) {
      auto bid_offset = offset[bid];
      EdgeIndex step = bid_offset;
      while (step < bid_offset + size) {
        eid++;
        if (eid == num_vertices) {
          break;
        }
        step = offset[eid];
      }
      // get relative info for block
      block_bids.push_back(bid);
      block_eids.push_back(eid);
      block_num_edges.push_back(step - bid_offset);
      block_num_vertexes.push_back(eid - bid);
      // get next block init state
      block_num++;
      bid = eid;
    }
    block_num_edges.at(block_num - 1) =
        block_num_edges.at(block_num - 1) + degree[num_vertices - 1];
    if (block_num_edges.at(block_num - 1) == 0) {
      auto v_num = block_num_vertexes.at(block_num - 1);
      block_num--;
      block_eids.at(block_num - 1) = block_eids.at(block_num - 1) + v_num;
      block_num_vertexes.at(block_num - 1) =
          block_num_vertexes.at(block_num - 1) + v_num;
    }

    // todo: border vertices info
    // std::vector<int> borders = std::vector<int>(num_vertices, 0);

    // write back subgraph to disk
    for (BlockID i = 0; i < block_num; i++) {
      auto bid = block_bids[i];
      auto eid = block_eids[i];
      auto num_edge = block_num_edges[i];
      auto num_vertex = block_num_vertexes[i];

      std::ofstream block_file(out_dir + "graphs/" + std::to_string(i) + ".bin",
                               std::ios::binary);
      if (!block_file) {
        LOG_FATAL("Error opening bin file: ",
                  out_dir + "graphs/" + std::to_string(i) + ".bin");
      }
      auto offse_new = new EdgeIndex[num_vertex];
      for (size_t i = 0; i < num_vertex; i++) {
        offse_new[i] = offset[i + bid] - offset[bid];
      }
      auto degree_begin = degree + bid;
      block_file.write((char*)degree_begin, sizeof(VertexID) * num_vertex);
      block_file.write((char*)offse_new, sizeof(EdgeIndex) * num_vertex);
      auto edge_begin = edges + offset[bid];
      block_file.write((char*)edge_begin, sizeof(VertexID) * num_edge);
      block_file.close();
      delete[] offse_new;
      // is_in_graph_bitmap
      // TODO: write is_in_graph_bitmap
    }
    delete[] vid_by_index;
    delete[] degree;
    delete[] offset;
    delete[] edges;

    YAML::Node meta;
    graph_metadata.set_type("block");
    graph_metadata.set_num_subgraphs(block_num);
    std::vector<sics::graph::core::data_structures::BlockMetadata> tmp;
    for (BlockID i = 0; i < block_num; i++) {
      sics::graph::core::data_structures::BlockMetadata block_metadata;
      block_metadata.bid = i;
      block_metadata.num_vertices = block_num_vertexes[i];
      block_metadata.num_outgoing_edges = block_num_edges[i];
      block_metadata.begin_id = block_bids[i];
      block_metadata.end_id = block_eids[i];
      tmp.push_back(block_metadata);
    }
    graph_metadata.set_block_metadata_vec(tmp);
    meta["GraphMetadata"] = graph_metadata;
    std::ofstream meta_out(out_dir + "meta.yaml");
    meta_out << meta;
    meta_out.close();
    LOG_INFO("Finish writing subgraphs info");
  }

  {
    core::data_structures::GraphMetadata graph_metadata(root_path);
    auto block_num = graph_metadata.get_num_blocks();
    core::data_structures::TwoDMetadata metadata;
    metadata.num_vertices = graph_metadata.get_num_vertices();
    metadata.num_edges = graph_metadata.get_num_edges();
    metadata.num_blocks = block_num;
    for (BlockID gid = 0; gid < block_num; gid++) {
      auto block_metadata = graph_metadata.GetBlockMetadata(gid);
      std::string file_path =
          root_path + "graphs/" + std::to_string(gid) + ".bin";
      std::ifstream file(file_path, std::ios::binary);
      if (!file) {
        LOG_FATAL("Error opening bin file: ", file_path.c_str());
      }
      auto num_vertices = block_metadata.num_vertices;
      auto num_edges = block_metadata.num_outgoing_edges;

      auto degree = new core::common::VertexDegree[num_vertices];
      auto offset = new core::common::EdgeIndex[num_vertices];
      auto edges = new core::common::VertexID[num_edges];
      file.read((char*)(degree),
                num_vertices * sizeof(core::common::VertexDegree));
      file.read((char*)(offset),
                num_vertices * sizeof(core::common::EdgeIndex));
      file.read((char*)(edges), num_edges * sizeof(core::common::VertexID));

      auto p = ((num_vertices - 1) / cut_v) + 1;
      auto size = (num_vertices + p - 1) / p;

      fs::path dir = root_path + "graphs/" + std::to_string(gid) + "_blocks";
      if (!fs::exists(dir)) {
        if (!fs::create_directories(dir)) {
          LOGF_FATAL("Failed creating directory: {}", dir.c_str());
        }
      }

      auto num_offsets = ((num_vertices - 1) / ratio) + 1;
      auto offset_new = new EdgeIndex[num_offsets];
      for (uint32_t i = 0; i < num_offsets; i++) {
        auto index = i * ratio;
        offset_new[i] = offset[index];
      }

      std::ofstream index_file(dir.string() + "/index.bin", std::ios::binary);
      index_file.write((char*)offset_new, num_offsets * sizeof(EdgeIndex));
      index_file.write((char*)degree, num_vertices * sizeof(VertexDegree));
      index_file.close();
      delete[] offset_new;

      std::vector<core::data_structures::SubBlock> blks(p);
      auto bid = block_metadata.begin_id;
      auto eid = block_metadata.end_id;
      for (uint32_t i = 0; i < p; i++) {
        auto begin_id = i * size;
        auto end_id = (i + 1) * size;
        auto num_edge = 0;
        if (end_id >= num_vertices) {
          end_id = num_vertices;
          num_edge = offset[end_id - 1] - offset[begin_id] + degree[end_id - 1];
        } else {
          num_edge = offset[end_id] - offset[begin_id];
        }
        blks.at(i).id = i;
        blks.at(i).begin_id = begin_id + bid;
        blks.at(i).end_id = end_id + bid;
        blks.at(i).num_edges = num_edge;
        blks.at(i).num_vertices = end_id - begin_id;
        blks.at(i).begin_offset = offset[begin_id];
        // cut edges
        std::ofstream out_file(dir.string() + "/" + std::to_string(i) + ".bin",
                               std::ios::binary);
        out_file.write((char*)(edges + offset[begin_id]),
                       num_edge * sizeof(VertexID));
        out_file.close();
      }

      delete[] degree;
      delete[] offset;
      delete[] edges;

      sics::graph::core::data_structures::Block block;
      block.id = gid;
      block.num_sub_blocks = p;
      block.num_vertices = num_vertices;
      block.num_edges = num_edges;
      block.offset_ratio = ratio;
      block.begin_id = bid;
      block.end_id = eid;
      block.vertex_offset = size;
      block.sub_blocks = blks;
      metadata.blocks.push_back(block);
    }

    YAML::Node meta;
    meta["GraphMetadata"] = metadata;
    std::ofstream meta_file(root_path + "graphs/blocks_meta.yaml");
    meta_file << meta;
    meta_file.close();
    LOG_INFO("Finish writing blocks info!");
  }
  return 0;
}