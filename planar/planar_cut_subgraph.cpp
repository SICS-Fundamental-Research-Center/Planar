#include <gflags/gflags.h>

#include <filesystem>
#include <fstream>

#include "core/common/bitmap.h"
#include "core/data_structures/graph_metadata.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");
DEFINE_uint32(cut_v, 10000000, "cut number of vertex");
DEFINE_uint32(offset_ratio, 64, "offset compress ratio");

namespace fs = std::filesystem;

using namespace sics::graph;
using core::common::BlockID;
using core::common::EdgeIndex;
using core::common::GraphID;
using core::common::VertexDegree;
using core::common::VertexID;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string root_path = FLAGS_i;
  uint32_t ratio = FLAGS_offset_ratio;
  uint32_t cut_v = FLAGS_cut_v;
  // read subgraph info
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
    file.read((char*)(offset), num_vertices * sizeof(core::common::EdgeIndex));
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
  return 0;
}
