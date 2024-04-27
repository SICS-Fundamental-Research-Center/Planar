#include <gflags/gflags.h>

#include <fstream>

#include "core/apps/coloring_app.h"
#include "core/common/bitmap.h"
#include "core/data_structures/graph_metadata.h"
#include "core/planar_system.h"

DEFINE_string(i, "/testfile", "graph files root path");

using namespace sics::graph;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string root_path = FLAGS_i;
  // read subgraph info

  core::data_structures::GraphMetadata graph_metadata(root_path);

  for (uint32_t i = 0; i < graph_metadata.get_num_subgraphs(); i++) {
    auto subgraph = graph_metadata.GetSubgraphMetadata(i);
    // generate is_in_graph bitmap
    std::string meta_path = root_path + "graphs/" + std::to_string(i) + ".bin";

    std::ifstream meta_file(meta_path, std::ios::binary);
    if (!meta_file) {
      LOG_FATAL("Error opening bin file: ", meta_path.c_str());
    }

    auto vid_by_index = new core::common::VertexID[subgraph.num_vertices];
    meta_file.read((char*)(vid_by_index),
                   subgraph.num_vertices * sizeof(core::common::VertexID));

    core::common::Bitmap is_in_graph_bitmap(graph_metadata.get_num_vertices());
    for (uint32_t i = 0; i < subgraph.num_vertices; i++) {
      is_in_graph_bitmap.SetBit(vid_by_index[i]);
    }

    // write back is_in_graph_bitmap
    std::string is_in_graph_path =
        root_path + "bitmap/is_in_graph/" + std::to_string(i) + ".bin";
    std::ofstream is_in_graph_file(is_in_graph_path, std::ios::binary);
    is_in_graph_file.write(
        (char*)(is_in_graph_bitmap.GetDataBasePointer()),
        ((is_in_graph_bitmap.size() >> 6) + 1) * sizeof(uint64_t));
    if (!is_in_graph_file) {
      LOG_FATAL("Error opening bin file: ", is_in_graph_path.c_str());
    }
    meta_file.close();
    is_in_graph_file.close();
  }

  return 0;
}
