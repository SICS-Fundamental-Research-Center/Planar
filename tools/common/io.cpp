#include "io.h"

namespace sics::graph::tools::common {

using sics::graph::core::common::Bitmap;
using sics::graph::core::common::EdgeIndex;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexLabel;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::atomic::WriteMax;
using sics::graph::core::util::atomic::WriteMin;
using std::filesystem::create_directory;
using std::filesystem::exists;

void GraphFormatConverter::WriteSubgraph(
    const std::vector<std::vector<Vertex>>& vertex_buckets,
    const GraphMetadata& graph_metadata, StoreStrategy store_strategy) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();
  task_package.reserve(parallelism);

  std::vector<SubgraphMetadata> subgraph_metadata_vec;
  std::ofstream border_vertices_file(output_root_path_ +
                                     "bitmap/border_vertices.bin");
  std::ofstream index_file(output_root_path_ + "index/global_index.bin");

  auto aligned_max_vid = (((graph_metadata.get_max_vid() + 1) >> 6) << 6) + 64;
  Bitmap border_vertices(aligned_max_vid);
  auto buffer_globalid2index = new VertexID[aligned_max_vid]();

  // Write subgraph.
  for (GraphID gid = 0; gid < graph_metadata.get_num_subgraphs(); gid++) {
    auto bucket = vertex_buckets.at(gid);

    std::ofstream out_data_file(output_root_path_ + "graphs/" +
                                std::to_string(gid) + ".bin");
    std::ofstream src_map_file(output_root_path_ + "bitmap/src_map/" +
                               std::to_string(gid) + ".bin");
    std::ofstream is_in_graph_file(output_root_path_ + "bitmap/is_in_graph/" +
                                   std::to_string(gid) + ".bin");

    EdgeIndex count_in_edges = 0, count_out_edges = 0;
    VertexID num_vertices = bucket.size();
    VertexID max_vid = 0, min_vid = MAX_VERTEX_ID;
    Bitmap src_map(num_vertices), is_in_graph(aligned_max_vid);

    auto buffer_globalid = new VertexID[num_vertices]();
    auto buffer_indegree = new VertexID[num_vertices]();
    auto buffer_outdegree = new VertexID[num_vertices]();

    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([&, i]() {
        for (VertexID j = i; j < num_vertices; j += parallelism) {
          buffer_globalid[j] = bucket.at(j).vid;
          is_in_graph.SetBit(bucket.at(j).vid);
          buffer_globalid2index[bucket.at(j).vid] = j;
          buffer_indegree[j] = bucket.at(j).indegree;
          buffer_outdegree[j] = bucket.at(j).outdegree;
          WriteAdd(&count_out_edges, (EdgeIndex)bucket.at(j).outdegree);
          WriteAdd(&count_in_edges, (EdgeIndex)bucket.at(j).indegree);
          WriteMin(&min_vid, buffer_globalid[j]);
          WriteMax(&max_vid, buffer_globalid[j]);
        }
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();

    // Write index 2 globalid.
    out_data_file.write(reinterpret_cast<char*>(buffer_globalid),
                        sizeof(VertexID) * num_vertices);
    delete[] buffer_globalid;

    // Compute offset for each vertex.
    auto buffer_in_offset = new EdgeIndex[num_vertices]();
    auto buffer_out_offset = new EdgeIndex[num_vertices]();
    for (VertexID i = 1; i < num_vertices; i++) {
      buffer_in_offset[i] = buffer_in_offset[i - 1] + buffer_indegree[i - 1];
      buffer_out_offset[i] = buffer_out_offset[i - 1] + buffer_outdegree[i - 1];
    }

    // Save degree buffer and offset buffer.
    switch (store_strategy) {
      case kOutgoingOnly:
        out_data_file.write(reinterpret_cast<char*>(buffer_outdegree),
                            sizeof(VertexID) * num_vertices);
        out_data_file.write(reinterpret_cast<char*>(buffer_out_offset),
                            sizeof(EdgeIndex) * num_vertices);
        break;
      case kIncomingOnly:
        out_data_file.write(reinterpret_cast<char*>(buffer_indegree),
                            sizeof(VertexID) * num_vertices);
        out_data_file.write(reinterpret_cast<char*>(buffer_in_offset),
                            sizeof(EdgeIndex) * num_vertices);
        break;
      case kUnconstrained:
        out_data_file.write(reinterpret_cast<char*>(buffer_indegree),
                            sizeof(VertexID) * num_vertices);
        out_data_file.write(reinterpret_cast<char*>(buffer_outdegree),
                            sizeof(VertexID) * num_vertices);
        out_data_file.write(reinterpret_cast<char*>(buffer_in_offset),
                            sizeof(EdgeIndex) * num_vertices);
        out_data_file.write(reinterpret_cast<char*>(buffer_out_offset),
                            sizeof(EdgeIndex) * num_vertices);
        break;
      case kUndefinedStrategy:
        LOG_FATAL("Store_strategy is undefined");
    }
    delete[] buffer_indegree;
    delete[] buffer_outdegree;

    // Fill edges.
    auto buffer_in_edges = new VertexID[count_in_edges]();
    auto buffer_out_edges = new VertexID[count_out_edges]();

    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([&, i, parallelism]() {
        for (VertexID j = i; j < num_vertices; j += parallelism) {
          if (bucket.at(j).indegree != 0) {
            memcpy(buffer_in_edges + buffer_in_offset[j],
                   bucket.at(j).incoming_edges,
                   bucket.at(j).indegree * sizeof(VertexID));
            std::sort(
                buffer_in_edges + buffer_in_offset[j],
                buffer_in_edges + buffer_in_offset[j] + bucket.at(j).indegree);
          }
          if (bucket.at(j).outdegree != 0) {
            memcpy(buffer_out_edges + buffer_out_offset[j],
                   bucket.at(j).outgoing_edges,
                   bucket.at(j).outdegree * sizeof(VertexID));
            std::sort(buffer_out_edges + buffer_out_offset[j],
                      buffer_out_edges + buffer_out_offset[j] +
                          bucket.at(j).outdegree);
          }
        }
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();

    delete[] buffer_in_offset;
    delete[] buffer_out_offset;

    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([&, i, parallelism, store_strategy]() {
        for (VertexID j = i; j < num_vertices; j += parallelism) {
          switch (store_strategy) {
            case kOutgoingOnly:
              for (VertexID nbr_i = 0; nbr_i < bucket.at(j).outdegree;
                   nbr_i++) {
                if (!is_in_graph.GetBit(bucket.at(j).outgoing_edges[nbr_i])) {
                  border_vertices.SetBit(bucket.at(j).vid);
                  break;
                }
              }
              break;
            case kIncomingOnly:
              for (VertexID nbr_i = 0; nbr_i < bucket.at(j).indegree; nbr_i++) {
                if (!is_in_graph.GetBit(bucket.at(j).incoming_edges[nbr_i])) {
                  border_vertices.SetBit(bucket.at(j).vid);
                  break;
                }
              }
              break;
            case kUnconstrained:
              for (VertexID nbr_i = 0; nbr_i < bucket.at(j).outdegree;
                   nbr_i++) {
                if (!is_in_graph.GetBit(bucket.at(j).outgoing_edges[nbr_i])) {
                  border_vertices.SetBit(bucket.at(j).vid);
                  break;
                }
              }
              break;
            case kUndefinedStrategy:
              LOG_FATAL("Store_strategy is undefined");
          }
        }
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();

    // Write bitmap that indicate whether a vertex has outgoing edges.
    src_map_file.write(reinterpret_cast<char*>(src_map.GetDataBasePointer()),
                       ((src_map.size() >> 6) + 1) * sizeof(uint64_t));
    src_map_file.close();
    is_in_graph_file.write(
        reinterpret_cast<char*>(is_in_graph.GetDataBasePointer()),
        ((is_in_graph.size() >> 6) + 1) * sizeof(uint64_t));
    is_in_graph_file.close();

    // Write edges.
    switch (store_strategy) {
      case kOutgoingOnly:
        out_data_file.write(reinterpret_cast<char*>(buffer_out_edges),
                            sizeof(VertexID) * count_out_edges);
        break;
      case kIncomingOnly:
        out_data_file.write(reinterpret_cast<char*>(buffer_in_edges),
                            sizeof(VertexID) * count_in_edges);
        break;
      case kUnconstrained:
        out_data_file.write(reinterpret_cast<char*>(buffer_in_edges),
                            sizeof(VertexID) * count_in_edges);
        out_data_file.write(reinterpret_cast<char*>(buffer_out_edges),
                            sizeof(VertexID) * count_out_edges);
        break;
      case kUndefinedStrategy:
        LOG_FATAL("Store_strategy is undefined");
    }
    delete[] buffer_in_edges;
    delete[] buffer_out_edges;
    out_data_file.close();

    // Write label data with all 0.
    std::ofstream out_label_file(output_root_path_ + "label/" +
                                 std::to_string(gid) + ".bin");
    auto buffer_label = new VertexLabel[num_vertices]();
    out_label_file.write(reinterpret_cast<char*>(buffer_label),
                         sizeof(VertexLabel) * num_vertices);
    delete[] buffer_label;
    out_label_file.close();

    // Write subgraph metadata.
    switch (store_strategy) {
      case kOutgoingOnly:
        subgraph_metadata_vec.push_back(
            {gid, num_vertices, 0, count_out_edges, max_vid, min_vid});
        break;
      case kIncomingOnly:
        subgraph_metadata_vec.push_back(
            {gid, num_vertices, count_in_edges, 0, max_vid, min_vid});
        break;
      case kUnconstrained:
        subgraph_metadata_vec.push_back({gid, num_vertices, count_in_edges,
                                         count_out_edges, max_vid, min_vid});
        break;
      case kUndefinedStrategy:
        LOG_FATAL("Store_strategy is undefined");
    }

    src_map_file.close();
    is_in_graph_file.close();
  }

  // Write border vertices bitmap.
  border_vertices_file.write(
      reinterpret_cast<char*>(border_vertices.GetDataBasePointer()),
      ((border_vertices.size() >> 6) + 1) * sizeof(uint64_t));
  border_vertices_file.close();

  // Write globalid2index.
  index_file.write(reinterpret_cast<char*>(buffer_globalid2index),
                   sizeof(VertexID) * aligned_max_vid);
  delete[] buffer_globalid2index;
  index_file.close();

  // Write Metadata
  std::ofstream out_meta_file(output_root_path_ + "meta.yaml");
  YAML::Node out_node;
  out_node["GraphMetadata"]["num_vertices"] = graph_metadata.get_num_vertices();
  out_node["GraphMetadata"]["num_edges"] = graph_metadata.get_num_edges();
  out_node["GraphMetadata"]["max_vid"] = graph_metadata.get_max_vid();
  out_node["GraphMetadata"]["min_vid"] = graph_metadata.get_min_vid();
  out_node["GraphMetadata"]["count_border_vertices"] = border_vertices.Count();
  out_node["GraphMetadata"]["num_subgraphs"] =
      graph_metadata.get_num_subgraphs();
  out_node["GraphMetadata"]["subgraphs"] = subgraph_metadata_vec;

  out_meta_file << out_node << std::endl;
  out_meta_file.close();
}

// For vertex cut.
void GraphFormatConverter::WriteSubgraph(const std::vector<Edges>& edge_buckets,
                                         const GraphMetadata& graph_metadata,
                                         StoreStrategy store_strategy) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();
  task_package.reserve(parallelism);

  GraphID n_subgraphs = graph_metadata.get_num_subgraphs();

  std::vector<SubgraphMetadata> subgraph_metadata_vec;
  std::ofstream out_meta_file(output_root_path_ + "meta.yaml");
  std::ofstream border_vertices_file(output_root_path_ +
                                     "bitmap/border_vertices.bin");
  std::ofstream dependency_matrix_file(output_root_path_ +
                                       "dependency_matrix/dm.bin");

  auto aligned_max_vid = (((graph_metadata.get_max_vid() + 1) >> 6) << 6) + 64;
  auto frequency_of_vertices = new int[aligned_max_vid]();
  Bitmap border_vertices(aligned_max_vid);
  std::vector<Bitmap> is_in_graph_vec;
  is_in_graph_vec.resize(n_subgraphs);

  for (GraphID gid = 0; gid < n_subgraphs; gid++) {
    std::ofstream out_data_file(output_root_path_ + "graphs/" +
                                std::to_string(gid) + ".bin");
    std::ofstream src_map_file(output_root_path_ + "bitmap/src_map/" +
                               std::to_string(gid) + ".bin");
    std::ofstream is_in_graph_file(output_root_path_ + "bitmap/is_in_graph/" +
                                   std::to_string(gid) + ".bin");
    std::ofstream index_file(output_root_path_ + "index/" +
                             std::to_string(gid) + ".bin");

    auto bucket = edge_buckets.at(gid);
    ImmutableCSRGraph csr_graph(gid);
    util::format_converter::Edgelist2CSR(bucket, store_strategy, &csr_graph);

    auto buffer_globalid2index = new VertexID[aligned_max_vid]();
    auto buffer_globalid = csr_graph.GetGloablIDBasePointer();
    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([&, i]() {
        for (VertexID j = i; j < csr_graph.get_num_vertices();
             j += parallelism) {
          buffer_globalid2index[buffer_globalid[j]] = j;
        }
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();

    // Write globalid2index.
    index_file.write(reinterpret_cast<char*>(buffer_globalid2index),
                     sizeof(VertexID) * aligned_max_vid);
    delete[] buffer_globalid2index;
    index_file.close();

    Bitmap src_map(csr_graph.get_num_vertices());
    //    Bitmap is_in_graph(csr_graph.get_num_vertices());
    Bitmap is_in_graph(aligned_max_vid);
    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([gid, i, parallelism, &csr_graph, &src_map,
                             &is_in_graph_vec, &frequency_of_vertices]() {
        for (VertexID j = i; j < csr_graph.get_num_vertices();
             j += parallelism) {
          if (csr_graph.GetOutDegreeByLocalID(j) > 0) src_map.SetBit(j);
          is_in_graph_vec.at(gid).SetBit(csr_graph.GetGlobalIDByLocalID(j));
          WriteAdd(frequency_of_vertices + csr_graph.GetGlobalIDByLocalID(j),
                   1);
        }
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();

    // Write topology of graph.
    out_data_file.write(
        reinterpret_cast<char*>(csr_graph.GetGloablIDBasePointer()),
        sizeof(VertexID) * csr_graph.get_num_vertices());

    // Write subgraph metadata.
    switch (store_strategy) {
      case kOutgoingOnly:
        subgraph_metadata_vec.push_back(
            {csr_graph.get_gid(), csr_graph.get_num_vertices(), 0,
             csr_graph.get_num_outgoing_edges(), csr_graph.get_max_vid(),
             csr_graph.get_min_vid()});
        out_data_file.write(
            reinterpret_cast<char*>(csr_graph.GetOutDegreeBasePointer()),
            sizeof(VertexID) * csr_graph.get_num_vertices());
        out_data_file.write(
            reinterpret_cast<char*>(csr_graph.GetOutOffsetBasePointer()),
            sizeof(EdgeIndex) * csr_graph.get_num_vertices());
        out_data_file.write(
            reinterpret_cast<char*>(csr_graph.GetOutgoingEdgesBasePointer()),
            sizeof(VertexID) * csr_graph.get_num_outgoing_edges());
        break;
      case kIncomingOnly:
        subgraph_metadata_vec.push_back(
            {csr_graph.get_gid(), csr_graph.get_num_vertices(),
             csr_graph.get_num_incoming_edges(), 0, csr_graph.get_max_vid(),
             csr_graph.get_min_vid()});
        out_data_file.write(
            reinterpret_cast<char*>(csr_graph.GetInDegreeBasePointer()),
            sizeof(VertexID) * csr_graph.get_num_vertices());
        out_data_file.write(
            reinterpret_cast<char*>(csr_graph.GetInOffsetBasePointer()),
            sizeof(EdgeIndex) * csr_graph.get_num_vertices());
        out_data_file.write(
            reinterpret_cast<char*>(csr_graph.GetIncomingEdgesBasePointer()),
            sizeof(VertexID) * csr_graph.get_num_incoming_edges());
        break;
      case kUnconstrained:
        out_data_file.write(
            reinterpret_cast<char*>(csr_graph.GetInDegreeBasePointer()),
            sizeof(VertexID) * csr_graph.get_num_vertices());
        out_data_file.write(
            reinterpret_cast<char*>(csr_graph.GetOutDegreeBasePointer()),
            sizeof(VertexID) * csr_graph.get_num_vertices());
        out_data_file.write(
            reinterpret_cast<char*>(csr_graph.GetInOffsetBasePointer()),
            sizeof(EdgeIndex) * csr_graph.get_num_vertices());
        out_data_file.write(
            reinterpret_cast<char*>(csr_graph.GetOutOffsetBasePointer()),
            sizeof(EdgeIndex) * csr_graph.get_num_vertices());
        out_data_file.write(
            reinterpret_cast<char*>(csr_graph.GetIncomingEdgesBasePointer()),
            sizeof(VertexID) * csr_graph.get_num_outgoing_edges());
        out_data_file.write(
            reinterpret_cast<char*>(csr_graph.GetOutgoingEdgesBasePointer()),
            sizeof(VertexID) * csr_graph.get_num_outgoing_edges());
        subgraph_metadata_vec.push_back(
            {csr_graph.get_gid(), csr_graph.get_num_vertices(),
             csr_graph.get_num_incoming_edges(),
             csr_graph.get_num_outgoing_edges(), csr_graph.get_max_vid(),
             csr_graph.get_min_vid()});
        break;
      default:
        LOG_FATAL("Undefined store strategy.");
    }

    // Write label data with all 0.
    std::ofstream out_label_file(output_root_path_ + "label/" +
                                 std::to_string(csr_graph.get_gid()) + ".bin");
    auto buffer_label = new VertexLabel[csr_graph.get_num_vertices()]();
    out_label_file.write(reinterpret_cast<char*>(buffer_label),
                         sizeof(VertexLabel) * csr_graph.get_num_vertices());
    delete[] buffer_label;

    // Write Bitmaps that might potentially benefit a number of graph
    // applications.
    src_map_file.write(reinterpret_cast<char*>(src_map.GetDataBasePointer()),
                       ((src_map.size() >> 6) + 1) * sizeof(uint64_t));
    is_in_graph_file.write(
        reinterpret_cast<char*>(is_in_graph_vec.at(gid).GetDataBasePointer()),
        ((is_in_graph_vec.at(gid).size() >> 6) + 1) * sizeof(uint64_t));

    out_data_file.close();
    out_label_file.close();
    src_map_file.close();
    is_in_graph_file.close();
  }

  // Get border_vertices bitmap.
  for (unsigned int i = 0; i < parallelism; i++) {
    auto task = std::bind([&, i]() {
      for (VertexID j = i; j < aligned_max_vid; j += parallelism) {
        if (frequency_of_vertices[j] > 1) border_vertices.SetBit(j);
      }
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  task_package.clear();
  delete[] frequency_of_vertices;

  // Write the global border vertices bitmap to disk.
  border_vertices_file.write(
      reinterpret_cast<char*>(border_vertices.GetDataBasePointer()),
      ((border_vertices.size() >> 6) + 1) * sizeof(uint64_t));
  border_vertices_file.close();

  // Write dependency matrix.
  auto dependency_matrix = new VertexID[n_subgraphs * n_subgraphs]();
  for (GraphID i = 0; i < n_subgraphs; i++) {
    auto bucket = edge_buckets.at(i);
    for (GraphID j = 0; j < n_subgraphs; j++) {
      if (i == j) continue;
      for (size_t k = 0; k < bucket.get_metadata().num_edges; k++) {
        auto e = bucket.get_edge_by_index(k);
        if (is_in_graph_vec[j].GetBit(e.src)) {
          WriteAdd(&dependency_matrix[i * n_subgraphs + j], (VertexID)1);
        }
      }
    }
  }
  dependency_matrix_file.write(reinterpret_cast<char*>(dependency_matrix),
                               (n_subgraphs * n_subgraphs) * sizeof(VertexID));
  dependency_matrix_file.close();
  delete[] dependency_matrix;

  //  Write metadata.
  YAML::Node out_node;
  out_node["GraphMetadata"]["num_vertices"] = graph_metadata.get_num_vertices();
  out_node["GraphMetadata"]["num_edges"] = graph_metadata.get_num_edges();
  out_node["GraphMetadata"]["max_vid"] = graph_metadata.get_max_vid();
  out_node["GraphMetadata"]["min_vid"] = graph_metadata.get_min_vid();
  out_node["GraphMetadata"]["count_border_vertices"] = border_vertices.Count();
  out_node["GraphMetadata"]["num_subgraphs"] = subgraph_metadata_vec.size();
  out_node["GraphMetadata"]["subgraphs"] = subgraph_metadata_vec;
  out_meta_file << out_node << std::endl;
  out_meta_file.close();
}

}  // namespace sics::graph::tools::common