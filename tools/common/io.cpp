#include "io.h"

using sics::graph::core::common::GraphID;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::atomic::WriteMax;
using sics::graph::core::util::atomic::WriteMin;
using std::filesystem::create_directory;
using std::filesystem::exists;

namespace sics::graph::tools::common {

bool IOConverter::WriteSubgraph(
    const std::vector<folly::ConcurrentHashMap<VertexID, Vertex>*>&
        subgraph_vec,
    const GraphMetadata& graph_metadata, StoreStrategy store_strategy) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();

  GraphID gid = 0;
  std::vector<SubgraphMetadata> subgraph_metadata_vec;
  size_t global_num_vertices = 0, global_num_edges = 0;
  VertexID global_max_vid = 0, global_min_vid = MAX_VERTEX_ID;
  for (auto iter : subgraph_vec) {
    auto vertex_map = iter;
    std::ofstream out_data_file(output_root_path_ + "graphs/" +
                                std::to_string(gid) + ".bin");
    std::ofstream out_label_file(output_root_path_ + "label/" +
                                 std::to_string(gid) + ".bin");
    out_label_file.close();

    auto num_vertices = vertex_map->size();
    auto buffer_globalid =
        (VertexID*)malloc(sizeof(VertexID) * vertex_map->size());
    auto buffer_indegree =
        (VertexID*)malloc(sizeof(VertexID) * vertex_map->size());
    auto buffer_outdegree =
        (VertexID*)malloc(sizeof(VertexID) * vertex_map->size());
    size_t count_in_edges = 0, count_out_edges = 0;

    // Serialize subgraph
    auto csr_vertex_buffer =
        (Vertex*)malloc(sizeof(Vertex) * vertex_map->size());

    size_t count = 0;
    for (auto it = vertex_map->begin(); it != vertex_map->end(); ++it) {
      csr_vertex_buffer[count++] = it->second;
    }

    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([i, parallelism, num_vertices = vertex_map->size(),
                             &buffer_globalid, &buffer_indegree,
                             &buffer_outdegree, &vertex_map, &count_in_edges,
                             &count_out_edges, &csr_vertex_buffer]() {
        for (VertexID j = i; j < num_vertices; j += parallelism) {
          auto u = csr_vertex_buffer[j];
          buffer_globalid[j] = u.vid;
          buffer_indegree[j] = u.indegree;
          buffer_outdegree[j] = u.outdegree;
          WriteAdd(&count_out_edges, (size_t)u.outdegree);
          WriteAdd(&count_in_edges, (size_t)u.indegree);
        }
        return;
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();

    out_data_file.write(reinterpret_cast<char*>(buffer_globalid),
                        sizeof(VertexID) * num_vertices);
    delete buffer_globalid;

    auto buffer_in_offset = (VertexID*)malloc(sizeof(VertexID) * num_vertices);
    auto buffer_out_offset = (VertexID*)malloc(sizeof(VertexID) * num_vertices);
    memset(buffer_in_offset, 0, sizeof(VertexID) * num_vertices);
    memset(buffer_out_offset, 0, sizeof(VertexID) * num_vertices);

    // Compute offset for each vertex.
    for (VertexID i = 1; i < num_vertices; i++) {
      buffer_in_offset[i] = buffer_in_offset[i - 1] + buffer_indegree[i - 1];
      buffer_out_offset[i] = buffer_out_offset[i - 1] + buffer_outdegree[i - 1];
    }

    switch (store_strategy) {
      case kOutgoingOnly:
        out_data_file.write((char*)buffer_outdegree,
                            sizeof(VertexID) * num_vertices);
        out_data_file.write((char*)buffer_out_offset,
                            sizeof(VertexID) * num_vertices);
        break;
      case kIncomingOnly:
        out_data_file.write((char*)buffer_indegree,
                            sizeof(VertexID) * num_vertices);
        out_data_file.write((char*)buffer_in_offset,
                            sizeof(VertexID) * num_vertices);
        break;
      case kUnconstrained:
        out_data_file.write((char*)buffer_indegree,
                            sizeof(VertexID) * num_vertices);
        out_data_file.write((char*)buffer_outdegree,
                            sizeof(VertexID) * num_vertices);
        out_data_file.write((char*)buffer_in_offset,
                            sizeof(VertexID) * num_vertices);
        out_data_file.write((char*)buffer_out_offset,
                            sizeof(VertexID) * num_vertices);
        break;
      case kUndefinedStrategy:
        LOG_ERROR("Store_strategy is undefined");
        break;
    }
    delete buffer_indegree;
    delete buffer_outdegree;

    auto buffer_in_edges = (VertexID*)malloc(sizeof(VertexID) * count_in_edges);
    auto buffer_out_edges =
        (VertexID*)malloc(sizeof(VertexID) * count_out_edges);
    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([i, parallelism, &num_vertices, &buffer_in_edges,
                             &buffer_out_edges, &buffer_in_offset,
                             &csr_vertex_buffer, &buffer_out_offset]() {
        for (VertexID j = i; j < num_vertices; j += parallelism) {
          memcpy(buffer_in_edges + buffer_in_offset[j],
                 csr_vertex_buffer[j].in_edges,
                 csr_vertex_buffer[j].indegree * sizeof(VertexID));
          memcpy(buffer_out_edges + buffer_out_offset[j],
                 csr_vertex_buffer[j].out_edges,
                 csr_vertex_buffer[j].outdegree * sizeof(VertexID));
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
      case kUndefinedStrategy:
        LOG_ERROR("Store_strategy is undefined");
        break;
    }
    delete buffer_in_edges;
    delete buffer_out_edges;

    auto min_vid = csr_vertex_buffer[0].vid;
    auto max_vid = csr_vertex_buffer[num_vertices - 1].vid;
    WriteMax(&global_max_vid, max_vid);
    WriteMin(&global_min_vid, min_vid);

    switch (store_strategy) {
      case kOutgoingOnly:
        WriteAdd(&global_num_edges, count_out_edges);
        subgraph_metadata_vec.push_back(
            {gid, num_vertices, 0, count_out_edges, max_vid, min_vid});
        break;
      case kIncomingOnly:
        WriteAdd(&global_num_edges, count_in_edges);
        subgraph_metadata_vec.push_back(
            {gid, num_vertices, count_in_edges, 0, max_vid, min_vid});
        break;
      case kUnconstrained:
        WriteAdd(&global_num_edges, count_out_edges + count_in_edges);
        subgraph_metadata_vec.push_back({gid, num_vertices, count_in_edges,
                                         count_out_edges, max_vid, min_vid});
        break;
      case kUndefinedStrategy:
        LOG_ERROR("Store_strategy is undefined");
        break;
    }
    gid++;

    delete[] csr_vertex_buffer;
    out_data_file.close();
    out_label_file.close();
  }

  // Write Metadata
  std::ofstream out_meta_file(output_root_path_ + "meta.yaml");
  YAML::Node out_node;
  out_node["GraphMetadata"]["num_vertices"] = global_num_vertices;
  out_node["GraphMetadata"]["num_edges"] = global_num_edges;
  out_node["GraphMetadata"]["max_vid"] = global_max_vid;
  out_node["GraphMetadata"]["min_vid"] = global_min_vid;
  out_node["GraphMetadata"]["num_subgraphs"] = subgraph_vec.size();
  out_node["GraphMetadata"]["subgraphs"] = subgraph_metadata_vec;

  out_meta_file << out_node << std::endl;
  out_meta_file.close();
  return 0;
}

// For vertex cut.
bool IOConverter::WriteSubgraph(
    VertexID** edge_bucket, const GraphMetadata& graph_metadata,
    const std::vector<EdgelistMetadata>& edgelist_metadata_vec,
    StoreStrategy store_strategy) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();

  if (!exists(output_root_path_)) create_directory(output_root_path_);
  if (!exists(output_root_path_ + "graphs"))
    create_directory(output_root_path_ + "graphs");
  if (!exists(output_root_path_ + "label"))
    create_directory(output_root_path_ + "label");

  VertexID n_subgraphs = graph_metadata.get_num_subgraphs();

  std::vector<SubgraphMetadata> subgraph_metadata_vec;
  std::ofstream out_meta_file(output_root_path_ + "meta.yaml");
  for (VertexID i = 0; i < n_subgraphs; i++) {
    std::ofstream out_data_file(output_root_path_ + "graphs/" +
                                std::to_string(i) + ".bin");
    std::ofstream out_label_file(output_root_path_ + "label/" +
                                 std::to_string(i) + ".bin");
    ImmutableCSRGraph csr_graph(i);
    util::format_converter::Edgelist2CSR(
        edge_bucket[i], edgelist_metadata_vec[i], store_strategy, &csr_graph);
    delete edge_bucket[i];
    out_data_file.write((char*)csr_graph.GetGlobalIDBuffer(),
                        sizeof(VertexID) * csr_graph.get_num_vertices());
    switch (store_strategy) {
      case kOutgoingOnly:
        subgraph_metadata_vec.push_back(
            {csr_graph.get_gid(), csr_graph.get_num_vertices(), 0,
             csr_graph.get_num_outgoing_edges(), csr_graph.get_max_vid(),
             csr_graph.get_min_vid()});
        out_data_file.write((char*)csr_graph.GetOutDegree(),
                            sizeof(VertexID) * csr_graph.get_num_vertices());
        out_data_file.write((char*)csr_graph.GetOutOffset(),
                            sizeof(VertexID) * csr_graph.get_num_vertices());
        out_data_file.write(
            (char*)csr_graph.GetOutEdges(),
            sizeof(VertexID) * csr_graph.get_num_outgoing_edges());
        break;
      case kIncomingOnly:
        subgraph_metadata_vec.push_back(
            {csr_graph.get_gid(), csr_graph.get_num_vertices(),
             csr_graph.get_num_incoming_edges(), 0, csr_graph.get_max_vid(),
             csr_graph.get_min_vid()});
        out_data_file.write((char*)csr_graph.GetInDegree(),
                            sizeof(VertexID) * csr_graph.get_num_vertices());
        out_data_file.write((char*)csr_graph.GetInOffset(),
                            sizeof(VertexID) * csr_graph.get_num_vertices());
        out_data_file.write(
            (char*)csr_graph.GetInEdges(),
            sizeof(VertexID) * csr_graph.get_num_incoming_edges());
        break;
      case kUnconstrained:
        out_data_file.write((char*)csr_graph.GetInDegree(),
                            sizeof(VertexID) * csr_graph.get_num_vertices());
        out_data_file.write((char*)csr_graph.GetOutDegree(),
                            sizeof(VertexID) * csr_graph.get_num_vertices());
        out_data_file.write((char*)csr_graph.GetInOffset(),
                            sizeof(VertexID) * csr_graph.get_num_vertices());
        out_data_file.write((char*)csr_graph.GetOutOffset(),
                            sizeof(VertexID) * csr_graph.get_num_vertices());
        out_data_file.write(
            (char*)csr_graph.GetInEdges(),
            sizeof(VertexID) * csr_graph.get_num_outgoing_edges());
        out_data_file.write(
            (char*)csr_graph.GetOutEdges(),
            sizeof(VertexID) * csr_graph.get_num_outgoing_edges());
        subgraph_metadata_vec.push_back(
            {csr_graph.get_gid(), csr_graph.get_num_vertices(),
             csr_graph.get_num_incoming_edges(),
             csr_graph.get_num_outgoing_edges(), csr_graph.get_max_vid(),
             csr_graph.get_min_vid()});
        break;
      default:
        LOG_ERROR("Undefined store strategy.");
        return -1;
    }
    out_data_file.close();
    out_label_file.close();
  }

  // Write metadata
  YAML::Node out_node;
  out_node["GraphMetadata"]["num_vertices"] = graph_metadata.get_num_vertices();
  out_node["GraphMetadata"]["num_edges"] = graph_metadata.get_num_edges();
  out_node["GraphMetadata"]["max_vid"] = graph_metadata.get_max_vid();
  out_node["GraphMetadata"]["min_vid"] = graph_metadata.get_min_vid();
  out_node["GraphMetadata"]["num_subgraphs"] = subgraph_metadata_vec.size();
  out_node["GraphMetadata"]["subgraphs"] = subgraph_metadata_vec;

  out_meta_file << out_node << std::endl;
  out_meta_file.close();
  return false;
}

}  // namespace sics::graph::tools::common