#include "io.h"

using sics::graph::core::common::Bitmap;
using sics::graph::core::common::GraphID;
using sics::graph::core::common::TaskPackage;
using sics::graph::core::common::VertexID;
using sics::graph::core::common::VertexLabel;
using sics::graph::core::data_structures::GraphMetadata;
using sics::graph::core::data_structures::SubgraphMetadata;
using sics::graph::core::util::atomic::WriteAdd;
using sics::graph::core::util::atomic::WriteMax;
using sics::graph::core::util::atomic::WriteMin;
using std::filesystem::create_directory;
using std::filesystem::exists;

namespace tools {

CSRIOAdapter::CSRIOAdapter(const std::string& output_root_path) {
  output_root_path_ = output_root_path;
  if (!std::filesystem::exists(output_root_path))
    std::filesystem::create_directory(output_root_path);
  if (!std::filesystem::exists(output_root_path + "/label"))
    std::filesystem::create_directory(output_root_path + "/label");
  if (!std::filesystem::exists(output_root_path + "/graphs"))
    std::filesystem::create_directory(output_root_path + "/graphs");
};

bool CSRIOAdapter::WriteSubgraph(
    std::vector<folly::ConcurrentHashMap<VertexID, TMPCSRVertex>*>&
        subgraph_vec,
    StoreStrategy store_strategy) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = sics::graph::core::common::ThreadPool(parallelism);
  auto task_package = TaskPackage();

  GraphID gid = 0;
  std::list<SubgraphMetadata> subgraphs;
  size_t global_num_vertices = 0;
  size_t global_num_edges = 0;
  VertexID global_max_vid = 0;
  VertexID global_min_vid = MAX_VERTEX_ID;
  for (auto iter : subgraph_vec) {
    auto vertex_map = iter;
    LOG_INFO(vertex_map->size());

    auto it = vertex_map->begin();
    while (it != vertex_map->end()) {
      LOG_INFO(it->first);
      ++it;
    }

    std::ofstream out_data_file(output_root_path_ + "graphs/" +
                                std::to_string(gid) + ".bin");
    std::ofstream out_label_file(output_root_path_ + "label/" +
                                 std::to_string(gid) + ".bin");

    auto num_vertices = vertex_map->size();
    auto buffer_globalid =
        (VertexID*)malloc(sizeof(VertexID) * vertex_map->size());
    auto buffer_indegree = (size_t*)malloc(sizeof(size_t) * vertex_map->size());
    auto buffer_outdegree =
        (size_t*)malloc(sizeof(size_t) * vertex_map->size());
    size_t count_in_edges = 0, count_out_edges = 0;

    // Serialize subgraph
    auto csr_vertex_buffer =
        (TMPCSRVertex*)malloc(sizeof(TMPCSRVertex) * vertex_map->size());

    size_t count = 0;
    for (auto it = vertex_map->begin(); it != vertex_map->end(); ++it) {
      csr_vertex_buffer[count++] = it->second;
    }

    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([i, parallelism, num_vertices = vertex_map->size(),
                             &buffer_globalid, &buffer_indegree,
                             &buffer_outdegree, &vertex_map, &count_in_edges,
                             &count_out_edges, &csr_vertex_buffer]() {
        for (size_t j = i; j < num_vertices; j += parallelism) {
          auto u = csr_vertex_buffer[j];
          buffer_globalid[j] = u.vid;
          buffer_indegree[j] = u.indegree;
          buffer_outdegree[j] = u.outdegree;
          WriteAdd(&count_out_edges, u.outdegree);
          WriteAdd(&count_in_edges, u.indegree);
        }
        return;
      });
      task_package.push_back(task);
    }
    thread_pool.SubmitSync(task_package);
    task_package.clear();

    out_data_file.write(reinterpret_cast<char*>(buffer_globalid),
                        sizeof(VertexID) * num_vertices);
    auto buffer_vdata =
        (VertexLabel*)malloc(sizeof(VertexLabel) * num_vertices);
    memset(buffer_vdata, 0, sizeof(VertexLabel) * num_vertices);
    out_label_file.write(reinterpret_cast<char*>(buffer_vdata),
                         sizeof(VertexLabel) * num_vertices);
    delete buffer_globalid;
    delete buffer_vdata;
    out_label_file.close();

    auto buffer_in_offset = (size_t*)malloc(sizeof(size_t) * num_vertices);
    auto buffer_out_offset = (size_t*)malloc(sizeof(size_t) * num_vertices);
    memset(buffer_in_offset, 0, sizeof(size_t) * num_vertices);
    memset(buffer_out_offset, 0, sizeof(size_t) * num_vertices);

    // Compute offset for each vertex.
    for (size_t i = 1; i < num_vertices; i++) {
      buffer_in_offset[i] = buffer_in_offset[i - 1] + buffer_indegree[i - 1];
      buffer_out_offset[i] = buffer_out_offset[i - 1] + buffer_outdegree[i - 1];
    }
    delete buffer_indegree;
    delete buffer_outdegree;

    switch (store_strategy) {
      case kOutgoingOnly:
        out_data_file.write((char*)buffer_out_offset,
                            sizeof(size_t) * num_vertices);
        break;
      case kIncomingOnly:
        out_data_file.write((char*)buffer_in_offset,
                            sizeof(size_t) * num_vertices);
        break;
      case kUnconstrained:
        out_data_file.write((char*)buffer_in_offset,
                            sizeof(size_t) * num_vertices);
        out_data_file.write((char*)buffer_out_offset,
                            sizeof(size_t) * num_vertices);
        break;
      default:
        LOG_ERROR("Undefined store strategy.");
        return -1;
    }

    auto buffer_in_edges = (VertexID*)malloc(sizeof(VertexID) * count_in_edges);
    auto buffer_out_edges =
        (VertexID*)malloc(sizeof(VertexID) * count_out_edges);
    for (unsigned int i = 0; i < parallelism; i++) {
      auto task = std::bind([i, parallelism, &num_vertices, &buffer_in_edges,
                             &buffer_out_edges, &buffer_in_offset,
                             &csr_vertex_buffer, &buffer_out_offset]() {
        for (size_t j = i; j < num_vertices; j += parallelism) {
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
      default:
        LOG_ERROR("Undefined store strategy.");
        return -1;
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
        subgraphs.push_front(
            {gid, num_vertices, 0, count_out_edges, max_vid, min_vid});
        break;
      case kIncomingOnly:
        WriteAdd(&global_num_edges, count_in_edges);
        subgraphs.push_front(
            {gid, num_vertices, count_in_edges, 0, max_vid, min_vid});
        break;
      case kUnconstrained:
        WriteAdd(&global_num_edges, count_out_edges + count_in_edges);
        subgraphs.push_front({gid, num_vertices, count_in_edges,
                              count_out_edges, max_vid, min_vid});
        break;
      default:
        LOG_ERROR("Undefined store strategy.");
        return -1;
    }
    gid++;

    delete csr_vertex_buffer;
    out_data_file.close();
  }

  // Write Metadata
  std::ofstream out_meta_file(output_root_path_ + "meta.yaml");
  YAML::Node out_node;
  out_node["GraphMetadata"]["num_vertices"] = global_num_vertices;
  out_node["GraphMetadata"]["num_edges"] = global_num_edges;
  out_node["GraphMetadata"]["max_vid"] = global_max_vid;
  out_node["GraphMetadata"]["min_vid"] = global_min_vid;
  out_node["GraphMetadata"]["num_subgraphs"] = subgraphs.size();

  out_node["GraphMetadata"]["subgraphs"] = subgraphs;
  out_meta_file << out_node << std::endl;

  // Close files.
  out_meta_file.close();
  return 0;
}

}  // namespace tools