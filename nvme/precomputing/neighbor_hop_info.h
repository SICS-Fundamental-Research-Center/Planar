#ifndef GRAPH_SYSTEMS_NVME_PRECOMPUTING_NEIGHBOR_HOP_INFO_H_
#define GRAPH_SYSTEMS_NVME_PRECOMPUTING_NEIGHBOR_HOP_INFO_H_

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <mutex>
#include <queue>
#include <unordered_map>

#include "core/common/bitmap.h"
#include "core/common/multithreading/thread_pool.h"
#include "nvme/precomputing/basic.h"

namespace sics::graph::nvme::precomputing {

void ComputeNeighborInfo(std::string& root_path, uint32_t parallelism = 1) {
  // make sure the precomputing directory exists
  std::string precomputing_dir = root_path + "/precomputing";
  if (!fs::exists(precomputing_dir)) {
    if (!fs::create_directories(precomputing_dir)) {
      LOGF_FATAL("Failed creating directory: {}", precomputing_dir.c_str());
    }
  }

  // read GraphMetadata
  core::data_structures::BlockMetadata metadata(root_path);

  LOG_INFO("Begin counting two-hop neighbors");
  data_structures::graph::BlockCSRGraph graph(root_path, &metadata);
  data_structures::NeighborHopInfo hop_info(root_path, &metadata);
  core::common::ThreadPool pool(parallelism);

  core::common::TaskPackage tasks;
  // iter block j for the join of block i

  auto num_vertices = graph.GetVerticesNum();
  auto task_size = (num_vertices + parallelism - 1) / parallelism;
  LOGF_INFO("task size: {}", task_size);
  // iter vertex in block i
  VertexID k = 0;
  while (k < num_vertices) {
    auto b_k = k;
    auto e_k = std::min(k + task_size, num_vertices);
    auto task = [&graph, &hop_info, b_k, e_k]() {
      auto idx = 0;
      for (auto k = b_k; k < e_k; k++) {
        auto degree_k = graph.GetOutDegree(k);
        if (degree_k == 0) continue;
        auto one_hop_edges = graph.GetOutEdges(k);
        for (VertexID l = 0; l < degree_k; l++) {
          auto hop_1 = one_hop_edges[l];
          hop_info.UpdateOneHopInfo(k, hop_1);
          auto degree_2 = graph.GetOutDegree(hop_1);
          if (degree_2 == 0) continue;
          auto two_hop_edges = graph.GetOutEdges(hop_1);
          for (VertexDegree m = 0; m < degree_2; m++) {
            hop_info.UpdateTwoHopInfo(k, two_hop_edges[m]);
          }
        }
        idx++;
        if (idx % 10000 == 0) {
          LOGF_INFO("{}->{}: Processed {} vertices", b_k, e_k, idx);
        }
      }
    };
    tasks.push_back(task);
    k = e_k;
  }
  LOGF_INFO("Task num: {}", tasks.size());
  pool.SubmitSync(tasks);

  LOG_INFO("Begin write two-hop info to disk");
  // write two-hop info of block i to disk
  hop_info.SerializeToDisk();

  LOG_INFO("Two-hop neighbors are precomputed.");
}

struct TriangleTmp {
  VertexID a;
  VertexID b;
};

struct TwoNeighbors {
  VertexID first;
  VertexID second;
};

void PatternIndex(std::string& root_path, uint32_t parallelism = 1,
                  uint32_t task_factor = 100) {
  // make sure the precomputing directory exists
  std::string precomputing_dir = root_path + "/precomputing";
  if (!fs::exists(precomputing_dir)) {
    if (!fs::create_directories(precomputing_dir)) {
      LOGF_FATAL("Failed creating directory: {}", precomputing_dir.c_str());
    }
  }
  // read GraphMetadata
  core::data_structures::BlockMetadata metadata(root_path);
  nvme::data_structures::IndexMeta index_meta;

  LOG_INFO("Begin counting two-hop neighbors");
  data_structures::graph::BlockCSRGraph graph(root_path, &metadata);
  graph.ReadAllSubBlock();
  precomputing::BlockIndexInfo block_info(root_path, &metadata);
  core::common::ThreadPool pool(parallelism);

  // path
  // 1. a -> b -> c and b has no other neighbor except a and c
  // 2. a <-> b <-> c and
  {
    auto begin_time = std::chrono::system_clock::now();
    core::common::TaskPackage tasks;
    // 1. filtering all vertex of degree = 2
    std::vector<Path> path_tmp;
    for (uint32_t id = 0; id < metadata.num_vertices; id++) {
      auto degree = graph.GetOutDegree(id);
      if (degree == 0) continue;
      auto edges = graph.GetOutEdges(id);
      Path p;
      for (uint32_t j = 0; j < degree; j++) {
        p.a = id;
        p.b = edges[j];
        path_tmp.push_back(p);
      }
    }

    std::mutex mtx;
    std::vector<Path> paths;
    auto task_num = parallelism * task_factor;
    auto task_size = (path_tmp.size() + task_num - 1) / task_num;
    task_size = task_size < 10 ? 10 : task_size;
    uint64_t idx_begin = 0, idx_end = 0;
    for (; idx_end < path_tmp.size();) {
      idx_end += task_size;
      if (idx_end > path_tmp.size()) idx_end = path_tmp.size();
      auto task = [&graph, &path_tmp, &paths, &mtx, idx_begin, idx_end]() {
        for (uint64_t idx = idx_begin; idx < idx_end; idx++) {
          auto path = path_tmp[idx];
          auto degree = graph.GetOutDegree(path.b);
          if (degree == 0 || degree > 2) continue;
          auto edges = graph.GetOutEdges(path.b);
          if (degree == 1) {
            if (edges[0] != path.a) {
              path.c = edges[0];
              std::lock_guard<std::mutex> lock(mtx);
              paths.push_back(path);
            }
          } else {
            if (edges[0] != path.a && edges[1] != path.a) continue;
            if (edges[0] == path.a && edges[1] == path.a) {
              LOG_INFO("Error(duplicated edges) in edges list of vertex: ",
                       path.b);
              continue;
            }
            if (edges[0] == path.a) {
              std::lock_guard<std::mutex> lock(mtx);
              path.c = edges[1];
              paths.push_back(path);
            }
            if (edges[1] == path.a) {
              std::lock_guard<std::mutex> lock(mtx);
              path.c = edges[0];
              paths.push_back(path);
            }
          }
        }
      };
      tasks.push_back(task);
      idx_begin = idx_end;
    }
    pool.SubmitSync(tasks);
    path_tmp.clear();
    auto end_time = std::chrono::system_clock::now();
    LOG_INFO("Time for count 3-path: ",
             std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                                   begin_time)
                 .count(),
             " ms");
    index_meta.num_paths = paths.size();
    LOG_INFO("Number of paths: ", paths.size());
    // serialize to disk
    std::string path_file = root_path + "precomputing/paths.bin";
    std::ofstream ofs(path_file, std::ios::binary);
    ofs.write(reinterpret_cast<const char*>(paths.data()),
              sizeof(Path) * paths.size());
    if (!ofs) {
      LOG_FATAL("Write file error, ", path_file.c_str());
    }
    ofs.close();
    paths.clear();
  }
  // star4 (exactly four star. 5-star or higher is not considered!)
  {
    auto begin_time = std::chrono::system_clock::now();
    auto task_size = (metadata.num_vertices + parallelism - 1) / parallelism;
    core::common::TaskPackage tasks;
    std::mutex mtx;
    std::vector<Star4> star4s;

    uint32_t bid = 0, eid = 0;
    for (; eid < metadata.num_vertices;) {
      eid += task_size;
      if (eid > metadata.num_vertices) eid = metadata.num_vertices;
      auto task = [&graph, &star4s, &mtx, bid, eid]() {
        for (auto id = bid; id < eid; id++) {
          auto degree = graph.GetOutDegree(id);
          if (degree != 4) continue;
          auto edges = graph.GetOutEdges(id);
          Star4 tmp(id, edges[0], edges[1], edges[2], edges[3]);
          {
            std::lock_guard<std::mutex> lock(mtx);
            star4s.push_back(tmp);
          }
        }
      };
      tasks.push_back(task);
      bid = eid;
    }
    pool.SubmitSync(tasks);
    auto end_time = std::chrono::system_clock::now();
    LOG_INFO("Time for count star4: ",
             std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                                   begin_time)
                 .count(),
             " ms");
    index_meta.num_stars4 = star4s.size();
    LOG_INFO("Number of star4: ", star4s.size());
    // serialize to disk
    std::string star4_file = root_path + "precomputing/star4s.bin";
    std::ofstream ofs(star4_file, std::ios::binary);
    ofs.write(reinterpret_cast<const char*>(star4s.data()),
              sizeof(Star4) * star4s.size());
    if (!ofs) {
      LOG_FATAL("Write file error, ", star4_file.c_str());
    }
    ofs.close();
    star4s.clear();
  }

  // TODO: checko in large dataset
  // triangle (a->b->c->a)
  {
    auto begin_time = std::chrono::system_clock::now();
    std::vector<TriangleTmp> triangle;
    std::mutex mtx;
    core::common::TaskPackage tasks;
    std::mutex map_mtx;
    //    std::unordered_map<TwoNeighbors, bool> is_neighbor;

    // 1. first iter for a and b in Triangle
    for (auto id = 0; id < metadata.num_vertices; id++) {
      auto degree = graph.GetOutDegree(id);
      if (degree == 0) continue;
      auto edges = graph.GetOutEdges(id);
      for (auto j = 0; j < degree; j++) {
        TriangleTmp t;
        t.a = id;
        t.b = edges[j];
        if (t.b > t.a) {
          triangle.push_back(t);
        }
      }
    }

    // 2. resort triangle for destination vertex hash
    std::sort(triangle.begin(), triangle.end(),
              [](const auto& first, const auto& second) {
                if (first.b != second.b) {
                  return first.b < second.b;  // Compare second elements first
                } else {
                  return first.a < second.a;  // If second elements are equal,
                                              // compare first elements
                }
              });

    // 3. cut task for parallel execution
    std::vector<Triangle> triangle_mtx;
    std::vector<std::vector<Triangle>> triangles_tmp(parallelism);
    auto task_size = (triangle.size() + parallelism - 1) / parallelism;
    task_size = task_size < 10 ? 10 : task_size;
    LOG_INFO("Task size: ", task_size);
    uint64_t idx_begin = 0, idx_end = 0;
    uint64_t i = 0;
    for (; idx_end < triangle.size();) {
      idx_end += task_size;
      if (idx_end > triangle.size()) idx_end = triangle.size();
      auto& tri_queue = triangles_tmp[i];
      auto task = [&graph, &triangle, &tri_queue, idx_begin, idx_end]() {
        Triangle t;
        for (uint64_t i = idx_begin; i < idx_end; i++) {
          auto& tri = triangle[i];
          auto b = tri.b;
          auto degree = graph.GetOutDegree(b);
          if (degree == 0) continue;
          auto edges = graph.GetOutEdges(b);
          for (uint32_t j = 0; j < degree; j++) {
            t.c = edges[j];
            if (t.c == tri.a) continue;
            t.a = tri.a;
            t.b = tri.b;
            // check if degree of c is zero
            if (graph.GetOutDegree(t.c) != 0 && t.c > t.b) {
              tri_queue.push_back(t);
            }
          }
        }
      };
      tasks.push_back(task);
      idx_begin = idx_end;
      i++;
    }
    LOG_INFO("Number of task: ", tasks.size(), " Parallelism: ", parallelism);
    pool.SubmitSync(tasks);
    triangle.clear();
    uint64_t size = 0;
    for (auto& tri : triangles_tmp) {
      triangle_mtx.insert(triangle_mtx.end(), tri.begin(), tri.end());
      tri.clear();
    }
    LOG_INFO("Size of triangle: ", triangle_mtx.size());

    // 4. check real triangle in candidates
    auto task_num = parallelism * task_factor;
    task_size = (triangle_mtx.size() + task_num - 1) / task_num;
    task_size = task_size < 10 ? 10 : task_size;

    tasks.clear();
    idx_begin = 0;
    idx_end = 0;
    i = 0;
    std::vector<Triangle> final;
    for (; idx_end < triangle_mtx.size();) {
      idx_end += task_size;
      if (idx_end > triangle_mtx.size()) idx_end = triangle_mtx.size();
      auto task = [&graph, &triangle_mtx, &final, &mtx, idx_begin, idx_end]() {
        for (uint64_t i = idx_begin; i < idx_end; i++) {
          auto& tri = triangle_mtx[i];
          auto degree = graph.GetOutDegree(tri.c);
          if (degree == 0) continue;
          auto edges = graph.GetOutEdges(tri.c);
          for (int j = 0; j < degree; j++) {
            if (edges[j] == tri.a) {
              std::lock_guard<std::mutex> lock(mtx);
              final.push_back(tri);
              break;
            }
          }
        }
      };
      tasks.push_back(task);
      idx_begin = idx_end;
      i++;
    }
    pool.SubmitSync(tasks);
    auto end_time = std::chrono::system_clock::now();
    LOG_INFO("Time for count triangle: ",
             std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                                   begin_time)
                 .count(),
             " ms");

    LOG_INFO("Size of final tri: ", final.size());
    // serialize to disk
    index_meta.num_triangles = final.size();
    std::string tri_file = root_path + "precomputing/triangles.bin";
    std::ofstream ofs(tri_file, std::ios::binary);
    ofs.write(reinterpret_cast<const char*>(final.data()),
              sizeof(Triangle) * final.size());
    if (!ofs) {
      LOG_FATAL("Write file error, ", tri_file.c_str());
    }
    ofs.close();
    final.clear();
    //    for (auto i = 0; i < final.size(); i++) {
    //      auto& tri = final[i];
    //      LOG_INFO(tri.a, " -> ", tri.b, " -> ", tri.c);
    //    }
  }

  YAML::Node meta_yaml;
  meta_yaml["IndexMeta"] = index_meta;
  std::ofstream meta_file_name(root_path + "/precomputing/index_meta.yaml");
  meta_file_name << meta_yaml;
  meta_file_name.close();
  LOG_INFO("Index meta is written to disk.");
}

}  // namespace sics::graph::nvme::precomputing

#endif  // GRAPH_SYSTEMS_NVME_PRECOMPUTING_NEIGHBOR_HOP_INFO_H_
