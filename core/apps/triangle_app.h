#ifndef GRAPH_SYSTEMS_CORE_APPS_TRIANGLE_APP_H
#define GRAPH_SYSTEMS_CORE_APPS_TRIANGLE_APP_H

#include <fstream>
#include <iostream>
#include <unordered_map>

#include "apis/planar_app_base_op.h"
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

class TriangleAppOp : public apis::PlanarAppBaseOp<uint32_t> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

  struct Pair {
    core::common::VertexID one;
    core::common::VertexID two;
  };

 public:
  TriangleAppOp() : apis::PlanarAppBaseOp<uint32_t>() {}
  ~TriangleAppOp() override { delete[] label; };

  void AppInit(
      common::TaskRunner* runner, data_structures::TwoDMetadata* meta,
      scheduler::EdgeBuffer2* buffer,
      std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs,
      scheduler::MessageHub* hub, scheduler::GraphState* state) override {
    apis::PlanarAppBaseOp<uint32_t>::AppInit(runner, meta, buffer, graphs, hub,
                                             state);
    label = new uint32_t[meta->num_vertices];
    std::ifstream file(
        core::common::Configurations::Get()->root_path + "label.bin",
        std::ios::binary);
    file.read((char*)label, 4 * meta->num_vertices);
    if (!file) {
      LOG_FATAL("Error in open label file!");
    }
    //    for (int i = 0; i < meta->num_vertices; i++) {
    //      LOG_INFO(i, " label: ", label[i]);
    //    }
  }

  void PEval() final {
    auto init = [this](VertexID id) { Init(id); };
    auto triangle_count = [this](VertexID id) { PathTriangle(id); };

    LOG_INFO("PEval begins");
    ParallelVertexDoWithEdges(init);
    //    LogVertexState();
    ParallelVertexDoWithEdges(triangle_count);

    uint32_t sum = 0;
    for (uint32_t i = 0; i < meta_->num_vertices; i++) {
      sum += Read(i);
    }
    LOG_INFO("triangle count for self citation is: ", sum);

    UnsetActive();
    LOG_INFO("PEval finishes");
  }

  void IncEval() final {
    LOG_INFO("IncEval begins");
    LOG_INFO("IncEval finishes");
  }

  void Assemble() final {}

 private:
  void Init(VertexID id) { Write(id, 0); }

  // 0 -> researcher
  // 1 -> paper
  // 2 -> journal
  // 3 -> conference
  // 4 -> city

  void FindSelf(VertexID src) {
    if (label[src] != 1) return;
    auto degree = GetOutDegree(src);
    if (degree == 0) return;
    auto edges = GetOutEdges(src);
    for (VertexDegree i = 0; i < degree; i++) {
      auto dst = edges[i];
      if (label[dst] != 1) continue;
      AddSelfCitation(src, dst);
    }
  }

  void TriangleCount(VertexID src) {
    if (label[src] != 0) return;  // src should be a researcher
    auto degree = GetOutDegree(src);
    if (degree < 2) return;
    auto edges = GetOutEdges(src);
    uint32_t sum = 0;

    std::vector<VertexID> neis;
    for (VertexDegree i = 0; i < degree; i++) {
      neis.push_back(edges[i]);
    }

    for (VertexDegree i = 0; i < degree; i++) {
      auto one = edges[i];
      if (label[one] != 1 || triangles.find(one) != triangles.end()) continue;
      auto& vec = triangles[src];

      for (VertexDegree k = 0; k < degree; k++) {
        if (k == i) continue;
        if (label[k] != 1)
          for (int j = 0; j < vec.size(); j++) {
          }
        //        if (j == i) continue;
        //        if (label[j] != 1) continue;
        //        auto two = edges[j];
        //        if (IsNeighbor(one, two)) {
        //          sum++;
        //        }
      }
    }
    Write(src, sum);
  }

  void PathTriangle(VertexID src) {
    auto degree = GetOutDegree(src);
    if (degree == 0) return;
    auto edges = GetOutEdges(src);
    uint32_t sum = 0;
    for (VertexDegree i = 0; i < degree; i++) {
      auto dst = edges[i];
      auto d2 = GetOutDegree(dst);
      if (d2 == 0) continue;
      auto e2 = GetOutEdges(dst);
      for (VertexDegree j = 0; j < d2; j++) {
        auto three = e2[j];
        auto d3 = GetOutDegree(three);
        if (d3 == 0) continue;
        auto e3 = GetOutEdges(three);
        for (VertexDegree k = 0; k < d3; k++) {
          auto final = e3[k];
          if (final == src) {
            sum++;
            break;
          }
        }
      }
    }
    Write(src, sum);
  }

  void AddSelfCitation(VertexID src, VertexID dst) {
    std::lock_guard<std::mutex> lck(mtx);
    if (triangles.find(src) != triangles.end()) {
      triangles[src].push_back(dst);
    } else {
      triangles.insert({src, std::vector<VertexID>(dst)});
    }
  }

 private:
  std::mutex mtx;
  std::condition_variable cv;

  std::unordered_map<VertexID, std::vector<VertexID>> triangles;

  uint32_t* label;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_TRIANGLE_APP_H
