#ifndef GRAPH_SYSTEMS_CORE_APPS_PAGERANK_APP_OP_H_
#define GRAPH_SYSTEMS_CORE_APPS_PAGERANK_APP_OP_H_

#include "apis/planar_app_base.h"
#include "apis/planar_app_op.h"
#include "common/bitmap.h"
#include "common/types.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::graph::core::apps {

class PageRankOpApp : public apis::PlanarAppOpBase<
                          data_structures::graph::MutableBlockCSRGraphFloat> {
  using VertexIndex = common::VertexIndex;
  using EdgeIndex = common::EdgeIndex;
  using VertexID = common::VertexID;
  using VertexDegree = common::VertexDegree;

 public:
  using VertexData = CSRGraph::VertexData;
  using EdgeData = CSRGraph::EdgeData;

  PageRankOpApp() = default;
  ~PageRankOpApp() override = default;

  void AppInit(const std::string& root_path) {
    iter = core::common::Configurations::Get()->pr_iter;
    in_memory_ = core::common::Configurations::Get()->in_memory;
  }

  void PEval(){

  };

  void IncEval(){

  };

  void Assemble(){

  };

 private:
  void Init(VertexID id) {
    auto degree = 0;

  }

  void Pull(VertexID id) {}

 private:
  const float kDampingFactor = 0.85;
  const float kLambda = 0.15;
  const float kEpsilon = 1e-6;
  int step = 0;
  uint32_t iter = 10;

  bool in_memory_ = false;
};

}  // namespace sics::graph::core::apps

#endif  // GRAPH_SYSTEMS_CORE_APPS_PAGERANK_APP_OP_H_
