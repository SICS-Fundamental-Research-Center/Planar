#include "apps/pagerank_app.h"

namespace sics::graph::core::apps {

void PageRankApp::PEval() {
  LOG_INFO("PEval begins!");
  auto count_degree = [this](VertexID id) { CountDegree(id); };

  ParallelVertexDo(count_degree);
  update_store_->SetActive();
  LOG_INFO("PEval finished!");
}

void PageRankApp::IncEval() {
  LOG_INFO("IncEval begins!");
  auto init = [this](VertexID id) { Init(id); };
  auto message_passing = [this](VertexID id) { MessagePassing(id); };
  auto pull = [this](VertexID id) { Pull(id); };

  if (round_ != step) {
    update_store_->ResetWriteBuffer();
  }
  if (round_ == 1) {
    //    LogDegree();
    ParallelVertexDo(init);
    //    graph_->LogVertexData();
    //    graph_->LogGraphInfo();
    //    graph_->LogEdges();
    ParallelVertexDo(pull);
    //    update_store_->LogGlobalMessage();
    //    graph_->LogVertexData();
  } else {
    //    graph_->LogVertexData();
    ParallelVertexDo(message_passing);
    //    graph_->LogVertexData();

    ParallelVertexDo(pull);
    //    graph_->LogVertexData();
    //    update_store_->LogGlobalMessage();
  }

  if (round_ == iter) {
    update_store_->UnsetActive();
  } else {
    update_store_->SetActive();
  }
  step = round_;
  LOG_INFO("IncEval finished!");
}

void PageRankApp::Assemble() {}

void PageRankApp::CountDegree(VertexID id) {
  VertexDegree degree = graph_->GetOutDegreeByID(id);
  id2degree_[id] += degree;
}

void PageRankApp::Init(VertexID id) {
  auto degree = id2degree_[id];
  if (degree != 0) {
    graph_->WriteVertexDataByID(id, 1.0 / degree);
    //    update_store_->Write(id, 1.0 / degree);
  } else {
    graph_->WriteVertexDataByID(id, 1.0 / vertexNum_);
    //    update_store_->Write(id, 1.0 / vertexNum_);
  }
}

void PageRankApp::Pull(VertexID id) {
  auto degree = graph_->GetOutDegreeByID(id);
  if (degree != 0) {
    auto edges = graph_->GetOutEdgesByID(id);
    float sum = 0;
    for (int i = 0; i < degree; i++) {
      sum += graph_->ReadLocalVertexDataByID(edges[i]);
    }
    float pr_new = 0;
    if (round_ == iter) {
      pr_new = kDampingFactor * sum;
    } else {
      pr_new = (kDampingFactor * sum) / id2degree_[id];
    }
    graph_->WriteVertexDataByID(id, pr_new);
    update_store_->WriteAdd(id, pr_new);
  }
}

void PageRankApp::MessagePassing(VertexID id) {
  if (update_store_->IsBorderVertex(id)) {
    auto pr = update_store_->Read(id) + kLambda;
    graph_->WriteVertexDataByID(id, pr);
  } else {
    auto pr = graph_->ReadLocalVertexDataByID(id) + kLambda;
    graph_->WriteVertexDataByID(id, pr);
  }
}

}  // namespace sics::graph::core::apps