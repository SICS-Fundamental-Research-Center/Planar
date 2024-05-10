#include "apps/gnn_app.h"

namespace sics::graph::core::apps {

void GNNApp::PEval() {
  LOG_INFO("PEval begins!");
  auto init = [this](VertexID id) { this->Init(id); };
  //  graph_->LogEdges();
  //  update_store_->LogBorderVertexInfo();
  ParallelVertexDo(init);
  update_store_->SetActive();
  //  graph_->LogVertexData();
  //  update_store_->LogGlobalMessage();
  LOG_INFO("PEval finished!");
}

void GNNApp::IncEval() {
  LOG_INFO("IncEval begins!");
  auto message_passing = [this](VertexID id) { this->MessagePassing(id); };
  auto forward = [this](VertexID id) { this->Forward(id); };

  if (round_ != step) {
    update_store_->ResetWriteBuffer();
  }

  if (round_ == 1) {
    //    LogDegree();
    ParallelVertexDo(forward);
    //    graph_->LogVertexData();
    //    update_store_->LogGlobalMessage();
  } else {
    ParallelVertexDo(message_passing);
    LOG_INFO("Message passing finished!");
    //    graph_->LogVertexData();
    ParallelVertexDo(forward);
    LOG_INFO("Forward finished!");
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

void GNNApp::Assemble() {}

void GNNApp::Init(VertexID id) {
  VertexDegree degree = graph_->GetOutDegreeByID(id);
  // One subgraph one time, so not need to use mutex and lock.
  id2degree_[id] += degree;
  graph_->WriteVertexDataByID(id, rand_float());
}

void GNNApp::Forward(VertexID id) {
  // w*u + B*v
  auto degree = graph_->GetOutDegreeByID(id);
  if (degree != 0) {
    auto edges = graph_->GetOutEdgesByID(id);
    float u = 0;
    for (VertexDegree i = 0; i < degree; i++) {
      if (if_take()) {
        u += graph_->ReadLocalVertexDataByID(edges[i]);
      }
    }
    float tmp = spmv(w, u);
    //    float v_new = sigmod(tmp / id2degree_[id]);
    float v_new = tmp / id2degree_[id];
    graph_->WriteVertexDataByID(id, v_new);
    update_store_->WriteAdd(id, v_new);
  }
}

void GNNApp::MessagePassing(VertexID id) {
  if (update_store_->IsBorderVertex(id)) {
    auto data = update_store_->Read(id);
    graph_->WriteVertexDataByID(id, data);
  } else {
  }
}

float GNNApp::sigmod(float x) { return 1 / (1 + exp(-x)); }

float GNNApp::spmv(float w, float v) { return w * v; }

}  // namespace sics::graph::core::apps