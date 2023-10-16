#include "apps/coloring_app.h"

namespace xyz::graph::core::apps {

void ColoringApp::PEval() {
  auto init = [this](VertexID id) { this->Init(id); };
  auto color_vertex = [this](VertexID id) { this->ColorVertex(id); };

  //  graph_->LogVertexData();
  ParallelVertexDo(init);
  LOG_INFO("init finished");
  //  graph_->LogVertexData();
  //  graph_->LogGraphInfo();
  //  update_store_->LogGlobalMessage();
  active_ = 1;
  while (active_) {
    active_ = 0;
    //    ParallelEdgeDo(color_edge);
    ParallelVertexDoStep(color_vertex);
    LOGF_INFO("coloring finished, active: {}", active_);
    //    graph_->LogVertexData();
    //    update_store_->LogGlobalMessage();
    round_++;
  }
  LOGF_INFO("PEval finished, round: {}", round_);
}

void ColoringApp::IncEval() {
  auto message_passing = [this](VertexID id) { this->MessagePassing(id); };
  auto color_vertex = [this](VertexID id) { this->ColorVertex(id); };
  //  graph_->LogVertexData();
  active_ = 0;
  ParallelVertexDo(message_passing);
  LOGF_INFO("message passing finished, active: {}", active_);
  //  graph_->LogVertexData();

  while (active_ != 0) {
    active_ = 0;
    //    ParallelEdgeDo(color_edge);
    ParallelVertexDo(color_vertex);
    LOGF_INFO("coloring finished, active: {}", active_);
    //    graph_->LogVertexData();
    round_++;
  }
  LOGF_INFO("IncEval finished, round: {}", round_);
}

void ColoringApp::Assemble() {}

// private
void ColoringApp::Init(VertexID id) { graph_->WriteMaxReadDataByID(id, 0); }

void ColoringApp::FindConflictsVertex(VertexID src_id, VertexID dst_id) {}

void ColoringApp::FindConflictsColor(VertexID src_id, VertexID dst_id) {}

void ColoringApp::Color(VertexID id) {}

void ColoringApp::MessagePassing(VertexID id) {
  if (graph_->ReadLocalVertexDataByID(id) < update_store_->Read(id)) {
    graph_->WriteMaxReadDataByID(id, update_store_->Read(id));
    //    bitmap_.SetBit(id);
    util::atomic::WriteAdd(&active_, 1);
  }
}

void ColoringApp::ColorEdge(VertexID src_id, VertexID dst_id) {
  VertexData src_data = graph_->ReadLocalVertexDataByID(src_id);
  VertexData dst_data = graph_->ReadLocalVertexDataByID(dst_id);
  if (src_data == dst_data) {
    //    LOGF_INFO("Conflict: {} -> {}; data: {} -> {}", src_id, dst_id,
    //    src_data,
    //              dst_data);
    if (src_id < dst_id) {
      if (graph_->WriteMaxReadDataByID(src_id, src_data + 1))
        update_store_->WriteMax(src_id, src_data + 1);
    } else {
      if (graph_->WriteMaxReadDataByID(dst_id, dst_data + 1))
        update_store_->WriteMax(dst_id, dst_data + 1);
    }
    util::atomic::WriteAdd(&active_, 1);
  }
}

void ColoringApp::ColorVertex(VertexID id) {
  auto degree = graph_->GetOutDegreeByID(id);
  if (degree != 0) {
    auto edges = graph_->GetOutEdgesByID(id);
    bool flag = false;
    for (int i = 0; i < degree; i++) {
      auto dst_id = edges[i];
      if (id < dst_id) {
        VertexData src_data = graph_->ReadLocalVertexDataByID(id);
        VertexData dst_data = graph_->ReadLocalVertexDataByID(dst_id);
        if (src_data == dst_data) {
          flag = true;
          graph_->WriteReadDataByIDWithoutAtomic(id,
                                                 src_data + GetRandomNumber());
        }
      }
    }
    if (flag) {
      VertexData src_data = graph_->ReadLocalVertexDataByID(id);
      update_store_->WriteMax(id, src_data);
      util::atomic::WriteAdd(&active_, 1);
    }
  }
}

int ColoringApp::GetRandomNumber() { return (rand() % max_round_) + 1; }

}  // namespace xyz::graph::core::apps