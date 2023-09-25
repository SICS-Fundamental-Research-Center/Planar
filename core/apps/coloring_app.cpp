#include "apps/coloring_app.h"

namespace sics::graph::core::apps {

void ColoringApp::PEval() {
  auto color = [this](VertexID src_id, VertexID dst_id) {
    this->ColorNew(src_id, dst_id);
  };

  do {
    active_ = false;
    ParallelEdgeDo(color);
  } while (active_);
}

void ColoringApp::IncEval() {}

void ColoringApp::Assemble() {}

// private
void ColoringApp::Init(VertexID id) {}

void ColoringApp::FindConflictsVertex(VertexID src_id, VertexID dst_id) {}

void ColoringApp::FindConflictsColor(VertexID src_id, VertexID dst_id) {}

void ColoringApp::Color(VertexID id) {}

void ColoringApp::MessagePassing(VertexID id) {}

void ColoringApp::ColorNew(VertexID src_id, VertexID dst_id) {
  VertexData src_data = graph_->ReadLocalVertexDataByID(src_id);
  VertexData dst_data = graph_->ReadLocalVertexDataByID(dst_id);
  if (src_data == dst_data) {
    if (src_id < dst_id) {
      if (graph_->WriteMaxReadDataByID(src_id, src_data + 1))
        update_store_->WriteMax(src_id, src_data + 1);
    } else {
      if (graph_->WriteMaxReadDataByID(dst_id, dst_data + 1))
        update_store_->WriteMax(src_id, dst_data + 1);
    }
    active_ = true;
  }
}

}  // namespace sics::graph::core::apps