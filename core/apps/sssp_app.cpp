#include "apps/sssp_app.h"

namespace sics::graph::core::apps {

void SsspApp::PEval() {
  auto init = [this](VertexID id) { this->Init(id); };
  auto relax = [this](VertexID id) { this->Relax(id); };

  auto init_im = [this](VertexID id) { this->Init_im(id); };
  auto pull_im = [this](VertexID id) { this->Relax_im(id); };

  active_.Init(graph_->GetVertexNums());
  active_next_.Init(graph_->GetVertexNums());
  active_.Clear();
  active_next_.Clear();
  //  graph_->LogGraphInfo();
  //  graph_->LogVertexData();
  if (radical_) {
    ParallelVertexDo(init_im);
    while (active_.Count() != 0) {
      ParallelVertexDoWithActive(pull_im);
      LOGF_INFO("pull_im finished, active: {}", active_.Count());
      SyncActive();
    }
    update_store_->UnsetActive();
  } else {
    ParallelVertexDo(init);
    LOGF_INFO("init finished, active: {}", active_.Count());
    //  graph_->LogVertexData();

    while (active_.Count() != 0) {
      ParallelVertexDoWithActive(relax);
      SyncActive();
      LOGF_INFO("relax finished, active: {}", active_.Count());
      //    if (active_.Count() <= 10) {
      ////      LogActive();
      //      flag = true;
      //    } else {
      //      flag = false;
      //    }
      //    graph_->LogVertexData();
    }
  }
}

void SsspApp::IncEval() {
  auto message_passing = [this](VertexID id) { this->MessagePassing(id); };
  auto relax = [this](VertexID id) { this->Relax(id); };
  active_.Init(graph_->GetVertexNums());
  active_next_.Init(graph_->GetVertexNums());
  active_.Clear();
  active_next_.Clear();
  //  update_store_->LogGlobalMessage();
  //  graph_->LogVertexData();
  ParallelVertexDo(message_passing);
  LOGF_INFO("message passing finished, active: {}", active_.Count());
  //  graph_->LogVertexData();
  //  flag = false;
  while (active_.Count() != 0) {
    ParallelVertexDoWithActive(relax);
    SyncActive();
    LOGF_INFO("relax finished, active: {}", active_.Count());
    //    if (active <= 10) {
    //      //      LogActive();
    //      flag = true;
    //    } else {
    //      flag = false;
    //    }
    //    graph_->LogVertexData();
  }
}

void SsspApp::Assemble() {}

void SsspApp::Init(VertexID id) {
  if (id == source_) {
    graph_->WriteVertexDataByID(id, 0);
    update_store_->WriteMinBorderVertex(id, 0);
    active_.SetBit(graph_->GetVertexIndexByID(id));
    LOGF_INFO("source of SSSP: {}", id);
  } else {
    graph_->WriteVertexDataByID(id, SSSP_INFINITY);
  }
}

void SsspApp::Init_im(VertexID id) {
  if (id == source_) {
    graph_->WriteVertexDataDirect(id, 0);
    active_.SetBit(id);
  } else {
    graph_->WriteVertexDataDirect(id, SSSP_INFINITY);
  }
}

void SsspApp::Relax(VertexID id) {
  // push to neighbors
  auto edges = graph_->GetOutEdgesByID(id);
  auto degree = graph_->GetOutDegreeByID(id);
  auto current_distance = graph_->ReadLocalVertexDataByID(id) + 1;
  for (VertexDegree i = 0; i < degree; i++) {
    auto dst_id = edges[i];
    auto data = graph_->ReadLocalVertexDataByID(dst_id);
    if (current_distance < data) {
      if (graph_->WriteMinVertexDataByID(dst_id, current_distance)) {
        update_store_->WriteMinBorderVertex(dst_id, current_distance);
        active_next_.SetBit(graph_->GetVertexIndexByID(dst_id));
        //          if (flag)
        //            LOGF_INFO(" === do this relax = {} -> {}, distance {},
        //            dst {}", id,
        //                      dst_id, current_distance, data);
      }
    }
  }
}

void SsspApp::Relax_im(VertexID id) {
  auto edges = graph_->GetOutEdgesDirect(id);
  auto degree = graph_->GetOutDegreeDirect(id);
  auto current_distance = graph_->ReadVertexDataDirect(id) + 1;
  for (VertexDegree i = 0; i < degree; i++) {
    auto dst_id = edges[i];
    auto data = graph_->ReadVertexDataDirect(dst_id);
    if (current_distance < data) {
      if (graph_->WriteVertexDataDirect(dst_id, current_distance)) {
        active_next_.SetBit(dst_id);
      }
    }
  }
}

void SsspApp::MessagePassing(VertexID id) {
  if (graph_->WriteMinVertexDataByID(id, update_store_->Read(id)))
    active_.SetBit(graph_->GetVertexIndexByID(id));
}

void SsspApp::LogActive() {
  for (size_t i = 0; i < active_.size(); i++) {
    if (active_.GetBit(i)) {
      VertexData tmp = graph_->ReadLocalVertexDataByID(i);
      auto edges = graph_->GetOutEdgesByID(i);
      std::string edges_str = "";
      for (VertexDegree j = 0; j < graph_->GetOutDegreeByID(i); j++) {
        //        auto edges2 = graph_->GetOutEdgesByID(edges[j]);
        //        std::string tmp2 = "";
        //        for (int k = 0; k < graph_->GetOutDegreeByID(edges[j]); k++) {
        //          auto data2 = graph_->ReadLocalVertexDataByID(edges2[k]);
        //          tmp2 += std::to_string(edges2[k]) + ":" +
        //          std::to_string(data2) + " ";
        //        }
        //        LOGF_INFO("neighbor {}, outedgse: {}", edges[i], tmp2);
        auto tmp1 = graph_->ReadLocalVertexDataByID(edges[j]);
        edges_str +=
            std::to_string(edges[j]) + ":" + std::to_string(tmp1) + " ";
      }
      LOGF_INFO("next round active vertex {}:{}, outedges: {}", i, tmp,
                edges_str);
    }
  }
}

}  // namespace sics::graph::core::apps
