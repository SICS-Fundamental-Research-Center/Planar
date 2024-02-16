#ifndef GRAPH_SYSTEMS_NVME_APPS_WCC_NVME_APP_H_
#define GRAPH_SYSTEMS_NVME_APPS_WCC_NVME_APP_H_

#include "core/apis/planar_app_base.h"
#include "nvme/apis/block_api.h"
#include "nvme/data_structures/graph/pram_block.h"

namespace sics::graph::nvme::apps {

using BlockGraph = data_structures::graph::BlockCSRGraphUInt32;

class WCCNvmeApp : public apis::BlockModel {
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexID = core::common::VertexID;

 public:
  using VertexData = typename BlockGraph ::VertexData;
  using EdgeData = typename BlockGraph ::EdgeData;
  WCCNvmeApp() = default;
  WCCNvmeApp(const std::string& root_path) : BlockModel(root_path) {}
  ~WCCNvmeApp() override = default;

  // actually, this is no use.
  void Init(VertexID id) { this->Write(id, id); }

  void Graft(VertexID src_id, VertexID dst_id) {
    VertexID src_parent_id = Read(src_id);
    VertexID dst_parent_id = Read(dst_id);

    if (src_parent_id < dst_parent_id) {
      this->WriteMin(dst_parent_id, src_parent_id);
    } else if (src_parent_id > dst_parent_id) {
      this->WriteMin(src_parent_id, dst_parent_id);
    }
  }

  void PointJump(VertexID src_id) {
    VertexID parent_id = Read(src_id);
    if (parent_id == src_id) {
      return;
    } else {
      while (parent_id != Read(parent_id)) {
        parent_id = Read(parent_id);
      }
      this->WriteMin(src_id, parent_id);
    }
  }

  void Contract(VertexID src_id, VertexID dst_id, EdgeIndex idx) {
    auto src_parent_id = Read(src_id);
    auto dst_parent_id = Read(dst_id);
    if (src_parent_id == dst_parent_id) {
      this->DeleteEdge(src_id, dst_id, idx);
    }
  }

  // TODO:
  // delete pointer 'this' in anonymous namespace
  void Compute() override {
    LOG_INFO("WCCNvmeApp::Compute() begin");
    std::function<void(VertexID)> init = [this](VertexID id) { Init(id); };
    std::function<void(VertexID, VertexID)> graft =
        [this](VertexID src_id, VertexID dst_id) { Graft(src_id, dst_id); };
    std::function<void(VertexID)> point_jump = [this](VertexID src_id) {
      PointJump(src_id);
    };
    std::function<void(VertexID, VertexID, EdgeIndex)> contract =
        [this](VertexID src_id, VertexID dst_id, EdgeIndex idx) {
          Contract(src_id, dst_id, idx);
        };

    //    update_store_->LogVertexData();
    //    MapVertex(&init);
    //    update_store_->LogVertexData();

    while (true) {
      //      update_store_->LogVertexData();
      MapEdge(&graft);
      //      update_store_->LogVertexData();

      MapVertex(&point_jump);
      //      update_store_->LogVertexData();

      //      update_store_->LogEdgeDelInfo();
      MapAndMutateEdge(&contract);
      //      update_store_->LogVertexData();
      //      update_store_->LogEdgeDelInfo();

      if (update_store_->GetLeftEdges() == 0) {
        LOG_INFO("======= Round end, no edges left =======");
        break;
      } else {
        LOGF_INFO("======= Round end, left edges num: {} =======",
                  update_store_->GetLeftEdges());
      }
    }
    LOG_INFO("WCCNvmeApp::Compute() end");
  }

 private:
};

}  // namespace sics::graph::nvme::apps
#endif  // GRAPH_SYSTEMS_NVME_APPS_WCC_NVME_APP_H_
