#ifndef GRAPH_SYSTEMS_NVME_APPS_GNN_NVME_INT_APP_H_
#define GRAPH_SYSTEMS_NVME_APPS_GNN_NVME_INT_APP_H_

#include <cstdlib>
#include <random>

#include "core/apis/planar_app_base.h"
#include "core/common/types.h"
#include "nvme/apis/block_api.h"
#include "nvme/data_structures/graph/pram_block.h"

namespace sics::graph::nvme::apps {

struct GNNWeightDataInt {
 public:
  GNNWeightDataInt() : m_(nullptr) {}
  GNNWeightDataInt(uint32_t num) : l_(num) {
    m_ = new int*[num];
    for (uint32_t i = 0; i < num; i++) {
      m_[i] = new int[num];
    }
  }
  void Show() {
    LOG_INFO("weights info: ");
    for (uint32_t i = 0; i < l_; i++) {
      std::string line = "";
      for (uint32_t j = 0; j < l_; j++) {
        line += std::to_string(m_[i][j]) + " ";
      }
      LOG_INFO(line);
    }
  }

 public:
  uint32_t l_;
  int** m_ = nullptr;
};

template <int N>
struct GNNVertexDataInt {
 public:
  GNNVertexDataInt() {}
  GNNVertexDataInt(int i) {}
  int features[N];
};

class GNNIntApp : public apis::BlockModel<int> {
  using VertexIndex = core::common::VertexIndex;
  using EdgeIndex = core::common::EdgeIndex;
  using VertexID = core::common::VertexID;

  using FuncVertex = core::common::FuncVertex;
  using FuncEdge = core::common::FuncEdge;
  using FuncEdgeMutate = core::common::FuncEdgeMutate;

 public:
  GNNIntApp() = default;
  GNNIntApp(const std::string& root_path)
      : BlockModel(root_path),
        gnn_l(core::common::Configurations::Get()->gnn_l),
        gnn_k(core::common::Configurations::Get()->gnn_k),
        iter(core::common::Configurations::Get()->iter) {
    // init weights
    srand(0);
    for (uint32_t i = 0; i < gnn_k; i++) {
      weights_.emplace_back(GNNWeightDataInt(gnn_l));
      for (uint32_t j = 0; j < gnn_l; j++) {
        for (uint32_t k = 0; k < gnn_l; k++) {
          weights_[i].m_[j][k] = rand_int();
          //          weights_[i].m_[j][k] = 0.5;
        }
      }
    }
    num_ = update_store_.GetMessageCount();
    read_data_ = new int[gnn_l * num_];
    write_data_ = new int[gnn_l * num_];
  }
  ~GNNIntApp() override {
    if (read_data_) {
      delete[] read_data_;
    }
    if (write_data_) {
      delete[] write_data_;
    }
  };

  void Init(VertexID id) {
    auto vdata = Write1(id);
    for (uint32_t i = 0; i < gnn_l; i++) {
      vdata[i] = rand_int();
    }
  }

  void Forward(VertexID id) {
    auto degree = this->GetOutDegree(id);
    if (degree == 0) return;
    auto edges = this->GetEdges(id);
    auto h_u_0 = Read1(id);

    int* tmp = new int[gnn_l];
    for (uint32_t i = 0; i < degree; i++) {
      auto h_v_0 = Read1(edges[i]);
      Add(h_v_0, tmp, gnn_l);
    }
    Add(h_u_0, tmp, gnn_l);
    Divide(tmp, degree + 1, gnn_l);
    // spmv
    auto wdata = Write1(id);
    Spmv(weights_[k_].m_, tmp, wdata, gnn_l);
    // activate
    //    for (int i = 0; i < gnn_l; i++) {
    //      wdata[i] = wdata[i] > 0 ? wdata[i] : 0;
    //    }
  }

  void Compute() override {
    LOG_INFO("GNN Compute() begin!");
    FuncVertex init = [&](VertexID id) { Init(id); };
    FuncVertex forward = [&](VertexID id) { Forward(id); };

    MapVertex(&init);
    sync();
    //    LogData();
    for (uint32_t j = 0; j < iter; j++) {
      MapVertex(&forward);
      sync();
      //      LogData();
      //      weights_[k_].Show();
      LOGF_INFO(" =========== iter: {} finished! ===========", j);
    }
    LOG_INFO("GNN Compute() end!");
  }

  float rand_int() {
    // generate random int number between -10 and 10
    return rand() % 21 - 10;
  }

  void Spmv(int** m, int* v, int* res, int num) {
    for (int i = 0; i < num; i++) {
      float tmp = 0;
      auto row = m[i];
      for (int j = 0; j < num; j++) {
        tmp += row[j] * v[j];
      }
      res[i] = tmp;
    }
  }

  void Add(int* src, int* dst, int num) {
    for (int i = 0; i < num; i++) {
      dst[i] += src[i];
    }
  }

  void Divide(int* src, int divisor, int num) {
    for (int i = 0; i < num; i++) {
      src[i] /= divisor;
    }
  }

 private:
  int* Read1(VertexID id) { return read_data_ + id * gnn_l; }
  int* Write1(VertexID id) { return write_data_ + id * gnn_l; }

  void sync() { memcpy(read_data_, write_data_, gnn_l * num_ * sizeof(float)); }

  void LogData() {
    LOG_INFO("VertexData info:");
    for (size_t i = 0; i < num_; i++) {
      std::string rline = "";
      std::string wline = "";
      for (uint32_t j = 0; j < gnn_l; j++) {
        rline += std::to_string(read_data_[i * gnn_l + j]) + ", ";
        wline += std::to_string(write_data_[i * gnn_l + j]) + ", ";
      }
      LOGF_INFO("Vdata id {} -> read: <{}> write: <{}>", i, rline, wline);
    }
  }

 private:
  std::vector<GNNWeightDataInt> weights_;
  size_t num_;
  int* read_data_ = nullptr;
  int* write_data_ = nullptr;
  const uint32_t gnn_l;
  uint32_t gnn_k = 1;
  uint32_t k_ = 0;
  uint32_t iter = 3;
};

}  // namespace sics::graph::nvme::apps

#endif  // GRAPH_SYSTEMS_NVME_APPS_GNN_NVME_INT_APP_H_
