#include "immutable_csr_graph.h"

#include <gtest/gtest.h>

#include "common/multithreading/mock_task_runner.h"
#include "common/types.h"
#include "data_structures/graph/immutable_csr_graph_config.h"
#include "io/reader.h"
#include "data_structures/graph/serialized_immutable_csr_graph.h"
#include "util/logging.h"

using ImmutableCSRGraph =
    sics::graph::core::data_structures::graph::ImmutableCSRGraph;
using SerializedImmutableCSR =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using Reader = sics::graph::core::io::Reader;
using VertexID = sics::graph::core::common::VertexID;
using ImmutableCSRGraphConfig =
    sics::graph::core::data_structures::graph::ImmutableCSRGraphConfig;

#define SUBGRAPH_0_PATH "../../../input/small_graph_part/0"
#define SUBGRAPH_1_PATH "../../../input/small_graph_part/1"

namespace sics::graph::core::test {
class SerializableImmutableCSRTest : public ::testing::Test {
 protected:
  // SerializableImmutableCSRTest() = default;
  // ~SerializableImmutableCSRTest() override = default;
  SerializableImmutableCSRTest() {
    // Initialize Serialized objects.
    serialized_immutable_csr_0_ = new SerializedImmutableCSR();
    serialized_immutable_csr_1_ = new SerializedImmutableCSR();
    // Initialize test configs.
    config_0_ = {
        28,  // num_vertex
        27,  // max_vertex
        61,  // sum_in_degree
        68,  // sum_out_degree
    };
    config_1_ = {
        28,  // num_vertex
        55,  // max_vertex
        39,  // sum_in_degree
        32,  // sum_out_degree
    };
  }

  ~SerializableImmutableCSRTest() override {
    delete serialized_immutable_csr_0_;
    delete serialized_immutable_csr_1_;
  };

  size_t compute_total_size(ImmutableCSRGraphConfig config) {
    VertexID aligned_max_vertex =
        std::ceil(config.max_vertex / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
    size_t size_localid = sizeof(VertexID) * config.num_vertex;
    size_t size_globalid = sizeof(VertexID) * config.num_vertex;
    size_t size_indegree = sizeof(size_t) * config.num_vertex;
    size_t size_outdegree = sizeof(size_t) * config.num_vertex;
    size_t size_in_offset = sizeof(size_t) * config.num_vertex;
    size_t size_out_offset = sizeof(size_t) * config.num_vertex;
    size_t size_in_edges = sizeof(VertexID) * config.sum_in_edges;
    size_t size_out_edges = sizeof(VertexID) * config.sum_out_edges;
    size_t size_localid_by_globalid = sizeof(VertexID) * aligned_max_vertex;

    size_t expected_size = size_localid + size_globalid + size_indegree +
                           size_outdegree + size_in_offset + size_out_offset +
                           size_in_edges + size_out_edges +
                           size_localid_by_globalid;

    return expected_size;
  }

  SerializedImmutableCSR* serialized_immutable_csr_0_;
  SerializedImmutableCSR* serialized_immutable_csr_1_;
  ImmutableCSRGraphConfig config_0_, config_1_;
};

TEST_F(SerializableImmutableCSRTest, TestDeserialize4Subgraph_0) {
  // Initialize reader.
  Reader reader;

  // Initialize immutable csr.
  ImmutableCSRGraph serializable_immutable_csr(0, std::move(config_0_));

  // Read a subgraph
  reader.ReadSubgraph(SUBGRAPH_0_PATH, serialized_immutable_csr_0_);

  /* Test: whether loaded */
  size_t loaded_size =
      serialized_immutable_csr_0_->GetCSRBuffer().front().front().GetSize();
  size_t expected_size = compute_total_size(config_0_);
  EXPECT_EQ(expected_size, loaded_size);

  // Deserialize
  common::MockTaskRunner runner;
  serializable_immutable_csr.Deserialize(
      runner, std::move(*serialized_immutable_csr_0_));

  /* Test: check global id value */
  VertexID* globalid_ptr =
      (VertexID*)(serializable_immutable_csr.GetGlobalIDByIndex());
  for (int i = 0; i < 28; ++i) {
    EXPECT_EQ(globalid_ptr[i], i);
  }

  /* Test: check local id value */
  VertexID* localid_ptr =
      (VertexID*)(serializable_immutable_csr.GetLocalIDByGlobalID());
  for (int i = 0; i < 28; ++i) {
    EXPECT_EQ(localid_ptr[globalid_ptr[i]], i);
  }

  /* Test: check in degree */
  size_t* indegree_ptr = (size_t*)(serializable_immutable_csr.GetInDegree());
  size_t expect_indegree[28] = {3, 3, 1, 2, 4, 4, 1, 2, 1, 1, 1, 2, 4, 4,
                                3, 1, 1, 2, 2, 2, 2, 3, 3, 2, 1, 1, 1, 4};
  for (int i = 0; i < 28; ++i) {
    EXPECT_EQ(indegree_ptr[i], expect_indegree[i]);
  }

  /* Test: check out degree */
  size_t* outdegree_ptr = (size_t*)(serializable_immutable_csr.GetOutDegree());
  size_t expect_outdegree[28] = {3, 3, 3, 0, 4, 4, 4, 3, 0, 3, 0, 4, 4, 4,
                                 3, 4, 0, 3, 3, 2, 2, 3, 3, 2, 0, 0, 0, 4};
  for (int i = 0; i < 28; ++i) {
    EXPECT_EQ(outdegree_ptr[i], expect_outdegree[i]);
  }

  /* Test: check in edges of vertex 0 (local id) */
  size_t* in_offset_ptr_0 = (size_t*)(serializable_immutable_csr.GetInOffset());
  VertexID* in_edge_ptr_0 =
      (VertexID*)(serializable_immutable_csr.GetInEdges());
  size_t expect_in_edges_0[3] = {1, 2, 19};
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(in_edge_ptr_0[in_offset_ptr_0[0] + i], expect_in_edges_0[i]);
  }

  /* Test: check out edges of vertex 0 (local id) */
  size_t* out_offset_ptr_0 =
      (size_t*)(serializable_immutable_csr.GetOutOffset());
  VertexID* out_edge_ptr_0 =
      (VertexID*)(serializable_immutable_csr.GetOutEdges());
  size_t expect_out_edges_0[3] = {1, 2, 19};
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(out_edge_ptr_0[out_offset_ptr_0[0] + i], expect_out_edges_0[i]);
  }

  /* Test: check in edges of vertex 15 (local id) */
  size_t* in_offset_ptr_15 =
      (size_t*)(serializable_immutable_csr.GetInOffset());
  VertexID* in_edge_ptr_15 =
      (VertexID*)(serializable_immutable_csr.GetInEdges());
  size_t expect_in_edges_15[1] = {13};
  for (int i = 0; i < 1; ++i) {
    EXPECT_EQ(in_edge_ptr_15[in_offset_ptr_15[15] + i], expect_in_edges_15[i]);
  }

  /* Test: check out edges of vertex 15 (local id) */
  size_t* out_offset_ptr_15 =
      (size_t*)(serializable_immutable_csr.GetOutOffset());
  VertexID* out_edge_ptr_15 =
      (VertexID*)(serializable_immutable_csr.GetOutEdges());
  size_t expect_out_edges_15[4] = {13, 16, 28, 35};
  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(out_edge_ptr_15[out_offset_ptr_15[15] + i],
              expect_out_edges_15[i]);
  }

  /* Test: check in edges of vertex 27 (local id) */
  size_t* in_offset_ptr_27 =
      (size_t*)(serializable_immutable_csr.GetInOffset());
  VertexID* in_edge_ptr_27 =
      (VertexID*)(serializable_immutable_csr.GetInEdges());
  size_t expect_in_edges_27[4] = {13, 17, 41, 44};
  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(in_edge_ptr_27[in_offset_ptr_27[27] + i], expect_in_edges_27[i]);
  }

  /* Test: check out edges of vertex 15 (local id) */
  size_t* out_offset_ptr_27 =
      (size_t*)(serializable_immutable_csr.GetOutOffset());
  VertexID* out_edge_ptr_27 =
      (VertexID*)(serializable_immutable_csr.GetOutEdges());
  size_t expect_out_edges_27[4] = {13, 17, 41, 44};
  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(out_edge_ptr_27[out_offset_ptr_27[27] + i],
              expect_out_edges_27[i]);
  }
}

TEST_F(SerializableImmutableCSRTest, TestDeserialize4Subgraph_1) {
  // Initialize reader.
  Reader reader;

  // Initialize immutable csr.
  ImmutableCSRGraph serializable_immutable_csr(1, std::move(config_1_));

  // Read a subgraph
  reader.ReadSubgraph(SUBGRAPH_1_PATH, serialized_immutable_csr_1_);

  /* Test: whether loaded */
  size_t loaded_size =
      serialized_immutable_csr_1_->GetCSRBuffer().front().front().GetSize();
  size_t expected_size = compute_total_size(config_1_);
  EXPECT_EQ(expected_size, loaded_size);

  // Deserialize
  common::MockTaskRunner runner;
  serializable_immutable_csr.Deserialize(
      runner, std::move(*serialized_immutable_csr_1_));

  /* Test: check global id value */
  VertexID* globalid_ptr =
      (VertexID*)(serializable_immutable_csr.GetGlobalIDByIndex());
  for (int i = 0; i < 28; ++i) {
    EXPECT_EQ(globalid_ptr[i], i + 28);
  }

  /* Test: check local id value */
  VertexID* localid_ptr =
      (VertexID*)(serializable_immutable_csr.GetLocalIDByGlobalID());
  for (int i = 0; i < 28; ++i) {
    EXPECT_EQ(localid_ptr[globalid_ptr[i]], i);
  }

  /* Test: check in degree */
  size_t* indegree_ptr = (size_t*)(serializable_immutable_csr.GetInDegree());
  size_t expect_indegree[28] = {1, 1, 1, 2, 3, 2, 1, 1, 2, 1, 2, 1, 1, 2,
                                1, 1, 4, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1};
  for (int i = 0; i < 28; ++i) {
    EXPECT_EQ(indegree_ptr[i], expect_indegree[i]);
  }

  /* Test: check out degree */
  size_t* outdegree_ptr = (size_t*)(serializable_immutable_csr.GetOutDegree());
  size_t expect_outdegree[28] = {0, 0, 0, 3, 3, 4, 0, 0, 2, 0, 4, 3, 0, 3,
                                 3, 1, 4, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0};
  for (int i = 0; i < 28; ++i) {
    EXPECT_EQ(outdegree_ptr[i], expect_outdegree[i]);
  }

  /* Test: check in edges of vertex 0 (local id) */
  size_t* in_offset_ptr_0 = (size_t*)(serializable_immutable_csr.GetInOffset());
  VertexID* in_edge_ptr_0 =
      (VertexID*)(serializable_immutable_csr.GetInEdges());
  size_t expect_in_edges_0[1] = {15};
  for (int i = 0; i < 1; ++i) {
    EXPECT_EQ(in_edge_ptr_0[in_offset_ptr_0[0] + i], expect_in_edges_0[i]);
  }

  /* Test: check in edges of vertex 15 (local id) */
  size_t* in_offset_ptr_15 =
      (size_t*)(serializable_immutable_csr.GetInOffset());
  VertexID* in_edge_ptr_15 =
      (VertexID*)(serializable_immutable_csr.GetInEdges());
  size_t expect_in_edges_15[1] = {44};
  for (int i = 0; i < 1; ++i) {
    EXPECT_EQ(in_edge_ptr_15[in_offset_ptr_15[15] + i], expect_in_edges_15[i]);
  }

  /* Test: check out edges of vertex 15 (local id) */
  size_t* out_offset_ptr_15 =
      (size_t*)(serializable_immutable_csr.GetOutOffset());
  VertexID* out_edge_ptr_15 =
      (VertexID*)(serializable_immutable_csr.GetOutEdges());
  size_t expect_out_edges_15[1] = {44};
  for (int i = 0; i < 1; ++i) {
    EXPECT_EQ(out_edge_ptr_15[out_offset_ptr_15[15] + i],
              expect_out_edges_15[i]);
  }

  /* Test: check in edges of vertex 27 (local id) */
  size_t* in_offset_ptr_27 =
      (size_t*)(serializable_immutable_csr.GetInOffset());
  VertexID* in_edge_ptr_27 =
      (VertexID*)(serializable_immutable_csr.GetInEdges());
  size_t expect_in_edges_27[1] = {38};
  for (int i = 0; i < 1; ++i) {
    EXPECT_EQ(in_edge_ptr_27[in_offset_ptr_27[27] + i], expect_in_edges_27[i]);
  }
}

}  // namespace sics::graph::core::test
