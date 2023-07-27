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
  SerializableImmutableCSRTest() = default;
  ~SerializableImmutableCSRTest() override = default;
  // SerializableImmutableCSRTest() {
    // // Initialize Serialized objects.
    // serialized_immutable_csr_0_ = new SerializedImmutableCSR();
    // serialized_immutable_csr_1_ = new SerializedImmutableCSR();
    // // Initialize test configs.
    // config_0_ = {
    //   28,  // num_vertex
    //   27,  // max_vertex
    //   61,  // sum_in_degree
    //   68,  // sum_out_degree
    // };
    // config_1_ = {
    //   28,  // num_vertex
    //   55,  // max_vertex
    //   39,  // sum_in_degree
    //   32,  // sum_out_degree
    // };
    // // Initialize check pack.
    // check_pack_0_ = new size_t[10];
    // check_pack_1_ = new size_t[10];
    // compute_total_size_and_ptr(config_0_, check_pack_0_);
    // compute_total_size_and_ptr(config_1_, check_pack_1_);
  // }

  // ~SerializableImmutableCSRTest() override {
  //   delete serialized_immutable_csr_0_;
  //   delete serialized_immutable_csr_1_;
  // };

  // void compute_total_size_and_ptr(ImmutableCSRGraphConfig config,
  //                                 size_t* ret_pack) {
  //   VertexID aligned_max_vertex =
  //       std::ceil(config.max_vertex / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
  //   size_t size_localid = sizeof(VertexID) * config.num_vertex;
  //   size_t size_globalid = sizeof(VertexID) * config.num_vertex;
  //   size_t size_indegree = sizeof(size_t) * config.num_vertex;
  //   size_t size_outdegree = sizeof(size_t) * config.num_vertex;
  //   size_t size_in_offset = sizeof(size_t) * config.num_vertex;
  //   size_t size_out_offset = sizeof(size_t) * config.num_vertex;
  //   size_t size_in_edges = sizeof(VertexID) * config.sum_in_edges;
  //   size_t size_out_edges = sizeof(VertexID) * config.sum_out_edges;
  //   size_t size_localid_by_globalid = sizeof(VertexID) * aligned_max_vertex;

  //   size_t expected_size = size_localid + size_globalid + size_indegree +
  //                          size_outdegree + size_in_offset + size_out_offset +
  //                          size_in_edges + size_out_edges +
  //                          size_localid_by_globalid;

  //   ret_pack[0] = expected_size;                  // expected size
  //   ret_pack[1] = 0;                              // start_globalid
  //   ret_pack[2] = ret_pack[1] + size_globalid;    // start_indegree
  //   ret_pack[3] = ret_pack[2] + size_indegree;    // start_outdegree
  //   ret_pack[4] = ret_pack[3] + size_outdegree;   // start_in_offset
  //   ret_pack[5] = ret_pack[4] + size_in_offset;   // start_out_offset
  //   ret_pack[6] = ret_pack[5] + size_out_offset;  // start_in_edges
  //   ret_pack[7] = ret_pack[6] + size_in_edges;    // start_out_edges
  //   ret_pack[8] = ret_pack[7] + size_out_edges;   // start_localid_by_globalid
  //   ret_pack[9] =
  //       ret_pack[8] + size_localid_by_globalid;  // end_localid_by_globalid
  // }

  // SerializedImmutableCSR* serialized_immutable_csr_0_;
  // SerializedImmutableCSR* serialized_immutable_csr_1_;
  // ImmutableCSRGraphConfig config_0_, config_1_;
  // size_t* check_pack_0_ = nullptr;
  // size_t* check_pack_1_ = nullptr;
};

TEST_F(SerializableImmutableCSRTest, TestDeserialize4Subgraph0) {

  // Initialize reader.
  Reader reader;

  SerializedImmutableCSR *serialized_immutable_csr = new SerializedImmutableCSR();

  ImmutableCSRGraphConfig config = {
      28,  // num_vertex
      27,  // max_vertex
      61,  // sum_in_degree
      68,  // sum_out_degree
  };

  // Initialize immutable csr.
  ImmutableCSRGraph serializable_immutable_csr(0, std::move(config));

  // Read a subgraph
  reader.ReadSubgraph(SUBGRAPH_0_PATH, serialized_immutable_csr);

  /* Test: whether loaded */
  // size_t loaded_size = serialized_immutable_csr->get_csr_buffer().front().front().GetSize();
  // LOG_INFO("TEST: expected size = ", check_pack_0_[0], "; loaded size = ", loaded_size);
  // EXPECT_EQ(check_pack_0_[0], loaded_size);

  // Deserialize
  common::MockTaskRunner runner;
  serializable_immutable_csr.Deserialize(runner,
                                         std::move(*serialized_immutable_csr));

  /* Test: check global id value */
  VertexID* globalid_ptr = (VertexID*)(serializable_immutable_csr.GetGlobalIDByIndex());
  for (int i = 0; i < 28; ++i) {
    EXPECT_EQ(globalid_ptr[i], i);
  }

  /* Test: check local id value */
  VertexID* localid_ptr = (VertexID*)(serializable_immutable_csr.GetLocalIDByGlobalID());
  for (int i = 0; i < 28; ++i) {
    EXPECT_EQ(localid_ptr[i], i);
  }

  /* Test: check in degree */
  size_t* indegree_ptr = (size_t*)(serializable_immutable_csr.GetInDegree());
  size_t expect_indegree[28] = {3,3,1,2,4,4,1,2,1,1,1,2,4,4,3,1,1,2,2,2,2,3,3,2,1,1,1,4};
  for (int i = 0; i < 28; ++i) {
    EXPECT_EQ(indegree_ptr[i], expect_indegree[i]);
  }

  /* Test: check out degree */
  size_t* outdegree_ptr = (size_t*)(serializable_immutable_csr.GetOutDegree());
  size_t expect_outdegree[28] = {3,3,3,0,4,4,4,3,0,3,0,4,4,4,3,4,0,3,3,2,2,3,3,2,0,0,0,4};
  for (int i = 0; i < 28; ++i) {
    EXPECT_EQ(outdegree_ptr[i], expect_outdegree[i]);
  }
}

// TEST_F(SerializableImmutableCSRTest, TestDeserialize4Subgraph1) {
//   // Initialize reader.
//   Reader reader_1_;

//   // Initialize immutable csr.
//   ImmutableCSRGraph serializable_immutable_csr_1_(1, std::move(config_1_));

//   // Read a subgraph
//   reader_1_.ReadSubgraph(SUBGRAPH_1_PATH, serialized_immutable_csr_1_);
//   size_t loaded_size = serialized_immutable_csr_0_->get_csr_buffer().front().front().GetSize();

//   // Deserialize
//   common::MockTaskRunner runner;
//   serializable_immutable_csr_1_.Deserialize(runner,
//                                          std::move(*serialized_immutable_csr_1_));

//   /* Test: whether loaded */
//   LOG_INFO("TEST: expected size = ", check_pack_1_[0], "; loaded size = ", loaded_size);
//   EXPECT_EQ(check_pack_1_[0], loaded_size);
// }

}  // namespace sics::graph::core::test
