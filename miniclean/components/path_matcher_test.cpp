#include "miniclean/components/path_matcher.h"

#include <fstream>
#include <sstream>

#include <gtest/gtest.h>

#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/io/basic_reader.h"
#include "miniclean/graphs/miniclean_csr_graph.h"
#include "miniclean/graphs/miniclean_csr_graph_config.h"

namespace sics::graph::miniclean::test {

using MiniCleanCSRGraph =  sics::graph::miniclean::graphs::MiniCleanCSRGraph;
using MiniCleanCSRGraphConfig = sics::graph::miniclean::graphs::MiniCleanCSRGraphConfig;
using PathMatcher = sics::graph::miniclean::components::PathMatcher;
using VertexID = sics::graph::core::common::VertexID;
using VertexLabel = sics::graph::core::common::VertexLabel;

class PathMatcherTest : public ::testing::Test {
 protected:
  PathMatcherTest() {
    config_ = {
        56,    // num_vertex
        55,    // max_vertex
        100,   // sum_in_degree
        100,   // sum_out_degree
    };
    path_patterns_ = {
        {1, 1},       {1, 2},       {1, 3},       {2, 1},       {2, 2},
        {2, 3},       {3, 1},       {3, 2},       {3, 3},       {1, 1, 1},
        {1, 1, 2},    {1, 1, 3},    {1, 2, 1},    {1, 2, 2},    {1, 2, 3},
        {1, 3, 1},    {1, 3, 2},    {1, 3, 3},    {2, 1, 1},    {2, 1, 2},
        {2, 1, 3},    {2, 2, 1},    {2, 2, 2},    {2, 2, 3},    {2, 3, 1},
        {2, 3, 2},    {2, 3, 3},    {3, 1, 1},    {3, 1, 2},    {3, 1, 3},
        {3, 2, 1},    {3, 2, 2},    {3, 2, 3},    {3, 3, 1},    {3, 3, 2},
        {3, 3, 3},    {1, 1, 1, 1}, {1, 1, 1, 2}, {1, 1, 1, 3}, {1, 1, 2, 1},
        {1, 1, 2, 2}, {1, 1, 2, 3}, {1, 1, 3, 1}, {1, 1, 3, 2}, {1, 1, 3, 3},
        {1, 2, 1, 1}, {1, 2, 1, 2}, {1, 2, 1, 3}, {1, 2, 2, 1}, {1, 2, 2, 2},
        {1, 2, 2, 3}, {1, 2, 3, 1}, {1, 2, 3, 2}, {1, 2, 3, 3}, {1, 3, 1, 1},
        {1, 3, 1, 2}, {1, 3, 1, 3}, {1, 3, 2, 1}, {1, 3, 2, 2}, {1, 3, 2, 3},
        {1, 3, 3, 1}, {1, 3, 3, 2}, {1, 3, 3, 3}, {2, 1, 1, 1}, {2, 1, 1, 2},
        {2, 1, 1, 3}, {2, 1, 2, 1}, {2, 1, 2, 2}, {2, 1, 2, 3}, {2, 1, 3, 1},
        {2, 1, 3, 2}, {2, 1, 3, 3}, {2, 2, 1, 1}, {2, 2, 1, 2}, {2, 2, 1, 3},
        {2, 2, 2, 1}, {2, 2, 2, 2}, {2, 2, 2, 3}, {2, 2, 3, 1}, {2, 2, 3, 2},
        {2, 2, 3, 3}, {2, 3, 1, 1}, {2, 3, 1, 2}, {2, 3, 1, 3}, {2, 3, 2, 1},
        {2, 3, 2, 2}, {2, 3, 2, 3}, {2, 3, 3, 1}, {2, 3, 3, 2}, {2, 3, 3, 3},
        {3, 1, 1, 1}, {3, 1, 1, 2}, {3, 1, 1, 3}, {3, 1, 2, 1}, {3, 1, 2, 2},
        {3, 1, 2, 3}, {3, 1, 3, 1}, {3, 1, 3, 2}, {3, 1, 3, 3}, {3, 2, 1, 1},
        {3, 2, 1, 2}, {3, 2, 1, 3}, {3, 2, 2, 1}, {3, 2, 2, 2}, {3, 2, 2, 3},
        {3, 2, 3, 1}, {3, 2, 3, 2}, {3, 2, 3, 3}, {3, 3, 1, 1}, {3, 3, 1, 2},
        {3, 3, 1, 3}, {3, 3, 2, 1}, {3, 3, 2, 2}, {3, 3, 2, 3}, {3, 3, 3, 1},
        {3, 3, 3, 2}, {3, 3, 3, 3},
    };
  }
  ~PathMatcherTest() override {}

  MiniCleanCSRGraphConfig config_;
  std::vector<std::vector<VertexID>> path_patterns_;
  std::string data_dir_ = TEST_DATA_DIR;
};

TEST_F(PathMatcherTest, CheckMatches) {
  MiniCleanCSRGraph graph(0, config_);
  VertexLabel num_label = 4;
  std::set<VertexID> candidates[num_label];
  PathMatcher path_matcher(&graph, path_patterns_,
                           config_, candidates, num_label);
  
  path_matcher.LoadGraph(data_dir_ + "/input/small_graph_path_matching/0");
  path_matcher.BuildCandidateSet(num_label);
  path_matcher.PathMatching();

  // Load matched result
  std::vector<std::vector<std::vector<VertexID>>> matched_results = path_matcher.get_results();

  // Read ground truth
  std::vector<std::vector<std::vector<VertexID>>> ground_truth;
  std::ifstream ground_truth_file(
      data_dir_ + "/input/small_graph_path_matching/ground_truth.txt");
  
  std::string line;
  std::vector<std::vector<VertexID>> temp;
  while (std::getline(ground_truth_file, line)) {
    if (line.empty()) {
      ground_truth.push_back(temp);
      temp.clear();
    } else {
      // judge whether the line is "empty"
      if (line == "empty") {
        // temp.push_back(std::vector<VertexID>());
        continue;
      }
      // split line with ";" and convert each item to VertexID
      std::stringstream ss(line);
      std::vector<VertexID> inst;
      std::string item;
      while (std::getline(ss, item, ';')) {
        inst.push_back(static_cast<VertexID>(std::stoi(item)));
      }
      temp.push_back(inst);
    }
  }

  // Compare the matched result with ground truth
  EXPECT_EQ(matched_results.size(), ground_truth.size());

  for (size_t i = 0; i < matched_results.size(); i++) {
    auto single_matched_result = matched_results[i];
    auto single_ground_truth = ground_truth[i];
    EXPECT_EQ(single_matched_result.size(), single_ground_truth.size());
    for (size_t j = 0; j < single_matched_result.size(); j++) {
      auto single_matched_result_inst = single_matched_result[j];
      auto single_ground_truth_inst = single_ground_truth[j];
      EXPECT_EQ(single_matched_result_inst.size(), single_ground_truth_inst.size());
      for (size_t k = 0; k < single_matched_result_inst.size(); k++) {
        EXPECT_EQ(single_matched_result_inst[k], single_ground_truth_inst[k]);
      }
    }
  }

}


}
