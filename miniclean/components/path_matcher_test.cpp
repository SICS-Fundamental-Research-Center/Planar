#include "miniclean/components/path_matcher.h"

#include <fstream>
#include <sstream>

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/data_structures/graph_metadata.h"
#include "miniclean/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::components {

using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using MiniCleanCSRGraph = sics::graph::miniclean::graphs::MiniCleanCSRGraph;
using PathMatcher = sics::graph::miniclean::components::PathMatcher;
using SubgraphMetadata = sics::graph::core::data_structures::SubgraphMetadata;
using VertexID = sics::graph::core::common::VertexID;
using VertexLabel = sics::graph::core::common::VertexLabel;

class PathMatcherTest : public ::testing::Test {
 protected:
  PathMatcherTest() {}
  ~PathMatcherTest() override {}

  std::string data_dir_ = TEST_DATA_DIR;
};

TEST_F(PathMatcherTest, CheckMatches) {
  YAML::Node metadata;
  try {
    metadata = YAML::LoadFile(data_dir_ +
                              "/input/small_graph_path_matching/meta.yaml");
  } catch (YAML::BadFile& e) {
    GTEST_LOG_(ERROR) << e.msg;
  }

  GraphMetadata graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();

  MiniCleanCSRGraph graph(graph_metadata.GetSubgraphMetadata(0));

  PathMatcher path_matcher(&graph);

  path_matcher.LoadGraph(data_dir_ + "/input/small_graph_path_matching");
  path_matcher.LoadPatterns(
      data_dir_ + "/input/small_graph_path_matching/path_patterns.txt");
  path_matcher.BuildCandidateSet();

  path_matcher.PathMatching(std::thread::hardware_concurrency(),
                            2 * std::thread::hardware_concurrency());

  // Load matched result
  std::vector<std::vector<std::vector<VertexID>>> matched_results =
      path_matcher.get_results();

  // Sort matched result
  for (std::vector<std::vector<VertexID>>& single_matched_result :
       matched_results) {
    std::sort(single_matched_result.begin(), single_matched_result.end());
  }

  // Read ground truth
  std::vector<std::vector<std::vector<VertexID>>> ground_truth;
  std::ifstream ground_truth_file(
      data_dir_ + "/input/small_graph_path_matching/0/ground_truth.txt");

  std::string line;
  std::vector<std::vector<VertexID>> temp;

  if (!ground_truth_file.is_open()) {
    GTEST_LOG_(ERROR) << "Failed to open ground truth file.";
  }

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
      EXPECT_EQ(single_matched_result_inst.size(),
                single_ground_truth_inst.size());
      for (size_t k = 0; k < single_matched_result_inst.size(); k++) {
        EXPECT_EQ(single_matched_result_inst[k], single_ground_truth_inst[k]);
      }
    }
  }
}

}  // namespace sics::graph::miniclean::components
