#include <iostream>
#include <memory>
#include <vector>

#include "miniclean/components/path_matcher.h"
#include "miniclean/graphs/miniclean_csr_graph.h"
#include "miniclean/graphs/miniclean_csr_graph_config.h"
#include "core/io/basic_reader.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/common/multithreading/thread_pool.h"

using MinicleanCSRGraph =  sics::graph::miniclean::graphs::MinicleanCSRGraph;
using MinicleanCSRGraphConfig = sics::graph::miniclean::graphs::MinicleanCSRGraphConfig;
using SerializedImmutableCSRGraph = sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using BasicReader = sics::graph::core::io::BasicReader;
using VertexID = sics::graph::core::common::VertexID;
using VertexLabel = sics::graph::core::common::VertexLabel;
using ThreadPool = sics::graph::core::common::ThreadPool;
using PathMatcher = sics::graph::miniclean::components::PathMatcher;

int main() {

  // MinicleanCSRGraphConfig config = {
  //     12009216,   // num_vertex
  //     12009215,   // max_vertex
  //     73286905,   // sum_in_degree
  //     73286905,   // sum_out_degree
  // };
  MinicleanCSRGraphConfig config = {
      56,   // num_vertex
      55,   // max_vertex
      100,   // sum_in_degree
      100,   // sum_out_degree
  };
  // // Path patterns to be matched.
  // std::vector<std::vector<VertexID>> path_patterns = {
  //     {1, 1},    {1, 2},    {1, 4},    {4, 3},      {1, 1, 1},
  //     {1, 1, 2}, {1, 1, 4}, {1, 4, 3}, {1, 1, 4, 3}};
  std::vector<std::vector<VertexID>> path_patterns = {
      {1, 1}, {1, 2}, {1, 3}, {2, 1},    {2, 2},    {2, 3},
      {3, 1}, {3, 2}, {3, 3}, {1, 1, 1}, {1, 1, 2}, {1, 1, 3}, {1, 1, 1, 1},
  };
  
  MinicleanCSRGraph graph(0, std::move(config));
  VertexLabel num_label = 4;
  std::set<VertexID> candidates[num_label];

  PathMatcher path_matcher(std::move(graph), std::move(path_patterns),
                           std::move(config), candidates, num_label);
  // path_matcher.LoadGraph("/home/baiwc/workspace/graph-systems/data/dblp_test/0", &graph);
  path_matcher.LoadGraph("/home/baiwc/workspace/graph-systems/data/small_graph/0");

  path_matcher.BuildCandidateSet(num_label);

  path_matcher.PathMatching();

  // Print matched results.
  path_matcher.PrintMatchedResults();

  return 0;
}