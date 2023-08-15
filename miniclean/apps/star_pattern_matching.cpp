#include "core/common/multithreading/thread_pool.h"

#include <iostream>
#include <memory>
#include <vector>

#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/io/basic_reader.h"
#include "miniclean/components/path_matcher.h"
#include "miniclean/graphs/miniclean_csr_graph.h"
#include "miniclean/graphs/miniclean_csr_graph_config.h"

using MiniCleanCSRGraph = sics::graph::miniclean::graphs::MiniCleanCSRGraph;
using MiniCleanCSRGraphConfig =
    sics::graph::miniclean::graphs::MiniCleanCSRGraphConfig;
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using BasicReader = sics::graph::core::io::BasicReader;
using VertexID = sics::graph::core::common::VertexID;
using VertexLabel = sics::graph::core::common::VertexLabel;
using ThreadPool = sics::graph::core::common::ThreadPool;
using PathMatcher = sics::graph::miniclean::components::PathMatcher;

// @DESCRIPTION: Match path patterns in the graph.
// @PARAMETER: For temporary usage, parameters are hard-coded.
//  - number of vertex labels that will exist in path patterns;
//  - path patterns in `path_patterns` in which each path pattern is represented
//    by a vector of vertex labels;
//  - directory of the input graph when calling `LoadGraph`;
//  - graph configuration in `config`;
int main() {

  MiniCleanCSRGraphConfig config = {
      12009216,   // num_vertex
      12009215,   // max_vertex
      73286905,   // sum_in_degree
      73286905,   // sum_out_degree
  };

  // Path patterns to be matched.
  std::vector<std::vector<VertexID>> path_patterns = {
      {1, 1},    {1, 2},    {1, 4},    {4, 3},      {1, 1, 1},
      {1, 1, 2}, {1, 1, 4}, {1, 4, 3}, {1, 1, 4, 3}};
  
  MiniCleanCSRGraph graph(0,config); 
  VertexLabel num_label = 4;
  std::set<VertexID> candidates[num_label];

  PathMatcher path_matcher(std::move(graph), std::move(path_patterns),
                           config, candidates, num_label);
  path_matcher.LoadGraph("/home/baiwc/workspace/graph-systems/data/dblp_test/0");

  path_matcher.BuildCandidateSet(num_label);

  path_matcher.PathMatching();

  // Print matched results.
  path_matcher.PrintMatchedResults();

  return 0;
}