#ifndef MINICLEAN_COMPONENTS_RULE_DISCOVERY_RULE_DISCOVERY_H_
#define MINICLEAN_COMPONENTS_RULE_DISCOVERY_RULE_DISCOVERY_H_

#include <string>

#include "miniclean/common/types.h"
#include "miniclean/data_structures/gcr/gcr.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::components::rule_discovery {

// TODO (bai-wenchao): Implement out-of-core version.
class RuleMiner {
 private:
  using GCR = sics::graph::miniclean::data_structures::gcr::GCR;
  using MiniCleanCSRGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using PathInstance = sics::graph::miniclean::common::PathInstance;
  using PathPattern = sics::graph::miniclean::common::PathPattern;

 public:
  RuleMiner() = default;

  void LoadMiniCleanCSRGraph(std::string graph_path);

  void LoadPathPatterns(std::string path_patterns_path);

  void LoadPathInstances(std::string path_instances_path);

  void LoadPredicates(std::string predicates_path);

  void InitGCRs();

  void RuleDiscovery();

 private:
  MiniCleanCSRGraph graph_;
  std::vector<GCR> gcrs_;
  std::vector<PathPattern> path_patterns_;
  std::vector<std::vector<PathInstance>> path_instances_;
  // TODO (bai-wenchao): design data structure to store loaded predicates.
};

}  // namespace sics::graph::miniclean::components::rule_discovery

#endif  // MINICLEAN_COMPONENTS_RULE_DISCOVERY_RULE_DISCOVERY_H_