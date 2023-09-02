#ifndef MINICLEAN_COMPONENTS_RULE_DISCOVERY_RULE_DISCOVERY_H_
#define MINICLEAN_COMPONENTS_RULE_DISCOVERY_RULE_DISCOVERY_H_

#include <list>
#include <string>
#include <unordered_map>
#include <vector>

#include "miniclean/common/types.h"
#include "miniclean/data_structures/gcr/gcr.h"
#include "miniclean/data_structures/gcr/predicate.h"
#include "miniclean/data_structures/graphs/miniclean_csr_graph.h"

namespace sics::graph::miniclean::components::rule_discovery {

// TODO (bai-wenchao): Implement out-of-core version.
class RuleMiner {
 private:
  using ConstantPredicate =
      sics::graph::miniclean::data_structures::gcr::ConstantPredicate;
  using GCR = sics::graph::miniclean::data_structures::gcr::GCR;
  using MiniCleanCSRGraph =
      sics::graph::miniclean::data_structures::graphs::MiniCleanCSRGraph;
  using PathInstance = sics::graph::miniclean::common::PathInstance;
  using PathPattern = sics::graph::miniclean::common::PathPattern;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using GCRPredicate =
      sics::graph::miniclean::data_structures::gcr::GCRPredicate;
  using VariablePredicate =
      sics::graph::miniclean::data_structures::gcr::VariablePredicate;
  using VertexLabel = sics::graph::miniclean::common::VertexLabel;

 public:
  RuleMiner(MiniCleanCSRGraph* graph) : graph_(graph) {}
  ~RuleMiner() {
    for (auto& predicates : predicate_pool_) {
      for (auto& predicate : predicates) {
        delete predicate;
      }
    }
  }

  void LoadGraph(const std::string& graph_path);

  void LoadPathPatterns(const std::string& path_patterns_path);

  void LoadPathInstances(const std::string& path_instances_path);

  void LoadPredicates(const std::string& predicates_path);

  // TODO (bai-wenchao): move this to pre-processing stage.
  void BuildVertexToPatternIndex();

  void InitGCRs();

  void RuleDiscovery();

 private:
  void InitGCRsRecur(GCR gcr, size_t depth,
                     std::vector<size_t> precondition_ids,
                     size_t consequence_id,
                     std::vector<GCRPredicate*>& predicate_pool);

  size_t predicate_restriction = 3;

  MiniCleanCSRGraph* graph_;
  std::list<GCR> gcrs_;
  std::vector<PathPattern> path_patterns_;
  std::vector<std::unordered_map<VertexID, std::list<std::vector<VertexID>>>>
      path_instances_;

  std::vector<std::vector<GCRPredicate*>> predicate_pool_;

  std::unordered_map<
      VertexLabel,
      std::unordered_map<VertexLabel, std::vector<VariablePredicate>>>
      variable_predicates_;
  std::unordered_map<VertexLabel, std::vector<ConstantPredicate>>
      constant_predicates_;
};

}  // namespace sics::graph::miniclean::components::rule_discovery

#endif  // MINICLEAN_COMPONENTS_RULE_DISCOVERY_RULE_DISCOVERY_H_