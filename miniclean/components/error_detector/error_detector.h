#ifndef MINICLEAN_COMPONENTS_ERROR_DETECTOR_ERROR_DETECTOR_H_
#define MINICLEAN_COMPONENTS_ERROR_DETECTOR_ERROR_DETECTOR_H_

#include <string>
#include <vector>
#include <memory>

#include "core/common/multithreading/task.h"
#include "core/common/multithreading/thread_pool.h"
#include "miniclean/common/error_detection_config.h"
#include "miniclean/common/types.h"
#include "miniclean/data_structures/gcr/light_gcr.h"
#include "miniclean/data_structures/graphs/miniclean_graph.h"

namespace sics::graph::miniclean::components::error_detector {

struct GCRPathCollection {
  std::vector<size_t> left_path_pattern_ids;
  std::vector<size_t> right_path_pattern_ids;
};

// Partial match.
struct ConstrainedStarInstance {
  sics::graph::miniclean::common::GraphID graph_id;
  sics::graph::miniclean::common::VertexID center_local_vid;

  // The size of the `attribute_values` equals to the total number of
  // attributes.
  // If the gcr constraint do not contain an attribute value, just leave it
  // empty.
  // The `string` type attribute can be converted to the related type by
  // refering to the configuration (i.e.,
  // `miniclean::common::ErrorDetectionConifg`).
  std::vector<std::string> attribute_values;

  // If gcr constraint do not contain edge predicates, then just skip these two
  // fields.
  std::vector<sics::graph::miniclean::common::VertexID> incoming_local_vids;
  std::vector<sics::graph::miniclean::common::VertexID> outgoing_local_vids;
};

class ErrorDetector {
 private:
  using Graph = sics::graph::miniclean::data_structures::graphs::MiniCleanGraph;
  using GraphID = sics::graph::miniclean::common::GraphID;
  using Vertex =
      sics::graph::miniclean::data_structures::graphs::MiniCleanVertex;
  using GCR = sics::graph::miniclean::data_structures::gcr::LightGCR;
  using StarConstraints =
      sics::graph::miniclean::data_structures::gcr::StarConstraints;
  using AttributedVertex =
      sics::graph::miniclean::data_structures::gcr::AttributedVertex;
  using VertexID = sics::graph::miniclean::common::VertexID;
  using VertexAttributeID = sics::graph::miniclean::common::VertexAttributeID;
  using VertexAttributeType =
      sics::graph::miniclean::data_structures::graphs::VertexAttributeType;
  using OpType = sics::graph::miniclean::data_structures::gcr::OpType;
  using Config = sics::graph::miniclean::common::ErrorDetectionConfig;
  using TaskPackage = sics::graph::core::common::TaskPackage;
  using ThreadPool = sics::graph::core::common::ThreadPool;

 public:
  explicit ErrorDetector(const std::string& gcr_path)
      : gcr_path_(gcr_path),
        parallelism_(Config::Get()->path_indexing_parallelism_),
        thread_pool_(Config::Get()->path_indexing_parallelism_) {}

  // Load GCR set decompose it to path patterns.
  //
  // This function will only be called once (ater the error detector is
  // created). It will analyse the input GCR set and decompose it to distinct
  // path patterns which are the minimum units for the pattern matching.
  // The decomposed components will be indexed and the mapping relation will be
  // stored in `gcr_index_`.
  void InitGCRSet();

  // Load basic components: subgraph, active vertices, and the index for
  // vertices.
  //
  // For the first round of error detection, the active vertices and index have
  // not been created yet, so they will be skipped.
  void LoadBasicComponents(GraphID graph_id);

  // Build path level index for every vertices in the subgraph.
  //
  // This function will be called once for each subgraph (after the subgraph and
  // the GCR set is loaded in the first round of error detection).
  // Since the index is build for vertice, we can reuse it directly if we hold
  // the IDs of active vertices.
  void BuildPathIndexForVertices();

  // Discharge partial results and basic components.
  //
  // This function should not only discharge the partial
  // results, but also the basic components. One should make sure that the
  // memory space for the basic components are freed before next round of error
  // detection.
  void DischargePartialResults(
      const std::vector<ConstrainedStarInstance>& partial_results);

  // Match constrained star patterns.
  //
  // This function will compute the matching results for the constrained star
  // patterns. Only vertices in the active vertex set should be considered. The
  // matching results will be returned as the partial results.
  std::vector<ConstrainedStarInstance> MatchConstrainedStarPattern();

  Graph* GetGraph() { return graph_; }
  const std::vector<GCR>& GetGCRs() const { return gcrs_; }
  const std::vector<std::vector<AttributedVertex>>& GetAttributedPaths() const {
    return attributed_path_patterns_;
  }
  const std::vector<GCRPathCollection>& GetGcrPathCollections() const {
    return gcr_path_pattern_collections_;
  }
  const std::vector<std::vector<size_t>>& GetVidToPathId() const {
    return vid_to_path_pattern_id_;
  }
  const std::vector<VertexID>& GetActiveVids() const { return active_vids_; }

 private:
  // Determine whether a path has existed in `attributed_paths_`.
  size_t GetAttributedPathID(std::vector<AttributedVertex> attributed_path);

  bool MatchPathForVertex(VertexID local_vid, size_t match_position,
                          const std::vector<AttributedVertex>& attributed_path);

  bool IsPredicateSatisfied(VertexAttributeType vattr_type, OpType op_type,
                            const std::string& rhs, const std::string& lhs);

  Graph* graph_;

  const std::string gcr_path_;
  std::vector<GCR> gcrs_;
  std::vector<std::vector<AttributedVertex>> attributed_path_patterns_;
  std::vector<GCRPathCollection> gcr_path_pattern_collections_;
  std::vector<std::vector<size_t>> vid_to_path_pattern_id_;
  std::vector<VertexID> active_vids_;

  unsigned int parallelism_;
  ThreadPool thread_pool_;
  std::mutex path_indexing_mtx_;
};

}  // namespace sics::graph::miniclean::components::error_detector
#endif  // MINICLEAN_COMPONENTS_ERROR_DETECTOR_ERROR_DETECTOR_H_