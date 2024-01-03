#ifndef MINICLEAN_COMPONENTS_ERROR_DETECTOR_ERROR_DETECTOR_H_
#define MINICLEAN_COMPONENTS_ERROR_DETECTOR_ERROR_DETECTOR_H_

#include <string>
#include <vector>

#include "miniclean/common/types.h"
#include "miniclean/data_structures/gcr/light_gcr.h"
#include "miniclean/data_structures/graphs/miniclean_graph.h"
#include "miniclean/components/error_detector/io_manager.h"

namespace sics::graph::miniclean::components::error_detector {

struct GCRIndex {
  std::vector<size_t> left_path_ids;
  std::vector<size_t> right_path_ids;
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

 public:
  ErrorDetector() = default;
  explicit ErrorDetector(IOManager* io_manager)
      : io_manager_(io_manager) {}

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

  const std::vector<std::vector<AttributedVertex>>& get_attributed_paths()
      const {
    return attributed_paths_;
  }
  const std::vector<GCRIndex>& get_gcr_index() const { return gcr_index_; }
  const std::vector<std::vector<size_t>>& get_vid_to_path_id() const {
    return vid_to_path_id_;
  }
  const std::vector<VertexID>& get_active_vids() const { return active_vids_; }

 private:
  // Determine whether a path has existed in `attributed_paths_`.
  size_t GetAttributedPathID(std::vector<AttributedVertex> attributed_path);
  
  IOManager* io_manager_;
  Graph* graph_;

  std::vector<std::vector<AttributedVertex>> attributed_paths_;
  std::vector<GCRIndex> gcr_index_;
  std::vector<std::vector<size_t>> vid_to_path_id_;
  std::vector<VertexID> active_vids_;
};

}  // namespace sics::graph::miniclean::components::error_detector
#endif  // MINICLEAN_COMPONENTS_ERROR_DETECTOR_ERROR_DETECTOR_H_