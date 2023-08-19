#include "miniclean/components/path_matcher.h"

#include <folly/system/ThreadId.h>
#include <memory>
#include <string>

#include "core/common/multithreading/task.h"
#include "core/common/multithreading/thread_pool.h"
#include "core/data_structures/graph/serialized_immutable_csr_graph.h"
#include "core/util/logging.h"
#include "core/scheduler/message.h"
#include "miniclean/io/miniclean_csr_reader.h"

namespace sics::graph::miniclean::components {

using MiniCleanCSRReader = sics::graph::miniclean::io::MiniCleanCSRReader;
using ReadMessage = sics::graph::core::scheduler::ReadMessage;
using SerializedImmutableCSRGraph =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using Task = sics::graph::core::common::Task;
using TaskPackage = sics::graph::core::common::TaskPackage;
using ThreadPool = sics::graph::core::common::ThreadPool;

void PathMatcher::LoadGraph(const std::string& data_path) {
  // Prepare reader.
  MiniCleanCSRReader reader(data_path);

  // Initialize Serialized object.
  std::unique_ptr<SerializedImmutableCSRGraph> serialized_graph =
      std::make_unique<SerializedImmutableCSRGraph>();

  // Initialize ReadMessage object.
  ReadMessage read_message;
  read_message.graph_id = miniclean_csr_graph_->get_metadata().gid;
  read_message.response_serialized = serialized_graph.get();

  // Read a subgraph.
  reader.Read(&read_message, nullptr);

  // Deserialize subgraph.
  ThreadPool thread_pool(1);
  miniclean_csr_graph_->Deserialize(thread_pool, std::move(serialized_graph));
}

void PathMatcher::BuildCandidateSet(VertexLabel num_label) {
  VertexLabel* vertex_label = miniclean_csr_graph_->get_vertex_label();
  for (VertexID i = 0; i < miniclean_csr_graph_->get_num_vertices() * 2;
       i += 2) {
    for (VertexLabel j = 0; j < num_label; j++) {
      if (vertex_label[i + 1] == j + 1) candidates_[j].insert(vertex_label[i]);
    }
  }
}

void PathMatcher::PathMatching() {
  // initialize the result vector
  matched_results_.resize(path_patterns_.size());

  for (size_t i = 0; i < path_patterns_.size(); i++) {
    PathMatching(path_patterns_[i], &matched_results_[i]);
  }
}

void PathMatcher::PathMatching(const std::vector<VertexLabel>& path_pattern,
                               std::vector<std::vector<VertexID>>* results) {
  // Prepare candidates.
  VertexLabel start_label = path_pattern[0];
  std::set<VertexID> candidates = candidates_[start_label - 1];
  std::vector<VertexID> partial_results;
  PathMatchRecur(path_pattern, 0, candidates, &partial_results, results);
}

void PathMatcher::PathMatching(unsigned int parallelism) {
  // Initialize the result vector.
  matched_results_.resize(path_patterns_.size());
  // Initialize the thread pool.
  ThreadPool thread_pool(parallelism);
  // Initialize the task package.
  TaskPackage task_package;

  std::vector<std::vector<VertexID>*> partial_result_pool;

  // Package tasks.
  for (size_t i = 0; i < path_patterns_.size(); i++) {
    // Initialize candidates.
    VertexLabel start_label = path_patterns_[i][0];
    std::set<VertexID> candidates = candidates_[start_label - 1];
    
    // Collect tasks.
    for (VertexID candidate : candidates) {
      std::set<VertexID> init_candidate = {candidate};
      // Initialize partial results.
      std::vector<VertexID>* partial_results = new std::vector<VertexID>;
      partial_result_pool.push_back(partial_results);

      Task task = std::bind(&PathMatcher::PathMatchRecur, this,
                            path_patterns_[i], 0, init_candidate,
                            partial_results, &matched_results_[i]);
      task_package.push_back(task);
    }
  }

  // Submit tasks.
  thread_pool.SubmitSync(task_package);

  // Clear task package.
  task_package.clear();

  // Delete partial results.
  for (std::vector<VertexID>* partial_results : partial_result_pool) {
    delete partial_results;
  }
}

void PathMatcher::PathMatchRecur(const std::vector<VertexLabel>& path_pattern,
                                 size_t match_position,
                                 const std::set<VertexID>& candidates,
                                 std::vector<VertexID>* partial_results,
                                 std::vector<std::vector<VertexID>>* results) {
  // Return condition.
  if (match_position == path_pattern.size()) {
    std::lock_guard<std::mutex> lck(mtx_);
    results->push_back(*partial_results);
    return;
  }

  for (VertexID candidate : candidates) {
    // Scan the out-edges of the candidate.
    VertexID cand_out_degree =
        miniclean_csr_graph_->GetOutDegreeBasePointer()[candidate];
    VertexID cand_out_offset =
        miniclean_csr_graph_->GetOutOffsetBasePointer()[candidate];
    std::set<VertexID> next_candidates;
    
    for (size_t i = 0; i < cand_out_degree; i++) {
      VertexID out_edge_id =
          miniclean_csr_graph_
              ->GetOutgoingEdgesBasePointer()[cand_out_offset + i];
      VertexLabel out_edge_label =
          miniclean_csr_graph_->get_vertex_label()[out_edge_id * 2 + 1];
      bool continue_flag = false;
      // Check whether cycle exists.
      for (VertexID vid : *partial_results) {
        if (vid == out_edge_id) {
          continue_flag = true;
          break;
        }
      }
      if (continue_flag) continue;
      // Check whether the label matches.
      if (out_edge_label == path_pattern[match_position + 1]) {
        next_candidates.insert(out_edge_id);
      }
    }
    partial_results->push_back(candidate);
    PathMatchRecur(path_pattern, match_position + 1, next_candidates,
                   partial_results, results);
    partial_results->pop_back();
  }
}

void PathMatcher::PrintMatchedResults() {
  for (size_t i = 0; i < matched_results_.size(); i++) {
    LOG_INFO("Pattern: ", i);
    for (size_t j = 0; j < path_patterns_[i].size(); j++) {
      LOG_INFO(path_patterns_[i][j]);
    }

    for (size_t j = 0; j < matched_results_[i].size(); j++) {
      LOG_INFO("Match: ", j);
      for (size_t k = 0; k < matched_results_[i][j].size(); k++) {
        LOG_INFO(matched_results_[i][j][k]);
      }
    }
  }
}
}  // namespace sics::graph::miniclean::components