#include "miniclean/components/path_matcher.h"

#include <fstream>
#include <memory>
#include <sstream>
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

void PathMatcher::LoadPatterns(const std::string& pattern_path) {
  std::ifstream pattern_file(pattern_path);
  if (!pattern_file) {
    LOG_FATAL("Error opening pattern file: ", pattern_path.c_str());
  }

  VertexLabel max_label_id = 1;

  std::string line;
  while (std::getline(pattern_file, line)) {
    // if the line is empty or the first char is `#`, continue.
    if (line.empty() || line[0] == '#') continue;

    std::vector<VertexLabel> pattern;
    std::stringstream ss(line);
    std::string label;

    while (std::getline(ss, label, ',')) {
      VertexLabel label_id = static_cast<VertexLabel>(std::stoi(label));
      max_label_id = std::max(max_label_id, label_id);
      pattern.push_back(label_id);
    }

    path_patterns_.push_back(pattern);
  }

  num_label_ = max_label_id;
}

void PathMatcher::BuildCandidateSet() {
  // Reserve memory space for candidates_.
  candidates_.reserve(num_label_);
  // Initialize candidates_.
  for (VertexLabel i = 0; i < num_label_; i++) {
    candidates_.emplace_back();
  }
  VertexLabel* vertex_label = miniclean_csr_graph_->get_vertex_label();
  for (VertexID i = 0; i < miniclean_csr_graph_->get_num_vertices() * 2;
       i += 2) {
    for (VertexLabel j = 0; j < num_label_; j++) {
      if (vertex_label[i + 1] == j + 1) candidates_[j].insert(vertex_label[i]);
    }
  }
}

// TODO (bai-wenchao): This function has non-trivial overhead, optimize it.
void PathMatcher::TaskScheduler(
    unsigned int parallelism,
    std::vector<std::vector<std::unordered_set<VertexID>>>* task_pool) {
  size_t candidate_counter = 0;
  for (size_t i = 0; i < path_patterns_.size(); i++) {
    VertexLabel start_label = path_patterns_[i][0];
    std::unordered_set<VertexID> candidates = candidates_[start_label - 1];
    for (VertexID candidate : candidates) {
      (*task_pool)[candidate_counter % parallelism][i].insert(candidate);
      candidate_counter++;
    }
  }
}

void PathMatcher::PathMatching(unsigned int parallelism) {
  LOG_INFO("Initialize auxiliary data structures.");
  // Initialize matched results.
  matched_results_.reserve(path_patterns_.size());

  // Initialize thread pool.
  ThreadPool thread_pool(parallelism);
  // Initialize partial results pool.
  std::vector<std::vector<VertexID>> partial_results_pool(parallelism);
  // Initialize matched results pool.
  std::vector<std::vector<std::vector<std::vector<VertexID>>>>
      matched_results_pool(parallelism);
  for (auto& results : matched_results_pool) {
    results.resize(path_patterns_.size());
  }
  // Initialize task pool.
  std::vector<std::vector<std::unordered_set<VertexID>>> task_pool(parallelism);
  for (auto& tasks : task_pool) {
    tasks.resize(path_patterns_.size());
  }
  LOG_INFO("Split tasks.");
  TaskScheduler(parallelism, &task_pool);

  // Package tasks.
  LOG_INFO("Package tasks.");
  TaskPackage task_package;
  task_package.reserve(parallelism);
  for (size_t i = 0; i < parallelism; i++) {
    Task task = [i, this, &task_pool, &partial_results_pool,
                 &matched_results_pool] {
      for (size_t j = 0; j < this->path_patterns_.size(); j++) {
        if (task_pool[i][j].empty()) continue;
        for (VertexID vid : task_pool[i][j]) {
          std::unordered_set<VertexID> init_candidate = {vid};
          PathMatchRecur(path_patterns_[j], 0, init_candidate,
                         &partial_results_pool[i], &matched_results_pool[i][j]);
          // Recover the partial results.
          partial_results_pool[i].clear();
        }
      }
    };
    task_package.push_back(task);
  }

  // Submit tasks.
  LOG_INFO("Submit tasks.");
  thread_pool.SubmitSync(task_package);
  task_package.clear();

  // Collect results.
  for (size_t i = 0; i < path_patterns_.size(); i++) {
    std::vector<std::vector<VertexID>> matched_results;
    for (size_t j = 0; j < parallelism; j++) {
      matched_results.insert(matched_results.end(),
                             matched_results_pool[j][i].begin(),
                             matched_results_pool[j][i].end());
    }
    LOG_INFO("Pattern: ", i, ", size: ", matched_results.size());
    matched_results_.push_back(matched_results);
  }
}

void PathMatcher::PathMatchRecur(const std::vector<VertexLabel>& path_pattern,
                                 size_t match_position,
                                 const std::unordered_set<VertexID>& candidates,
                                 std::vector<VertexID>* partial_results,
                                 std::vector<std::vector<VertexID>>* results) {
  // Return condition.
  if (match_position == path_pattern.size()) {
    results->push_back(*partial_results);
    return;
  }

  for (VertexID candidate : candidates) {
    // Scan the out-edges of the candidate.
    VertexID cand_out_degree =
        miniclean_csr_graph_->GetOutDegreeBasePointer()[candidate];
    VertexID cand_out_offset =
        miniclean_csr_graph_->GetOutOffsetBasePointer()[candidate];
    std::unordered_set<VertexID> next_candidates;

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
      if (match_position + 1 < path_pattern.size() &&
          out_edge_label == path_pattern[match_position + 1]) {
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