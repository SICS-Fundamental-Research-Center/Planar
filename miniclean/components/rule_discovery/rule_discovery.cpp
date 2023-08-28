#include <fstream>

#include "miniclean/components/rule_discovery/rule_discovery.h"

namespace sics::graph::miniclean::components::rule_discovery {

using PathPattern = sics::graph::miniclean::common::PathPattern;
using VertexLabel = sics::graph::miniclean::common::VertexLabel;
using EdgeLabel = sics::graph::miniclean::common::EdgeLabel;

void RuleMiner::LoadPathPatterns(const std::string& path_patterns_path) {
  std::ifstream pattern_file(path_patterns_path);
  if (!pattern_file.is_open()) {
    LOG_FATAL("Failed to open path pattern file: ", path_patterns_path.c_str());
  }

  std::string line;
  while (std::getline(pattern_file, line)) {
    // if the line is empty or the first char is `#`, continue.
    if (line.empty() || line[0] == '#') continue;

    PathPattern pattern;
    std::stringstream ss(line);
    std::string label;

    std::vector<uint8_t> label_vec;
    while (std::getline(ss, label, ',')) {
      uint8_t label_id = static_cast<uint8_t>(std::stoi(label));
      label_vec.push_back(label_id);
    }

    for (size_t i = 0; i < label_vec.size() - 1; i += 2) {
      VertexLabel src_label = static_cast<VertexLabel>(label_vec[i]);
      EdgeLabel edge_label = static_cast<EdgeLabel>(label_vec[i + 1]);
      VertexLabel dst_label = static_cast<VertexLabel>(label_vec[i + 2]);

      pattern.emplace_back(src_label, edge_label, dst_label);
    }

    path_patterns_.push_back(pattern);
  }

  pattern_file.close();
}

}