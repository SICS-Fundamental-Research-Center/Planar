#include "miniclean/components/preprocessor/index_collection.h"

#include <algorithm>

#include "core/data_structures/buffer.h"
#include "core/data_structures/graph_metadata.h"
#include "miniclean/components/matcher/path_matcher.h"

namespace sics::graph::miniclean::components::preprocessor {

using GraphMetadata = sics::graph::core::data_structures::GraphMetadata;
using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;

void VertexAttributeSegment::LoadAttributeBucket(
    VertexLabel vlabel, const std::string& workspace_dir,
    const std::vector<VertexAttributeID>& attribute_id_list,
    const std::vector<std::string>& bucket_path_list,
    const std::vector<std::string>& bucket_size_path_list,
    const std::vector<std::string>& bucket_offset_path_list) {
  AttributeBucket attribute_bucket;
  for (size_t i = 0; i < attribute_id_list.size(); i++) {
    std::vector<VertexID> bucket;
    std::vector<uint32_t> bucket_size;
    std::vector<uint32_t> bucket_offset;

    LoadBinFile<VertexID>(workspace_dir + '/' + bucket_path_list[i], bucket);
    LoadBinFile<uint32_t>(workspace_dir + '/' + bucket_size_path_list[i],
                          bucket_size);
    LoadBinFile<uint32_t>(workspace_dir + '/' + bucket_offset_path_list[i],
                          bucket_offset);

    ValueBucket value_bucket;
    for (size_t j = 0; j < bucket_size.size(); j++) {
      value_bucket.emplace(
          j, std::unordered_set<VertexID>(
                 bucket.begin() + bucket_offset[j],
                 bucket.begin() + bucket_offset[j] + bucket_size[j]));
    }
    attribute_bucket.emplace(attribute_id_list[i], value_bucket);
  }
  bucket_by_value_.emplace(vlabel, attribute_bucket);
}

void VertexAttributeSegment::LoadAttributeBlock(
    VertexLabel vlabel, const std::string& workspace_dir,
    const std::vector<VertexAttributeID>& attribute_id_list,
    const std::vector<std::string>& block_path_list,
    const std::vector<std::string>& block_offset_path_list) {
  AttributeBlock attribute_block;
  for (size_t i = 0; i < attribute_id_list.size(); i++) {
    std::vector<VertexID> block;
    std::vector<uint32_t> block_offset;

    LoadBinFile<VertexID>(workspace_dir + '/' + block_path_list[i], block);
    LoadBinFile<uint32_t>(workspace_dir + '/' + block_offset_path_list[i],
                          block_offset);

    ValueBlock value_block;
    value_block.reserve(block_offset.size());
    for (size_t j = 1; j < block_offset.size(); j++) {
      value_block.emplace_back(
          std::vector<VertexID>(block.begin() + block_offset[j - 1],
                                block.begin() + block_offset[j]));
    }
    attribute_block.emplace(attribute_id_list[i], value_block);
  }
  block_by_range_.emplace(vlabel, attribute_block);
}

void PathPatternIndex::BuildPathPatternIndex(
    const std::string& path_instances_path,
    const std::string& graph_config_path, const std::string& path_pattern_path,
    const std::string& range_config_path) {
  // Load graph config.
  YAML::Node metadata = YAML::LoadFile(graph_config_path);
  GraphMetadata graph_metadata = metadata["GraphMetadata"].as<GraphMetadata>();
  size_t num_vertices = graph_metadata.get_num_vertices();
  // Load path patterns.
  YAML::Node path_pattern_config = YAML::LoadFile(path_pattern_path);
  YAML::Node path_pattern_node = path_pattern_config["PathPatterns"];
  std::vector<PathPattern> path_pattern =
      path_pattern_node.as<std::vector<PathPattern>>();

  // Initialize buckets.
  path_instances_buckets_by_vertex_id_.resize(num_vertices);
  for (size_t i = 0; i < num_vertices; i++) {
    path_instances_buckets_by_vertex_id_[i].resize(path_pattern.size());
  }
  vertex_bucket_by_pattern_id_.resize(path_pattern.size());

  // Build buckets.
  BuildPathInstanceBucket(path_instances_path, path_pattern);
  BuildVertexBucket(path_instances_buckets_by_vertex_id_);
  BuildVertexRange(range_config_path);
}

void PathPatternIndex::BuildPathInstanceBucket(
    const std::string& path_instances_path,
    const std::vector<PathPattern>& path_patterns) {
  for (size_t i = 0; i < path_patterns.size(); i++) {
    // Open the file.
    std::ifstream instance_file(
        path_instances_path + "/" + std::to_string(i) + ".bin",
        std::ios::binary);
    if (!instance_file) {
      LOG_FATAL(
          "Failed to open path instance file: ",
          (path_instances_path + "/" + std::to_string(i) + ".bin").c_str());
    }

    // Get the file size.
    instance_file.seekg(0, std::ios::end);
    size_t file_size = instance_file.tellg();
    instance_file.seekg(0, std::ios::beg);

    // Create a buffer.
    OwnedBuffer buffer(file_size);

    // Read the instances.
    instance_file.read(reinterpret_cast<char*>(buffer.Get()), file_size);
    if (!instance_file) {
      LOG_FATAL(
          "Failed to read path instance file: ",
          (path_instances_path + "/" + std::to_string(i) + ".bin").c_str());
    }

    // Parse the buffer.
    size_t pattern_length = path_patterns[i].size();
    size_t num_instances = file_size / (pattern_length * sizeof(VertexID));
    VertexID* instance_buffer = reinterpret_cast<VertexID*>(buffer.Get());

    // Build the bucket.
    for (size_t j = 0; j < num_instances; j++) {
      std::vector<VertexID> instance(pattern_length);
      for (size_t k = 0; k < pattern_length; k++) {
        instance[k] = instance_buffer[j * pattern_length + k];
      }
      VertexID src_vertex_id = instance[0];
      // TODO: reserve the space for vector beforehand.
      path_instances_buckets_by_vertex_id_[src_vertex_id][i].emplace_back(
          std::move(instance));
    }

    // Close the file.
    instance_file.close();
  }
}

void PathPatternIndex::BuildVertexBucket(
    const std::vector<PathInstanceBuckets>&
        path_instances_buckets_by_vertex_id) {
  // For each vertex.
  for (size_t i = 0; i < path_instances_buckets_by_vertex_id.size(); i++) {
    // For each pattern.
    for (size_t j = 0; j < path_instances_buckets_by_vertex_id[i].size(); j++) {
      if (path_instances_buckets_by_vertex_id[i][j].size() == 0) continue;
      vertex_bucket_by_pattern_id_[j].emplace(i);
    }
  }
}

void PathPatternIndex::BuildVertexRange(const std::string& range_config_path) {
  YAML::Node vertex_range_node = YAML::LoadFile(range_config_path);
  std::vector<size_t> vertex_range =
      vertex_range_node["VertexLabelRange"].as<std::vector<size_t>>();
  vertex_range_by_label_id_.reserve(vertex_range.size() - 1);
  for (size_t i = 0; i < vertex_range.size() - 1; i++) {
    vertex_range_by_label_id_.emplace_back(
        std::make_pair(vertex_range[i], vertex_range[i + 1]));
  }
}

void IndexCollection::LoadIndexCollection(
    const std::string& vertex_attribute_file,
    const std::string& path_instance_file, const std::string& graph_config_path,
    const std::string& path_pattern_path,
    const std::string& range_config_path) {
  LoadVertexAttributeSegment(vertex_attribute_file);
  LoadPathPatternIndex(path_instance_file, graph_config_path, path_pattern_path,
                       range_config_path);
}

void IndexCollection::LoadVertexAttributeSegment(
    const std::string& vertex_attribute_file) {
  YAML::Node vertex_attribute_node = YAML::LoadFile(vertex_attribute_file);
  vertex_attribute_segment_ =
      vertex_attribute_node.as<VertexAttributeSegment>();
}

void IndexCollection::LoadPathPatternIndex(
    const std::string& path_instances_path,
    const std::string& graph_config_path, const std::string& path_pattern_path,
    const std::string& range_config_path) {
  path_pattern_index_.BuildPathPatternIndex(
      path_instances_path, graph_config_path, path_pattern_path,
      range_config_path);
}

}  // namespace sics::graph::miniclean::components::preprocessor