#include "miniclean/components/preprocessor/index_collection.h"

namespace sics::graph::miniclean::components::preprocessor {

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
          j, std::vector<VertexID>(
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

void IndexCollection::LoadIndexCollection(
    const std::string& attribute_config_file) {
  LoadVertexAttributeSegment(attribute_config_file);
}

void IndexCollection::LoadVertexAttributeSegment(
    const std::string& vertex_attribute_file) {
  YAML::Node vertex_attribute_node = YAML::LoadFile(vertex_attribute_file);
  vertex_attribute_segment_ =
      vertex_attribute_node.as<VertexAttributeSegment>();
}

}  // namespace sics::graph::miniclean::components::preprocessor