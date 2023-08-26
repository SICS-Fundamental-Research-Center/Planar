#ifndef MINICLEAN_COMMON_TYPES_H_
#define MINICLEAN_COMMON_TYPES_H_

#include <cstdint>
#include <vector>

namespace sics::graph::miniclean::common {

typedef uint8_t GraphID;
typedef uint32_t VertexID;
// TODO (bai-wenchao): uint8_t is enough, change the vertex_label format.
typedef uint32_t VertexLabel;
typedef uint8_t EdgeLabel;

// Pattern Vertex ID:
//   ID of a vertex in a star pattern.
//   The #1 element specifies the path pattern in the star pattern.
//   The #2 element specifies the vertex in the path pattern.
typedef std::tuple<uint8_t, uint8_t> PatternVertexID;  // uint8_t: 0 ~ 255
// Vertex Attribute ID:
//   Assuming attributes of a vertex have been grouped into several buckets.
//   The vertex attribute ID specifies the number of bucket of the attribute.
typedef uint8_t VertexAttributeID;

typedef std::tuple<VertexID, EdgeLabel, VertexID> EdgeInstance;
typedef std::vector<EdgeInstance> PathInstance;

typedef std::tuple<VertexLabel, EdgeLabel, VertexLabel> EdgePattern;
typedef std::vector<EdgePattern> PathPattern;
typedef std::tuple<VertexLabel, std::vector<PathPattern>> StarPattern;
typedef std::tuple<StarPattern, StarPattern> DualPattern;

}  // namespace sics::graph::miniclean::common

#endif  // MINICLEAN_COMMON_TYPES_H_