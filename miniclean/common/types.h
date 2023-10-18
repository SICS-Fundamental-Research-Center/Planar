#ifndef MINICLEAN_COMMON_TYPES_H_
#define MINICLEAN_COMMON_TYPES_H_

#include <cstdint>
#include <list>
#include <vector>

namespace sics::graph::miniclean::common {

typedef uint8_t GraphID;
typedef uint32_t VertexID;
// TODO (bai-wenchao): uint8_t is enough, change the vertex_label format.
typedef uint8_t VertexLabel;
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

typedef uint32_t VertexAttributeValue;

// Since EdgeLabel is checked, VertexID is enough to represent an edge instance.
typedef std::pair<VertexID, VertexID> EdgeInstance;
typedef std::vector<EdgeInstance> PathInstance;
// Assuming all matched paths will be loaded into memory, the path instance ID
// is the index of the path instance in the loaded instance list.
typedef uint32_t PathInstanceID;

typedef std::pair<std::list<PathInstanceID>, std::list<PathInstanceID>>
    GCRInstance;

typedef std::tuple<VertexLabel, EdgeLabel, VertexLabel> EdgePattern;
typedef std::vector<EdgePattern> PathPattern;
typedef uint8_t PathPatternID;
// Star pattern is a vector of path patterns.
//   The first vertex label of each path pattern is the star center.
typedef std::list<PathPatternID> StarPattern;
typedef std::pair<StarPattern, StarPattern> DualPattern;

#define MAX_VERTEX_ATTRIBUTE_VALUE \
  std::numeric_limits<             \
      sics::graph::miniclean::common::VertexAttributeValue>::max()

#define MAX_VERTEX_ATTRIBUTE_ID \
  std::numeric_limits<sics::graph::miniclean::common::VertexAttributeID>::max()

#define MAX_VERTEX_LABEL \
  std::numeric_limits<sics::graph::miniclean::common::VertexLabel>::max()

#define MAX_VERTEX_ID \
  std::numeric_limits<sics::graph::miniclean::common::VertexID>::max()

#define MAX_EDGE_LABEL \
  std::numeric_limits<sics::graph::miniclean::common::EdgeLabel>::max()

}  // namespace sics::graph::miniclean::common

#endif  // MINICLEAN_COMMON_TYPES_H_