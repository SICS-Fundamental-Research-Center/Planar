# Graph Partitioner

**Binary Path**: `bin/tools/graph_partitioner_exec`

**Source**: `tools/graph_partitioner/graph_partitioner.cpp`

-------------

graph_partitioner provides a set of programs for partitioning graphs.
It take as input the binary edgelist.

### Usage
You can use graph_partitioner as follows:
``` Bash
$ cd ${PROJECT_ROOT_DIR}
$ ./bin/tools/graph_partitioner -partitioner [options] -i [input path] -o [output path] -n_partitions [number of subgraphs] -store_strategty [options]
```

General options of partitioner:
* planarvertexcut - Using branch decomposition-based vertexcut partitioner. It is used as the default partitioner of the Planar system. We strongly suggest to used -biggraph command if the graph is too large.
* hashedgecut - Using hash-based edgecut partitioner.
* hashvertexcut - Using hash-based vertexcut partitioner.

General options of store strategy:
* incoming_only - store incoming edges for each vertex.
* outgoing_only - store outgoing edges for each vertex.
* unconstrained - store both incoming and outgoing edges for each vertex.
