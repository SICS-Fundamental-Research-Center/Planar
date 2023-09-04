`tools/graph_partitioner/graph_partitioner.cpp`
-------------

graph_partitioner provides a set of serial programs for partitioning graphs.
It take as input the binary edgelist.

### Usage
You can use graph_partitioner as follows:
``` Bash
$ cd ${PROJECT_ROOT_DIR}
$ graph-partitioner --partitioner=[options] -i <input file path> -o <output file path> --n_partitions=[number of subgraphs] --store_strategty [options]
```

General options of partitioner:
* edgecut: - Using edge cut partitioner
* vertexcut: - Using vertex cut partitioner

General options of store strategy:
* incoming_only - store incoming edges for each vertex.
* outgoing_only - store outgoing edges for each vertex.
* unconstrained: - store both incoming and outgoing edges for each vertex.
