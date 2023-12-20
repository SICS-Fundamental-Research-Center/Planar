# Graph Converter
**Binary Path**: `bin/tools/graph_converter_exec`

**Source**: `tools/graph_converter/graph_converter.cpp`


-------------
Many Graph applications work with graphs. 
We store graphs in a binary format. 

Other formats such as edge-list can be converted to 
our binary format with graph_converter tool provided in tools. 

### Usage
You can use graph_converter as follows:
``` Bash
$ cd ${PROJECT_ROOT_DIR}
$ ./bin/tools/graph_converter_exec -convert_mode [options] -i [input path] -o [output path] -sep [separator] (optiional commands -n_edges [numer of edges] -biggraph)
```
We strongly recommend users to -n_edges and -biggraph commands if they know how many edges to read.

General options of convert mode:
* edgelistcsv2edgelistbin - Convert txt of edgelist to binary edge list
* edgelistcsv2csrbin - Convert txt of edgelist to binary csr
* edgelistbin2csrbin - Convert binary edgelist to binary csr
