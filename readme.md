# Planar overview
## What is Planar?
Planar is a single-machine system for graph analytics by reusing existing PRAM algorithms. 
It supports both out-of-core and in-memory analytics, and provides a set of graph algorithms, 
including WCC, SSSP, MST, Coloring, PageRank and Random Walk.

## Getting Started
### Dependencies
Planar builds, runs, and has been tested on GNU/Linux. It depends on the following libraries:
- C++17 standard library
- CMake (>=2.8)
- Facebook folly library (latest version)
- GoogleTest (>= 1.11.0)
- yaml-cpp (>= 0.6.3)
- liburing (need linux kernel version >= 5.6)

### Build
First, install all dependencies. Then, run the following commands to build Planar:
```bash
git submodule init
git submodule update
```
Second, build yaml-cpp library for Planar.
```bash
cd third_party/yaml-cpp
mkdir build
cd build
cmake ..
make
```
Third, install liburing library for Planar.
```bash
git clone https://github.com/axboe/liburing.git
cd liburing
./configure
make
sudo make install
```

Finally, build Planar in project root directory.
```bash
./build.sh
```

### Running Planar Applications
After build, you can find the applications binary in root directory `bin/planar/`. 
To run the applications, you can use the following command:

#### WCC
```bash
./bin/planar/wcc_exec -i [input path] -p [parallelism] 
```
other applications are similar.
#### SSSP
```bash
./bin/planar/sssp_exec -i [input path] -p [parallelism] -source [source vertex id]
```
#### MST
```bash
./bin/planar/mst_exec -i [input path] -p [parallelism] 
```
#### Coloring
```bash
./bin/planar/coloring_exec -i [input path] -p [parallelism]
```
#### PageRank
```bash
./bin/planar/pagerank_exec -i [input path] -p [parallelism] -iter [iteration]
``` 
#### Random Walk
```bash
./bin/planar/random_walk_exec -i [input path] -p [parallelism] -walk [steps]
```

### Graph Format
Planar use customized CSR format. 
To get the final CSR format graph, you need to convert the Edgelist format graph file to a basic CSR binary file first. 
Then partitioning the CSR graph to multiple subgraphs. The process is: `Edgelist -> CSR -> Partitioned CSR`.

```bash
./bin/tools/graph_convertor_exec -i [input path] -o [output path] -t edgelistcsv2csrbin
./bin/planar/partitioner_exec -i [input path] -o [output path] -p [partition number] -cut_v [cut vertex number]
```

We also provide docs for the converter and partitioner, You can follow the `tools/docs` and `planar/docs` to do this.

## Contract Us
For bugs, please raise an issue on GiHub. Questions and comments are also welcome at my email: ly_act@buaa.edu.cn.