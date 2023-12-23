# Planar overview
## What is Planar?
Planar is an out-of-core system for graph computations with a single machine. It is designed to handle large graphs that do not fit in the memory of a single machine.


## Getting Started
### Dependencies
Planar builds, runs, and has been tested on GNU/Linux. Planar depends on the following libraries:
- C++17 standard library
- CMake (>=2.8)
- Facebook folly library (latest version)
- GoogleTest (>= 1.11.0)
- yaml-cpp (>= 0.6.3)

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
Finally, build Planar in project root directory.
```bash
./build.sh
```

### Running Planar Applications
Planar use customed CSR fotmat. To run Planar applications, you should first generate the CSR format graph. We provide a tool to convert the graph to CSR format. You can follow the `tools/docs` to do this.

All applications are located in `bin/planar/`. For example, to run the WCC application, use the following command:
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
#### Random Walk
```bash
./bin/planar/random_walk_exec -i [input path] -p [parallelism] -walk [steps]
```

#### for Clion user
set cmake TEST flag in setting->Build,Execution,Deployment->CMake->Cmake options.
`set -DTEST=true` 
then apply and quit.