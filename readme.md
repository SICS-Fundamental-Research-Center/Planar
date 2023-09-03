









### for Clion user
set cmake TEST flag in setting->Build,Execution,Deployment->CMake->Cmake options.
`set -DTEST=true` 
then apply and quit.

../bin/tools/graph_partitioner/graph_partitioner_exec -i /data/shared/toy/edgelist_bin/ -o /data/shared/toy/workspace/ -partitioner vertexcut -store_strategy unconstrained -n_partitions 2

../bin/tools/graph_converter/graph_converter_exec -i /data/shared/toy/test100_reduced.csv -sep "," -o /data/shared/toy/edgelist_bin/ -read_head -convert_mode edgelistcsv2edgelistbin