#!/bin/bash

edgelistcsv_path="./edgelist.csv"
edgelistbin_dir="./edgelist_bin/"
tempcsr_path="./temp/"
partition_result_path="./partition_result/"
converter_path="../../../bin/tools/graph_converter_exec"
partitioner_path="../../../bin/tools/graph_partitioner_exec"

$converter_path -not_reorder_vertices -i $edgelistcsv_path -sep "," -o $edgelistbin_dir -convert_mode edgelistcsv2edgelistbin
$partitioner_path -i $edgelistbin_dir -o $tempcsr_path -partitioner hashedgecut -store_strategy unconstrained -n_partitions 1
$partitioner_path -i $tempcsr_path -o $partition_result_path -partitioner bfsedgecut -store_strategy unconstrained -n_partitions 2
