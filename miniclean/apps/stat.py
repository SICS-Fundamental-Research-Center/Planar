import csv



with open('/home/baiwc/workspace/graph-systems/data/dblp/graph_edge_all_seq.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        num1 = int(row[0])
        num2 = int(row[1])
        