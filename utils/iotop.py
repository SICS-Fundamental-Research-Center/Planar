import datetime
import os
import matplotlib.pyplot as plt
import argparse
import time as t

def convert_to_mb(rd: str, uint: str) -> float:
    if uint == "B/s":
        return float(rd) / 1024 / 1024
    elif uint == "K/s":
        return float(rd) / 1024
    elif uint == "M/s":
        return float(rd)
    elif uint == "G/s":
        return float(rd) * 1024
    elif uint == "T/s":
        return float(rd) * 1024 * 1024


def get_time(time: str) -> int:
    tmp_time = datetime.datetime.strptime(time, "%H:%M:%S")
    return tmp_time.hour * 3600 + tmp_time.minute * 60 + tmp_time.second

# use K/s as the unit
# user set iotop -obtkq is better for parsing
def main(file: str, command: str):
    path = os.getcwd() + "/" + file
    file_name = file.split(".")[0]
    open_file = open(path, "r")
    data = open_file.readlines()
    times = []
    reads = []
    writes = []
    current_time = data[0].split()[0]
    begin_time = get_time(current_time)
    # first three lines are not useful
    time = current_time
    rd = 0.0
    wr = 0.0
    for index, line in enumerate(data):
        infos = line.split()
        if len(infos) == 0:
            continue
        if infos[0] == 'TIME':
            continue
        time = infos[0]

        if time != current_time:
            times.append(get_time(current_time) - begin_time)
            reads.append(rd)
            writes.append(wr)
            current_time = time
            rd = 0.0
            wr = 0.0
        else:
            if infos[1] + infos[2] + infos[3] == "TotalDISKREAD:":
                continue
            if infos[1] + infos[2] + infos[3] == "CurrentDISKREAD:":
                continue
            # filter command
            if command in infos[12]:
                rd_unit_cur = infos[5]
                rd_cur = infos[4]
                rd += convert_to_mb(rd_cur, rd_unit_cur)

                wr_unit_cur = infos[7]
                wr_cur = infos[6]
                wr += convert_to_mb(wr_cur, wr_unit_cur)

    times.append(get_time(current_time) - begin_time)
    reads.append(rd)
    writes.append(wr)

    # write the result into csv file
    with open(os.getcwd() + "/" + file_name + ".csv", "w") as f:
        f.write("time,reads,writes\n")
        for i in range(len(times)):
            f.write(f"{times[i]},{reads[i]},{writes[i]}\n")

    # compute average reads and writes
    avg_rd = sum(reads) / len(reads)
    avg_wr = sum(writes) / len(writes)
    print(f"Average reads: {avg_rd} MB/s")
    print(f"Average writes: {avg_wr} MB/s")

    # paint
    rd_mb = [a for a in reads]
    wr_mb = [a for a in writes]
    plt.plot(times, rd_mb, label="reads", color="red", marker="o")
    plt.plot(times, wr_mb, label="writes", color="blue", marker="o")
    plt.savefig(os.getcwd() + "/" + file_name + ".pdf")
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--file", help="file log to be parsed")
    parser.add_argument("-c", "--command", help="command to be filtered")

    args = parser.parse_args()

    main(args.file, args.command)
