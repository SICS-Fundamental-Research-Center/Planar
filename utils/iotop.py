from datetime import datetime
import os
import matplotlib.pyplot as plt
import argparse


def convert(rd: str, uint: str) -> float:
    if uint == "B/s":
        return float(rd) / 1024
    elif uint == "K/s":
        return float(rd)
    elif uint == "M/s":
        return float(rd) * 1024
    elif uint == "G/s":
        return float(rd) * 1024 * 1024
    elif uint == "T/s":
        return float(rd) * 1024 * 1024 * 1024


# use K/s as the unit
# user set iotop -obtkq is better for parsing
def main(file: str, command: str):
    path = os.getcwd() + "/" + file
    open_file = open(path, "r")
    data = open_file.readlines()
    times = []
    reads = []
    writes = []
    current_time = data[0].split()[0]
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
            times.append(current_time)
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
                rd += convert(rd_cur, rd_unit_cur)

                wr_unit_cur = infos[7]
                wr_cur = infos[6]
                wr += convert(wr_cur, wr_unit_cur)

    times.append(current_time)
    reads.append(rd)
    writes.append(wr)

    # write the result into csv file
    with open(os.getcwd() + "/" + command + ".csv", "w") as f:
        f.write("time,reads,writes\n")
        for i in range(len(times)):
            f.write(f"{times[i]},{reads[i]},{writes[i]}\n")

    # paint
    rd_mb = [a / 1024 for a in reads]
    wr_mb = [a / 1024 for a in writes]
    plt.plot(times, rd_mb, label="reads", color="red", marker="o")
    plt.plot(times, wr_mb, label="writes", color="blue", marker="o")
    plt.savefig(os.getcwd() + "/" + command + ".png")
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--file", help="file log to be parsed")
    parser.add_argument("-c", "--command", help="command to be filtered")

    args = parser.parse_args()

    main(args.file, args.command)
