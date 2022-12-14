import argparse
from mpi4py import MPI
import os

def split_line(input_line):
    input_splitted = input_line.split()
    return input_splitted[1:len(input_splitted)-1]

splitted_lines = []
def read_file(file_name):
    input_file = open(file_name, "r")
    lines = input_file.readlines()
    for line in lines:
        input_line = split_line(line)
        splitted_lines.append(input_line)

comm = MPI.COMM_WORLD
rank_count = comm.Get_size()
rank = comm.Get_rank()

def distribute_data():
    # Determine number of data per worker and store it in distribution list.
    distribution = []
    data_count = len(splitted_lines)
    number_of_workers = rank_count - 1
    # Distribute evenly.
    data_per_worker = data_count // number_of_workers
    for i in range(number_of_workers):
        distribution.append(data_per_worker)
    # Distribute remainder number of data starting from the last worker.
    remainder = data_count % number_of_workers
    for i in range(remainder):
        distribution[(number_of_workers-1)-i]+=1
    # Send data to workers
    data_iterator = 0
    for i in range(number_of_workers):
        worker_rank = i+1
        number_of_data = distribution[i]
        last_iterator = data_iterator + number_of_data
        comm.send(splitted_lines[data_iterator:last_iterator], dest=worker_rank, tag=worker_rank) 
        # print("data:", data_iterator, "-", last_iterator-1, "(",number_of_data,")", "sent from", rank, "to", worker_rank)
        data_iterator+=number_of_data

if (rank==0):
    parser = argparse.ArgumentParser()
    parser.add_argument("-input_file", "--input_file", dest = "input_file")
    parser.add_argument("-merge_method", "--merge_method", dest = "merge_method")
    parser.add_argument("-test_file", "--test_file", dest = "test_file")
    args = parser.parse_args()
    read_file(args.input_file)
    distribute_data()

else:
    data = comm.recv(source=0, tag=rank)
    print("Rank {} received {} sentences.".format(rank, len(data)))