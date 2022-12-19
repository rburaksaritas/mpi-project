import argparse
from mpi4py import MPI
import os

# Initialize MPI communication
comm = MPI.COMM_WORLD
rank_count = comm.Get_size()
rank = comm.Get_rank()

# Functions used in reading input for Requirement 1.
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

# Function used in distributing data to workers for Requirement 2.
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
        data_iterator+=number_of_data

# Functions used in merging data for Requirement 2 and 3.
def merge_data_master(calculated_data):
    collected_data = []
    if (rank!=0):
        comm.send(calculated_data, dest=0, tag=rank)
    else:
        number_of_workers = rank_count - 1
        # Each item in calculated_data array corresponds to total counts.
        for i in range(number_of_workers):
            received_data = comm.recv(source=i+1, tag=i+1)
            for j in range(received_data):
                collected_data[j] += received_data[j]
    
    return collected_data

def merge_data_workers(calculated_data):
    number_of_workers = rank_count - 1
    last_worker = number_of_workers - 1
    collected_data = []
    if (rank!=0):
        for i in range(number_of_workers):
            previous_worker = rank-1
            next_worker = rank+1
            # Receives data from previous worker. First worker does not.
            if (rank>1):
                received_data = comm.recv(source=previous_worker, tag=previous_worker)
                # Add received data to calculated data.
                for j in range(received_data):
                    calculated_data[j] += received_data[j] 
            # Sends data to next worker. Last worker sends data to master.
            if (rank!=last_worker):
                comm.send(calculated_data, dest=next_worker, tag=rank)
            else:
                comm.send(calculated_data, dest=0, tag=rank)
    else:
        collected_data = comm.recv(source=last_worker, tag=last_worker)
    
    return collected_data

# TODO: Functions used in counting unigrams and bigrams.
# TODO: Functions used in computing the conditional probabilites of bigrams.

calculated_data = []
# Requirement 1
if (rank==0):
    parser = argparse.ArgumentParser()
    parser.add_argument("-input_file", "--input_file", dest = "input_file")
    parser.add_argument("-merge_method", "--merge_method", dest = "merge_method")
    parser.add_argument("-test_file", "--test_file", dest = "test_file")
    args = parser.parse_args()
    read_file(args.input_file)
    distribute_data()

# Requirement 2
else:
    data = comm.recv(source=0, tag=rank)
    print("Rank {} received {} sentences.".format(rank, len(data)))
    # TODO: count unigrams and bigrams
    # TODO: add calculated data to array calculated_data to send it later
    # TODO: modify calculated data defined above

collected_data = []
if (args.merge_method == "MASTER"):
    collected_data = merge_data_master(calculated_data)

# Requirement 3
elif (args.merge_method == "WORKERS")
    collected_data = merge_data_workers(calculated_data)

# Requirement 4
if (rank==0):
    # TODO: compute conditional probabilites of bigrams read from input file.
    # TODO: print bigrams and their conditional probabilities.
