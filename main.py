"""
Student Name: Ramazan Burak Saritas, Ali Alperen Sonmez
Student Number: 2020400321, 2020400354
Compile Status: Compiling
Program Status: Working
Notes: 
"""
from mpi4py import MPI
import sys

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
    collected_data = {}
    # Workers sends their calculated data dictionary to Master.
    if (rank!=0):
        comm.send(calculated_data, dest=0, tag=rank)
    # Master receives all workers data sequentially and adds to its data dictionary.
    else:
        number_of_workers = rank_count - 1
        # Each item in calculated_data array corresponds to total counts.
        for i in range(number_of_workers):
            received_data = comm.recv(source=i+1, tag=i+1)
            for key in received_data:
                if key in collected_data:
                    collected_data[key]+=received_data[key]
                else:
                    collected_data[key]=received_data[key]
    return collected_data

def merge_data_workers(calculated_data):
    number_of_workers = rank_count - 1
    last_worker = number_of_workers
    previous_worker = rank - 1
    next_worker = rank + 1
    collected_data = calculated_data
    # Workers receive data from the previous worker and send to the next.
    if (rank==1):
        comm.send(collected_data, dest=next_worker, tag=rank)
    elif (rank>1):
        received_data = comm.recv(source=previous_worker, tag=previous_worker)
        for key in received_data:
            if key in collected_data:
                collected_data[key]+=received_data[key]
            else:
                collected_data[key]=received_data[key]
        if (rank!=last_worker):
            comm.send(collected_data, dest=next_worker, tag=rank)
        else:
            comm.send(collected_data, dest=0, tag=rank)
    else:
        collected_data = comm.recv(source=last_worker, tag=last_worker)
    return collected_data


# Counts frequencies of bigrams and unigrams, and records them in a dictionary.
def count_unigrams_bigrams(data):
    #increment_value function checks if a key exist in a dict. If so, increments value. Else, adds key to dict with value=1.
    def increment_value(key,dct):
        if key in dct:
            dct[key] += 1
        else:
            dct[key] = 1

    dct = dict()
    for sentence in data:
        if len(sentence)==0:
            continue
        if len(sentence)==1:
            increment_value(sentence[0], dct)
            continue

        increment_value(sentence[0], dct)
        for idx in range(len(sentence)-1):
            bigram = sentence[idx] + " " + sentence[idx+1]
            unigram = sentence[idx+1]
            increment_value(bigram, dct)
            increment_value(unigram, dct)  
    return dct
               
# Takes "new technologies" and dict as arguments and returns P(technologies|new) = Freq(new technologies)/Freq(new)
def compute_conditional_probability(unigram_bigram_count, bigram, unigram):
    count_bigram = 0
    count_unigram = 0
    if bigram in unigram_bigram_count:
        count_bigram = unigram_bigram_count[bigram]
    if unigram in unigram_bigram_count:
        count_unigram = unigram_bigram_count[unigram]
    return count_bigram/count_unigram

input_file = ""
merge_method = ""
test_file = ""
unigram_bigram_count = {}

# Requirement 1
if (rank==0):
    if(sys.argv[1]=="--input_file"):
        input_file = sys.argv[2]
    if(sys.argv[3]=="--merge_method"):
        merge_method = sys.argv[4]
    if(sys.argv[5]=="--test_file"):
        test_file = sys.argv[6]
    # Send data to workers of the merge method.
    number_of_workers = rank_count - 1
    for i in range(number_of_workers):
        worker_rank = i+1
        comm.send(merge_method, dest=worker_rank, tag=0)
    # Read input file.
    read_file(input_file)
    # Distribute the lists of sentences to workers.
    distribute_data()

# Requirement 2
else:
    merge_method = comm.recv(source=0, tag=0)
    data = comm.recv(source=0, tag=rank)
    print("Rank {} received {} sentences.".format(rank, len(data)))
    # TODO: count unigrams and bigrams
    dct = count_unigrams_bigrams(data)
    unigram_bigram_count = dct
    # TODO: add calculated data to array calculated_data to send it later 

# Requirement 3
if(merge_method=="MASTER"):
    unigram_bigram_count = merge_data_master(unigram_bigram_count)

elif(merge_method=="WORKERS"):
    unigram_bigram_count = merge_data_workers(unigram_bigram_count)

# Requirement 4
if (rank==0):
    bigram_file = open(test_file, "r")
    for bigram in bigram_file:
        unigram = bigram.split()[0]
        conditional_probability = compute_conditional_probability(unigram_bigram_count, bigram.rstrip(), unigram)
        print(bigram.rstrip(), ":", conditional_probability)
    bigram_file.close()