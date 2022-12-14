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

parser = argparse.ArgumentParser()
parser.add_argument("-input_file", "--input_file", dest = "input_file")
parser.add_argument("-merge_method", "--merge_method", dest = "merge_method")
parser.add_argument("-test_file", "--test_file", dest = "test_file")
args = parser.parse_args()

read_file(args.input_file)
