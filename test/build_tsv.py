import gzip
import os
import random
import string
import sys

num_categorical_vars = int(sys.argv[1])
num_discrete_vars = int(sys.argv[2])
num_continuous_vars = int(sys.argv[3])
num_rows = int(sys.argv[4])
out_file_path = sys.argv[5]

random.seed(0)

few_letters = string.ascii_letters[26:29]
all_letters = string.ascii_letters[26:]

if out_file_path.endswith(".gz"):
    out_file = gzip.open(out_file_path, 'wb', compresslevel=1)
else:
    out_file = open(out_file_path, 'wb')

out_file.write(b"ID\t")

for i in range(num_categorical_vars):
    out_file.write(("Categorical{}".format(i+1)).encode())

    if i < (num_categorical_vars - 1) or num_discrete_vars > 0 or num_continuous_vars > 0:
        out_file.write(b"\t")

for i in range(num_discrete_vars):
    out_file.write(("Discrete{}".format(i+1)).encode())

    if i < (num_discrete_vars - 1) or num_continuous_vars > 0:
        out_file.write(b"\t")

for i in range(num_continuous_vars):
    out_file.write(("Numeric{}".format(i+1)).encode())

    if i < (num_continuous_vars - 1):
        out_file.write(b"\t")

out_file.write(b"\n")

for row_num in range(num_rows):
    out_file.write((f"Row{row_num + 1}\t").encode())

    for i in range(num_categorical_vars):
        output = random.choice(few_letters)

        if i < (num_categorical_vars - 1) or num_discrete_vars > 0 or num_continuous_vars > 0:
            output += "\t"

        out_file.write(output.encode())

    for i in range(num_discrete_vars):
        output = random.choice(all_letters) + random.choice(all_letters)

        if i < (num_discrete_vars - 1) or num_continuous_vars > 0:
            output += "\t"

        out_file.write(output.encode())

    for i in range(num_continuous_vars):
        output = "{:.8f}".format(random.random())

        if i < (num_continuous_vars - 1):
            output += "\t"

        out_file.write(output.encode())

    out_file.write(b"\n")

out_file.close()
