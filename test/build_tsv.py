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

def save(text, out_file, flush=False):
    if len(text) > 20000 or flush:
        out_file.write(text)
        return b""

    return text

random.seed(0)

few_letters = string.ascii_letters[26:29]
all_letters = string.ascii_letters[26:]

if out_file_path.endswith(".gz"):
    out_file = gzip.open(out_file_path, 'wb', compresslevel=1)
else:
    out_file = open(out_file_path, 'wb')

output = b"ID\t"

for i in range(num_categorical_vars):
    output = save(output + ("Categorical{}".format(i+1)).encode(), out_file)

    if i < (num_categorical_vars - 1) or num_discrete_vars > 0 or num_continuous_vars > 0:
        output = save(output + b"\t", out_file)

for i in range(num_discrete_vars):
    output = save(output + ("Discrete{}".format(i+1)).encode(), out_file)

    if i < (num_discrete_vars - 1) or num_continuous_vars > 0:
        output = save(output + b"\t", out_file)

for i in range(num_continuous_vars):
    output = save(output + ("Numeric{}".format(i+1)).encode(), out_file)

    if i < (num_continuous_vars - 1):
        output = save(output + b"\t", out_file)

output = save(output + b"\n", out_file)

for row_num in range(num_rows):
    output = save(output + (f"Row{row_num + 1}\t").encode(), out_file)

    for i in range(num_categorical_vars):
        output = save(output + random.choice(few_letters).encode(), out_file)

        if i < (num_categorical_vars - 1) or num_discrete_vars > 0 or num_continuous_vars > 0:
            output = save(output + b"\t", out_file)

    for i in range(num_discrete_vars):
        output = save(output + random.choice(all_letters).encode() + random.choice(all_letters).encode(), out_file)

        if i < (num_discrete_vars - 1) or num_continuous_vars > 0:
            output = save(output + b"\t", out_file)

    for i in range(num_continuous_vars):
        output = save(output + ("{:.8f}".format(random.random())).encode(), out_file)

        if i < (num_continuous_vars - 1):
            output = save(output + b"\t", out_file)

    output = save(output + b"\n", out_file)

save(output, out_file, flush=True)
out_file.close()
