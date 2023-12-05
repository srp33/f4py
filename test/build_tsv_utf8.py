import gzip
import os
import random
import string
import sys

num_rows = int(sys.argv[1])
num_cols = int(sys.argv[2])
out_file_path = sys.argv[3]

def save(text, out_file, flush=False):
    if len(text) >= 256 or flush:
        out_file.write(text)
        return b""

    return text

random.seed(0)

utf8_characters = [chr(i) for i in range(256) if chr(i) not in ["\n", "\t"]]

out_file = gzip.open(out_file_path, 'wb', compresslevel=1)

output = b""

for i in range(num_cols):
    output = save(output + ("X{}".format(i+1)).encode(), out_file)

    if i < (num_cols - 1):
        output = save(output + b"\t", out_file)

output = save(output + b"\n", out_file)

for row_num in range(num_rows):
    if row_num > 0 and row_num % 1000000 == 0:
        print(f"Row {row_num}", flush=True)

    for i in range(num_cols):
        if i > 0 and i % 1000000 == 0:
            print(f"Col {i}", flush=True)

        output = save(output + random.choice(utf8_characters).encode(), out_file)

        if i < (num_cols):
            output = save(output + b"\t", out_file)

    output = save(output + b"\n", out_file)

save(output, out_file, flush=True)
out_file.close()
