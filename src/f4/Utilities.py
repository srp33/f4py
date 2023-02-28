import copy
import datetime
import glob
import gzip
import fastnumbers
from itertools import chain
from joblib import Parallel, delayed
import math
import mmap
import msgspec
import operator
from operator import itemgetter
import os
import re
import shutil
import sys
import tempfile
import zstandard

# We use these dictionaries so that when we store the file map, it takes less space on disk.
FILE_KEY_ABBREVIATIONS = {"data": 1, "cc": 2, "mccl": 3, "ll": 4, "ct": 5, "mctl": 6, "cmpr": 7, "cndata": 8, "cncc": 9, "cnmccl": 10, "cnll": 11}

def open_read_file(file_path, file_extension=""):
    the_file = open(file_path + file_extension, 'rb')
    return mmap.mmap(the_file.fileno(), 0, prot=mmap.PROT_READ)

def read_str_from_file(file_path, file_extension=""):
    with open(file_path + file_extension, 'rb') as the_file:
        return the_file.read().rstrip()

def read_int_from_file(file_path, file_extension=""):
    return fastnumbers.fast_int(read_str_from_file(file_path, file_extension))

def write_str_to_file(file_path, the_string):
    with open(file_path, 'wb') as the_file:
        the_file.write(the_string)

def get_column_start_coords(column_sizes):
    # Calculate the position where each column starts.
    column_start_coords = []
    cumulative_position = 0
    for column_size in column_sizes:
        column_start_coords.append(str(cumulative_position).encode())
        cumulative_position += column_size
    column_start_coords.append(str(cumulative_position).encode())

    return column_start_coords

def build_string_map(the_list):
    # Find maximum length of value.
    max_value_length = get_max_string_length(the_list)

    column_items = format_column_items(the_list, max_value_length)
    #return b"\n".join(column_items), max_value_length
    return b"".join(column_items), max_value_length

def get_max_string_length(the_list):
    return max([len(x) for x in set(the_list)])

def format_column_items(the_list, max_value_length, suffix=""):
    formatter = "{:<" + str(max_value_length) + "}" + suffix
    return [formatter.format(value.decode()).encode() for value in the_list]

def print_message(message, verbose=False):
    if verbose:
        print(f"{message} - {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')}")

def decode_string(x):
    return x.decode()

def do_nothing(x):
     return(x)

def get_conversion_function(column_type):
    if column_type == "n": # column name
        return do_nothing
    elif column_type == "s":
        return decode_string
    elif column_type == "f":
        return fastnumbers.fast_float
    else:
        return fastnumbers.fast_int

def sort_first_column(list_of_lists):
    list_of_lists.sort(key=itemgetter(0))

def sort_first_two_columns(list_of_lists):
    list_of_lists.sort(key=itemgetter(0, 1))

def reverse_string(s):
    return s[::-1]

def get_delimited_file_handle(file_path):
    if file_path.endswith(".gz"):
        return gzip.open(file_path)
    else:
        return open(file_path, 'rb')

def format_string_as_fixed_width(x, size):
    return x + b" " * (size - len(x))

def compress_using_2_grams(value, compression_dict):
    compressed_value = b""

    for start_i in range(0, len(value), 2):
        end_i = (start_i + 2)
        gram = value[start_i:end_i]
        compressed_value += compression_dict[gram]

    return compressed_value

def get_bigram_size(num_bigrams):
    return math.ceil(math.log(num_bigrams, 2) / 8)

def decompress(compressed_value, compression_dict, bigram_size):
    if compression_dict["compression_type"] == b"c":
        return compression_dict["map"][convert_bytes_to_int(compressed_value)]

    value = b""
    for start_pos in range(0, len(compressed_value), bigram_size):
        end_pos = start_pos + bigram_size
        compressed_piece = convert_bytes_to_int(compressed_value[start_pos:end_pos])
        value += compression_dict["map"][compressed_piece]

    return value

def get_decompressor(decompression_type, decompressor):
    if not decompression_type:
        return None

    # We have to instantiate this object more than once because some functions are invoked in parallel, and they cannot be serialized.
    return zstandard.ZstdDecompressor() if decompression_type == "zstd" else decompressor

def convert_bytes_to_int(b):
    return int.from_bytes(b, byteorder="big")

def serialize(obj):
    #https://github.com/TkTech/json_benchmark
    return msgspec.msgpack.encode(obj)
#    return msgpack.packb(obj)

def deserialize(msg):
    return msgspec.msgpack.decode(msg)
#    return msgpack.unpackb(msg, strict_map_key=False)

def prepare_tmp_dirs(tmp_dir_path):
    # Figure out where temp files will be stored and create directory, if needed.
    if tmp_dir_path:
        os.makedirs(tmp_dir_path, exist_ok=True)
    else:
        tmp_dir_path = tempfile.mkdtemp()

    tmp_dir_path = fix_dir_path_ending(tmp_dir_path)

    tmp_dir_path_chunks = f"{tmp_dir_path}chunks/"
    tmp_dir_path_outputs = f"{tmp_dir_path}outputs/"
    tmp_dir_path_indexes = f"{tmp_dir_path}indexes/"

    os.makedirs(tmp_dir_path_chunks, exist_ok=True)
    os.makedirs(tmp_dir_path_outputs, exist_ok=True)
    os.makedirs(tmp_dir_path_indexes, exist_ok=True)

    return tmp_dir_path_chunks, tmp_dir_path_outputs, tmp_dir_path_indexes

def fix_dir_path_ending(dir_path):
    return dir_path if dir_path.endswith("/") else dir_path + "/"

def remove_tmp_dir(tmp_dir_path):
    # Remove the temp directory if it was generated by the code (not the user).
    if tmp_dir_path:
        try:
            shutil.rmtree(tmp_dir_path)
            print_message(f"Removed {tmp_dir_path} directory")
        except Exception as e:
            # Don't throw an exception if we can't delete the directory.
            print_message(f"Warning: {tmp_dir_path} directory could not be removed")
            print(e)
            pass

def combine_into_single_file(tmp_dir_path_chunks, tmp_dir_path_outputs, f4_file_path, num_processes=1, num_rows_per_write=1):
    def _create_file_map(start_end_positions):
        start_end_dict = {}

        for x in start_end_positions:
            file_name = x[0]
            start_end_dict[FILE_KEY_ABBREVIATIONS[file_name]] = x[1:]
            #start_end_dict[file_name] = x[1:]

        serialized = serialize(start_end_dict)

        return str(len(serialized)).encode() + b"\n" + serialized

    # Find out which files we have
    data_files = [x for x in glob.glob(f"{tmp_dir_path_chunks}*")]
    other_files = [x for x in glob.glob(f"{tmp_dir_path_outputs}*")]

    # Find out how much data we have
    data_size = sum([os.path.getsize(f) for f in data_files])

    # Determine start and end position of each file within the combined file
    start_end_positions = [["data", 0, data_size]]
    for f in other_files:
        start = start_end_positions[-1][-1]
        end = start + os.path.getsize(f)
        start_end_positions.append([os.path.basename(f), start, end])

    # Adjust the start and end positions based on the size of the values in this list.
    # The size of the file map can change as we advance the positions of the other file contents.
    original_start_end_positions = copy.deepcopy(start_end_positions)
    file_map_serialized = _create_file_map(start_end_positions)
    previous_file_map_length = 0

    while previous_file_map_length != (len(file_map_serialized)):
        previous_file_map_length = len(file_map_serialized)

        start_end_positions = copy.deepcopy(original_start_end_positions)
        for i in range(len(start_end_positions)):
            start_end_positions[i][1] += previous_file_map_length
            start_end_positions[i][2] += previous_file_map_length

        file_map_serialized = _create_file_map(start_end_positions)

    # Write the output file
    with open(f4_file_path, "wb") as f4_file:
        f4_file.write(file_map_serialized)

        _add_data_chunks(tmp_dir_path_chunks, f4_file, num_processes, num_rows_per_write)

        for other_file_path in other_files:
            f4_file.write(read_str_from_file(other_file_path))

def _add_data_chunks(tmp_dir_path_chunks, out_file_handle, num_processes, num_rows_per_write):
    out_lines = []

    for i in range(num_processes):
        chunk_file_path = f"{tmp_dir_path_chunks}{i}"

        if not os.path.exists(chunk_file_path):
            continue

        with open(chunk_file_path, "rb") as chunk_file:
            for line in chunk_file:
                out_lines.append(line)

                if len(out_lines) % num_rows_per_write == 0:
                    out_file_handle.write(b"".join(out_lines))
                    out_lines = []

    if len(out_lines) > 0:
        out_file_handle.write(b"".join(out_lines))

def get_index_file_path(data_file_path, index_name, custom_index_function=do_nothing):
    index_file_path_extension = f".idx_{index_name}"

    if custom_index_function != do_nothing:
        index_file_path_extension = f"{index_file_path_extension}_{custom_index_function.__name__}"

    return f"{data_file_path}{index_file_path_extension}"