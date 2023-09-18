from contextlib import contextmanager
from copy import deepcopy
import csv
from datetime import datetime
from glob import glob
import gzip
from fastnumbers import isint, isfloat, fast_int, fast_float
from itertools import chain
from math import ceil, log
from mmap import mmap, PROT_READ, PROT_WRITE
from msgspec import msgpack
from operator import eq, ge, gt, le, lt, ne, itemgetter
from os import makedirs, path, remove
from re import compile
import shelve
from shutil import copy, rmtree
import sqlite3
import sys
from tempfile import mkdtemp
from zstandard import ZstdCompressor, ZstdDecompressor

# We use these dictionaries so that when we store the file map, it takes less space on disk.
FILE_KEY_ABBREVIATIONS_STATS = {"mccl": 3, "mctl": 6, "cnmccl": 10, "cnll": 11, "mlll": 13}
FILE_KEY_ABBREVIATIONS_OTHER = {"cmpr": 7, "ver": 12}
FILE_KEY_ABBREVIATIONS_NOCACHE = {"data": 1, "cc": 2, "ll": 4, "ct": 5, "cndata": 8, "cncc": 9}

def get_current_version():
    return "0.5.3"

def get_current_version_major():
    return get_current_version().split(".")[0]

def read_str_from_file(file_path, file_extension=""):
    with open(file_path + file_extension, 'rb') as the_file:
        return the_file.read()

def write_str_to_file(file_path, the_string):
    with open(file_path, 'wb') as the_file:
        the_file.write(the_string)

def save_column_coords(column_sizes_dict_file_path, column_coords_dict_file_path):
    # Calculate the position where each column starts.
    with shelve.open(column_sizes_dict_file_path, "r") as column_sizes_dict:
        with shelve.open(column_coords_dict_file_path, "n") as column_coords_dict:
            cumulative_position = 0

            for column_index in range(len(column_sizes_dict)):
                column_index = str(column_index)
                column_coords_dict[column_index] = cumulative_position
                cumulative_position += column_sizes_dict[column_index]

            column_coords_dict[str(len(column_sizes_dict))] = cumulative_position

    # column_start_coords = []
    # cumulative_position = 0
    #
    # for column_size in column_sizes:
    #     column_start_coords.append(str(cumulative_position).encode())
    #     cumulative_position += column_size
    #
    # column_start_coords.append(str(cumulative_position).encode())
    #
    # return column_start_coords

def save_string_map(dict_file_path, out_values_file_path, out_lengths_file_path):
    with shelve.open(dict_file_path, "r") as the_dict:
        # Find maximum length of value.
        max_value_length = get_max_string_length(the_dict)
        write_str_to_file(out_lengths_file_path, str(max_value_length).encode())

        with open(out_values_file_path, "wb") as out_file:
            formatter = "{:<" + str(max_value_length) + "}"

            for index in range(len(the_dict)):
                value = the_dict[str(index)]
                out_file.write(formatter.format(value).encode())

    # # Find maximum length of value.
    # max_value_length = get_max_string_length(the_list)
    #
    # column_items = format_column_items(the_list, max_value_length)
    # return b"".join(column_items), max_value_length

def get_max_string_length(the_dict):
    max_length = 0

    for key, value in the_dict.items():
        length = len(str(value))
        if length > max_length:
            max_length = length

    return max_length
    # return max([len(x) for x in set(the_list)])

# def format_column_items(the_list, max_value_length):
#     formatter = "{:<" + str(max_value_length) + "}"
#     return [formatter.format(value.decode()).encode() for value in the_list]

def print_message(message, verbose=False, count=None):
    if verbose:
        if count:
            for y in range(20, 0, -1):
                if count <= 10**y and count % 10**(y - 1) == 0:
                    sys.stderr.write(f"{message} - count = {count} - {datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')}\n")
                    sys.stderr.flush()
                    break
        else:
            sys.stderr.write(f"{message} - {datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')}\n")
            sys.stderr.flush()

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
        return fast_float
    else:
        return fast_int

def sort_first_column(list_of_lists):
    list_of_lists.sort(key=itemgetter(0))

def sort_first_two_columns(list_of_lists):
    list_of_lists.sort(key=itemgetter(0, 1))

def reverse_string(s):
    return s[::-1]

def get_delimited_file_handle(file_path):
    if file_path.endswith(".gz"):
        return gzip.open(file_path)
    elif file_path.endswith(".zstd"):
        with open(file_path, "rb") as fh:
            return ZstdCompressor(level=1).stream_reader(fh)
    else:
        return open(file_path, "rb")

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
    return ceil(log(num_bigrams, 2) / 8)

# def compress_file_zstd(in_file, out_file):
#     chunk_size = 262144
#     compressor = ZstdCompressor(level=1)
#
#     read_count, write_count = compressor.copy_stream(in_file, out_file, read_size=chunk_size, write_size=chunk_size)
#
#     return write_count

# def decompress_file_zstd(in_file, out_file):
#     chunk_size = 262144
#     decompressor = ZstdDecompressor()
#
#     read_count, write_count = decompressor.copy_stream(in_file, out_file, read_size=chunk_size, write_size=chunk_size)
#
#     return write_count

def decompress(compressed_value, compression_dict, bigram_size):
    if compression_dict["compression_type"] == b"c":
        return compression_dict["map"][convert_bytes_to_int(compressed_value)]

    value = b""
    for start_pos in range(0, len(compressed_value), bigram_size):
        end_pos = start_pos + bigram_size
        compressed_piece = convert_bytes_to_int(compressed_value[start_pos:end_pos])
        value += compression_dict["map"][compressed_piece]

    return value

def convert_bytes_to_int(b):
    return int.from_bytes(b, byteorder="big")

def serialize(obj):
    #https://github.com/TkTech/json_benchmark
    return msgpack.encode(obj)
#    return msgpack.packb(obj)

def deserialize(msg):
    return msgpack.decode(msg)
#    return msgpack.unpackb(msg, strict_map_key=False)

def fix_dir_path_ending(dir_path):
    return dir_path if dir_path.endswith("/") else dir_path + "/"

def remove_tmp_dir(tmp_dir_path):
    # Remove the temp directory if it was generated by the code (not the user).
    if tmp_dir_path:
        try:
            rmtree(tmp_dir_path)
            print_message(f"Removed {tmp_dir_path} directory")
        except Exception as e:
            # Don't throw an exception if we can't delete the directory.
            print_message(f"Warning: {tmp_dir_path} directory could not be removed")
            print(e)
            pass

def combine_into_single_file(tmp_dir_path_chunks, tmp_dir_path_outputs, f4_file_path, num_parallel=1):
    def _create_file_map(start_end_positions):
        start_end_dict = {}

        for x in start_end_positions:
            file_name = x[0]

            if file_name in FILE_KEY_ABBREVIATIONS_STATS:
                start_end_dict[FILE_KEY_ABBREVIATIONS_STATS[file_name]] = x[1:]
            elif file_name in FILE_KEY_ABBREVIATIONS_OTHER:
                start_end_dict[FILE_KEY_ABBREVIATIONS_OTHER[file_name]] = x[1:]
            else:
                start_end_dict[FILE_KEY_ABBREVIATIONS_NOCACHE[file_name]] = x[1:]

        serialized = serialize(start_end_dict)

        return str(len(serialized)).encode() + b"\n" + serialized

    # Find out which files we have
    data_files = [x for x in glob(f"{tmp_dir_path_chunks}*")]
    other_files = [x for x in glob(f"{tmp_dir_path_outputs}*")]

    # Find out how much data we have
    data_size = sum([path.getsize(f) for f in data_files])

    # Determine start and end position of each file within the combined file
    start_end_positions = [["data", 0, data_size]]
    for f in other_files:
        start = start_end_positions[-1][-1]
        end = start + path.getsize(f)
        start_end_positions.append([path.basename(f), start, end])

    # Adjust the start and end positions based on the size of the values in this list.
    # The size of the file map can change as we advance the positions of the other file contents.
    original_start_end_positions = deepcopy(start_end_positions)
    file_map_serialized = _create_file_map(start_end_positions)
    previous_file_map_length = 0

    while previous_file_map_length != (len(file_map_serialized)):
        previous_file_map_length = len(file_map_serialized)

        start_end_positions = deepcopy(original_start_end_positions)
        for i in range(len(start_end_positions)):
            start_end_positions[i][1] += previous_file_map_length
            start_end_positions[i][2] += previous_file_map_length

        file_map_serialized = _create_file_map(start_end_positions)

    # Write the output file
    with open(f4_file_path, "wb") as f4_file:
        f4_file.write(file_map_serialized)

        add_data_chunks(tmp_dir_path_chunks, f4_file, num_parallel)

        for other_file_path in other_files:
            f4_file.write(read_str_from_file(other_file_path))

def add_data_chunks(tmp_dir_path_chunks, out_file, num_parallel):
    for i in range(num_parallel):
        chunk_file_path = f"{tmp_dir_path_chunks}{i}"

        if not path.exists(chunk_file_path):
            continue

        with open(chunk_file_path, "rb") as chunk_file:
            chunk_size = 100000
            while chunk := chunk_file.read(chunk_size):
                out_file.write(chunk)

# def get_index_file_path(f4_file_path):
#     return f"{f4_file_path}.idx.db"

# def get_index_file_path(f4_file_path):
#     for i in range(1, 1000000000):
#         index_file_path = f"{f4_file_path}.idx_{i}"
#
#         if not path.exists(index_file_path):
#             return index_file_path

def get_index_file_path(data_file_path, index_name, custom_index_type=None):
    if custom_index_type == None:
        index_file_path_extension = f"_{index_name}.idx"
    else:
        index_file_path_extension = f"_{index_name}_{custom_index_type}.idx"

    return f"{data_file_path}{index_file_path_extension}"

def split_integer_list_into_chunks(int_list, num_parallel):
    items_per_chunk = ceil(len(int_list) / num_parallel)

    return_indices = list()

    for an_int in int_list:
        return_indices.append(an_int)

        if len(return_indices) == items_per_chunk:
            yield return_indices
            return_indices = list()

    if len(return_indices) > 0:
        yield return_indices

def connect_sql(file_path):
    print(file_path)
    conn = sqlite3.connect(
        file_path,
        isolation_level = None,
        detect_types = sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES,
        timeout = 30)

    conn.row_factory = sqlite3.Row

    execute_sql(conn, "PRAGMA synchronous = OFF")
    execute_sql(conn, "PRAGMA cache_size=1000000")
    execute_sql(conn, "PRAGMA temp_store=MEMORY")
    execute_sql(conn, "PRAGMA journal_mode=MEMORY")
    execute_sql(conn, 'PRAGMA journal_mode=WAL')

    return conn

def execute_sql(conn, sql, params=(), commit=True):
    cursor = conn.cursor()
    cursor.execute(sql, params)
    lastrowid = cursor.lastrowid
    cursor.close()

    if commit:
        conn.commit()

    return lastrowid

def fetchone_sql(conn, sql, params=()):
    cursor = conn.cursor()
    cursor.execute(sql, params)
    result = cursor.fetchone()
    cursor.close()

    return result

# def fetchall_sql(conn, sql, params=()):
#     cursor = conn.cursor()
#     cursor.execute(sql, params)
#     result = cursor.fetchall()
#     cursor.close()
#
#     return result

def convert_to_sql_type(type_abbreviation):
    if type_abbreviation == "i":
        return "integer"
    elif type_abbreviation == "f":
        return "real"
    else:
        return "text"

# def convert_operator_to_sql(op):
#     if op == eq:
#         return "="
#     elif op == ge:
#         return ">="
#     elif op == gt:
#         return ">"
#     elif op == le:
#         return "<="
#     elif op == lt:
#         return "<"
#     return "<>"

def get_path_with_possible_suffix(file_path):
    if path.exists(file_path):
        return file_path

    paths = glob(f"{file_path}*")

    if len(paths) == 0:
        raise Exception("Did not find any file that matched this pattern: {file_path}*", verbose)

    elif len(paths) == 1:
        return paths[0]
    else:
        raise Exception("Found multiple files to remove that matched this pattern: {file_path}*. None were removed.", verbose)

# def remove_file_with_possible_suffix(file_path, verbose):
#     if path.exists(file_path):
#         remove(file_path)
#     else:
#         paths = glob(f"{file_path}*")
#
#         if len(paths) == 0:
#             print_message("Did not find any file to remove that matched this pattern: {file_path}*", verbose)
#         elif len(paths) == 1:
#             remove(paths[0])
#         else:
#             print_message("Found multiple files to remove that matched this pattern: {file_path}*. None were removed.", verbose)