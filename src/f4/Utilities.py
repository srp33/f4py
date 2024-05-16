from contextlib import contextmanager
from copy import deepcopy
import csv
from datetime import datetime
from glob import glob
import gzip
from fastnumbers import isint, isfloat, fast_int, fast_float
from inspect import stack
from itertools import chain
from math import ceil, log
from mmap import mmap, PROT_READ, PROT_WRITE
from msgspec import msgpack
from operator import eq, ge, gt, le, lt, ne, itemgetter
from os import makedirs, path, remove, rename
from re import compile
# import shelve
from shutil import copy, rmtree
import sqlite3
import sys
from tempfile import mkdtemp
from uuid import uuid4
from zstandard import ZstdCompressor, ZstdDecompressor

def get_current_version():
    return "1.0.3"

#####################################################
# Constants
#####################################################

def get_current_version_major():
    return get_current_version().split(".")[0]

def read_from_file(file_handle, start_position, end_position, use_memory_mapping):
    if use_memory_mapping:
        return file_handle[start_position:end_position]
    else:
        file_handle.seek(start_position)
        return file_handle.read(end_position - start_position)

def read_str_from_file(file_path, file_extension=""):
    with open_temp_file_compressed(file_path + file_extension) as the_file:
        return the_file.read()

def write_str_to_file(file_path, the_string, save_original_size=True):
    with open_temp_file_to_compress(file_path) as the_file:
        the_file.write(the_string)

    if save_original_size:
        write_temp_file_original_size(file_path, len(the_string))

def print_message(message, verbose=False, count=None):
    if verbose:
        if count:
            if log(count, 10) % 1 == 0:
                sys.stderr.write(f"{message} - count = {int(count)} - {datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')}\n")
                sys.stderr.flush()

            # log_count = log(count, 10)
            # These calculations take a while, so you could find a way to reduce the number
            # of times they are done.
            # thresholds = [10**power for power in range(ceil(log_count), floor(log_count) - 1, -1)]
            #
            # for i, threshold in enumerate(thresholds):
            #     if count % threshold == 0:
            #         sys.stderr.write(f"{message} - count = {int(count)} - {datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')}\n")
            #         sys.stderr.flush()
            #         break
            #     elif count > threshold:
            #         break
        else:
            sys.stderr.write(f"{message} - {datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')}\n")
            sys.stderr.flush()

def do_nothing(x):
     return(x)

def reverse_string(s):
    return s[::-1]

def has_checkpoint_been_reached_previously(use_checkpoints, tmp_dir_path, chunk_number, verbose):
    # This gets the name of the function that called this function.
    checkpoint_name = stack()[1].function
    checkpoint_file_path = f"{tmp_dir_path}checkpoint__{checkpoint_name}__{chunk_number}"

    if use_checkpoints and path.exists(checkpoint_file_path):
        print_message(f"Using previously saved data for checkpoint {checkpoint_name} from {checkpoint_file_path}.", verbose)
        return True

    return False

def record_checkpoint_reached(use_checkpoints, tmp_dir_path, chunk_number):
    # This gets the name of the function that called this function.
    checkpoint_name = stack()[1].function
    checkpoint_file_path = f"{tmp_dir_path}checkpoint__{checkpoint_name}__{chunk_number}"

    if use_checkpoints:
        write_str_to_file(checkpoint_file_path, b"", False)

def get_delimited_file_handle(file_path):
    if file_path.endswith(".gz"):
        return gzip.open(file_path)
    elif file_path.endswith(".zstd"):
        # FYI: This compression format is a hidden feature for input files.
        # Skipping comments is not supported.
        return open_temp_file_compressed(file_path)
    else:
        return open(file_path, "rb")

def write_temp_file_original_size(file_path, num_bytes):
    with open(get_temp_file_original_size_path(file_path), "wb") as size_file:
        size_file.write(str(num_bytes).encode())

def get_temp_file_original_size(file_path):
    with open(get_temp_file_original_size_path(file_path), "rb") as size_file:
        return int(size_file.read().decode())

def get_temp_file_original_size_path(file_path):
    return f"{file_path}__original_size"

def open_temp_file_to_compress(file_path, mode="wb"):
    fh = open(file_path, mode)
    return ZstdCompressor(level=1, write_content_size=True).stream_writer(fh)

def open_temp_file_compressed(file_path):
    fh = open(file_path, "rb")
    return ZstdDecompressor().stream_reader(fh)

def read_compressed_file_line_by_line(compressed_file_path):
    with open(compressed_file_path, 'rb') as compressed_file:
        dctx = ZstdDecompressor()

        # Create a stream reader to decompress data as it's read
        with dctx.stream_reader(compressed_file) as reader:
            # Buffer for storing decompressed data
            buffer = bytearray()
            while True:
                # Read a chunk of decompressed data
                chunk = reader.read(16384)  # Adjust chunk size as needed
                if not chunk:
                    break  # End of file

                buffer.extend(chunk)

                # Process buffer line by line
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    yield line

            # Yield the last line if there's no trailing newline
            if buffer:
                yield buffer

def format_string_as_fixed_width(x, size):
    return x + b" " * (size - len(x))

# def compress_using_2_grams(value, compression_dict):
#     compressed_value = b""
#
#     for start_i in range(0, len(value), 2):
#         end_i = (start_i + 2)
#         gram = value[start_i:end_i]
#         compressed_value += compression_dict[gram]
#
#     return compressed_value
#
# def get_bigram_size(num_bigrams):
#     return ceil(log(num_bigrams, 2) / 8)

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

def convert_bytes_to_str(bytes_data):
    return bytes_data.decode()

def convert_bytes_to_int(b):
    return int.from_bytes(b, byteorder="big")

def serialize(obj):
    #https://github.com/TkTech/json_benchmark
    return msgpack.encode(obj)

def deserialize(msg):
    return msgpack.decode(msg)

def fix_dir_path_ending(dir_path):
    return dir_path if dir_path.endswith("/") else dir_path + "/"

def prepare_tmp_dir(tmp_dir_path):
    # Figure out where temp files will be stored and create directory, if needed.
    if tmp_dir_path:
        makedirs(tmp_dir_path, exist_ok=True)
        tmp_dir_path = fix_dir_path_ending(tmp_dir_path)
        use_checkpoints = True
    else:
        tmp_dir_path = mkdtemp()
        tmp_dir_path = fix_dir_path_ending(tmp_dir_path)# + f"f4_{uuid4()}/"
        use_checkpoints = False

    return tmp_dir_path, use_checkpoints

def remove_tmp_dir(tmp_dir_path, verbose):
    # Remove the temp directory if it was generated by the code (not the user).
    if tmp_dir_path:
        try:
            rmtree(tmp_dir_path)
            print_message(f"Removed {tmp_dir_path} directory", verbose)
        except:
            print_message(f"Warning: {tmp_dir_path} directory could not be removed", verbose)
            pass

def remove_tmp_file(file_path):
    if path.exists(file_path):
        remove(file_path)

def split_list_into_chunks(my_list, max_items_per_chunk):
    cursor = 0
    total_elements = len(my_list)

    while cursor < total_elements:
        yield my_list[cursor:(cursor + max_items_per_chunk)]
        cursor += max_items_per_chunk

def iterate_single_value(value):
    yield value

def generate_range_chunks(total_n, n_per_chunk):
    num_chunks = ceil(total_n / n_per_chunk)
    current_n = -n_per_chunk

    for chunk_index in range(num_chunks):
        current_n += n_per_chunk
        yield range(current_n, min(total_n, current_n + n_per_chunk))

def connect_sql(file_path):
    conn = sqlite3.connect(
        file_path,
        isolation_level = None,
        detect_types = sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES,
        timeout = 30)

    conn.row_factory = sqlite3.Row

    execute_sql(conn, "PRAGMA synchronous=NORMAL")
    execute_sql(conn, "PRAGMA cache_size=100000")
    execute_sql(conn, 'PRAGMA journal_mode=OFF')

    return conn

def execute_sql(conn, sql, params=(), commit=True):
    cursor = conn.cursor()
    cursor.execute(sql, params)
    lastrowid = cursor.lastrowid
    cursor.close()

    if commit:
        conn.commit()

    return lastrowid

def query_sql(conn, sql, params=()):
    cursor = conn.cursor()
    cursor.execute(sql, params)
    result = cursor.fetchall()
    cursor.close()

    return result

# def convert_to_sql_type(type_abbreviation):
#     if type_abbreviation == "i":
#         return "integer"
#     elif type_abbreviation == "f":
#         return "real"
#     else:
#         return "text"

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
