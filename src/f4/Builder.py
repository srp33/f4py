import f4
import fastnumbers
from joblib import Parallel, delayed
import math
import zstandard

class Builder:
    def __init__(self, verbose=False):
        self.__verbose = verbose

    def convert_delimited_file(self, delimited_file_path, f4_file_path, index_columns=[], delimiter="\t", comment_prefix="#", compression_type=None, num_processes=1, num_cols_per_chunk=None, num_rows_per_write=100, tmp_dir_path=None):
        if type(delimiter) != str:
            raise Exception("The delimiter value must be a string.")

        if delimiter not in ("\t"):
            raise Exception("Invalid delimiter. Must be \t.")

        delimiter = delimiter.encode()

        if comment_prefix:
            if type(comment_prefix) != str:
                raise Exception("The comment_prefix value must be a string.")

            comment_prefix = comment_prefix.encode()

            if comment_prefix == "":
                comment_prefix = None

        self._print_message(f"Converting from {delimited_file_path}")

        tmp_dir_path_chunks, tmp_dir_path_outputs, tmp_dir_path_indexes = f4.prepare_tmp_dirs(tmp_dir_path)

        # Get column names. Remove any leading or trailing white space around the column names.
        with f4.get_delimited_file_handle(delimited_file_path) as in_file:
            if comment_prefix:
                for line in in_file:
                    if not line.startswith(comment_prefix):
                        header_line = line
                        break
            else:
                header_line = in_file.readline()

        column_names = [x.strip() for x in header_line.rstrip(b"\n").split(delimiter)]
        num_cols = len(column_names)

        if num_cols == 0:
            raise Exception(f"No data was detected in {delimited_file_path}.")

        # Iterate through the lines to summarize each column.
        self._print_message(f"Summarizing each column in {delimited_file_path}")
        if num_processes == 1:
            chunk_results = [self._parse_columns_chunk(delimited_file_path, delimiter, comment_prefix, 0, num_cols, compression_type)]
        else:
            column_chunk_indices = _generate_chunk_ranges(num_cols, num_cols_per_chunk)
            chunk_results = Parallel(n_jobs=num_processes)(delayed(self._parse_columns_chunk)(delimited_file_path, delimiter, comment_prefix, column_chunk[0], column_chunk[1], compression_type) for column_chunk in column_chunk_indices)

        # Summarize the column sizes and types across the chunks.
        column_sizes = []
        column_types = []
        column_compression_dicts = {}

        for chunk_tuple in chunk_results:
            for i, size in sorted(chunk_tuple[0].items()):
                column_sizes.append(size)

            for i, the_type in sorted(chunk_tuple[1].items()):
                column_types.append(the_type)

            if len(chunk_tuple) > 0:
                # This merges the dictionaries
                column_compression_dicts = {**column_compression_dicts, **chunk_tuple[2]}

        # When each chunk was processed, we went through all rows, so we can get these numbers from just the first chunk.
        num_rows = chunk_results[0][3]

        if num_rows == 0:
            raise Exception(f"A header row but no data rows were detected in {delimited_file_path}")

        line_length = self._get_line_length(delimited_file_path, tmp_dir_path_chunks, delimiter, comment_prefix, compression_type, column_sizes, column_compression_dicts, num_rows, num_processes, num_rows_per_write)

        self._print_message(f"Saving meta files for {f4_file_path}")
        self._write_meta_files(tmp_dir_path_outputs, tmp_dir_path_indexes, column_sizes, line_length, column_names, column_types, compression_type, column_compression_dicts, num_rows)

        self._print_message(f"Combining all data into a single file for {delimited_file_path}")
        f4.combine_into_single_file(tmp_dir_path_chunks, tmp_dir_path_outputs, f4_file_path, num_processes, num_rows_per_write)

        if index_columns:
            f4.IndexBuilder.build_indexes(f4_file_path, index_columns, tmp_dir_path_indexes)

        f4.remove_tmp_dir(tmp_dir_path_chunks)
        f4.remove_tmp_dir(tmp_dir_path_outputs)
        f4.remove_tmp_dir(tmp_dir_path_indexes)

        self._print_message(f"Done converting {delimited_file_path} to {f4_file_path}")

    #####################################################
    # Non-public functions
    #####################################################

    def _write_meta_files(self, tmp_dir_path_outputs, tmp_dir_path_indexes, column_sizes, line_length, column_names=None, column_types=None, compression_type=None, column_compression_dicts=None, num_rows=None):
        # Calculate and write the column coordinates and max length of these coordinates.
        column_start_coords = f4.get_column_start_coords(column_sizes)
        column_coords_string, max_column_coord_length = f4.build_string_map(column_start_coords)
        f4.write_str_to_file(f"{tmp_dir_path_outputs}cc", column_coords_string)
        f4.write_str_to_file(f"{tmp_dir_path_outputs}mccl", str(max_column_coord_length).encode())

        # Find and write the line length.
        f4.write_str_to_file(f"{tmp_dir_path_outputs}ll", str(line_length).encode())

        column_index_name_dict = {}
        column_name_index_dict = {}
        for column_index, column_name in enumerate(column_names):
            column_index_name_dict[column_index] = column_name
            column_name_index_dict[column_name] = column_index

        # Build an index of the column names and write this to a file.
        sorted_column_names = sorted(column_names)
        values_positions = [[x.decode(), column_name_index_dict[x]] for x in sorted_column_names]
        f4.IndexBuilder._customize_values_positions(values_positions, ["n"], f4.sort_first_column, f4.do_nothing)
        f4.IndexBuilder._write_index_files(values_positions, None, f"{tmp_dir_path_outputs}cn")

        if column_types:
            # Build a map of the column types and write this to a file.
            column_types_string, max_col_type_length = f4.build_string_map(column_types)
            f4.write_str_to_file(f"{tmp_dir_path_outputs}ct", column_types_string)
            f4.write_str_to_file(f"{tmp_dir_path_outputs}mctl", str(max_col_type_length).encode())

        self._write_compression_info(tmp_dir_path_outputs, compression_type, column_compression_dicts, column_index_name_dict)

    def _parse_columns_chunk(self, delimited_file_path, delimiter, comment_prefix, start_index, end_index, compression_type):
        with f4.get_delimited_file_handle(delimited_file_path) as in_file:
            self._exclude_comments_and_header(in_file, comment_prefix)

            # Initialize the column sizes and types.
            column_sizes_dict = {}
            column_types_values_dict = {} # TODO: This dictionary could get really large. Could modify the code to use sqlitedict or https://stackoverflow.com/questions/47233562/key-value-store-in-python-for-possibly-100-gb-of-data-without-client-server.
            column_types_dict = {}
            for i in range(start_index, end_index):
                column_sizes_dict[i] = 0
                column_types_values_dict[i] = {b"i": set(), b"f": set(), b"s": set()}

            # Loop through the file for the specified columns.
            num_rows = 0
            for line in in_file:
                line = line.rstrip(b"\n")

                line_items = line.split(delimiter)
                for i in range(start_index, end_index):
                    column_sizes_dict[i] = max([column_sizes_dict[i], len(line_items[i])])
                    inferred_type = _infer_type(line_items[i])
                    column_types_values_dict[i][inferred_type].add(line_items[i])

                num_rows += 1

                if num_rows % 100000 == 0:
                    self._print_message(f"Processed line {num_rows} of {delimited_file_path} for columns {start_index} - {end_index - 1}")

        for i in range(start_index, end_index):
            column_types_dict[i] = _infer_type_for_column(column_types_values_dict[i])

        column_compression_dicts = {}

        if compression_type == "dictionary":
            for i in range(start_index, end_index):
                unique_values = list(column_types_values_dict[i][b"s"] | column_types_values_dict[i][b"i"] | column_types_values_dict[i][b"f"])
                unique_values = sorted(unique_values)

                use_categorical_compression = (len(unique_values) / num_rows) <= 0.1
                column_compression_dicts[i] = {}
                column_compression_dicts[i]["map"] = {}

                if use_categorical_compression:
                    column_compression_dicts[i]["compression_type"] = b"c"
                    num_bytes = f4.get_bigram_size(len(unique_values))

                    for j, value in _enumerate_for_compression(unique_values):
                        #column_compression_dicts[i]["map"][value] = int2ba(j, length = length).to01()
                        column_compression_dicts[i]["map"][value] = j.to_bytes(length = num_bytes, byteorder = "big")

                    column_sizes_dict[i] = num_bytes
                else:
                    column_compression_dicts[i]["compression_type"] = column_types_dict[i]
                    bigrams = _find_unique_bigrams(unique_values)
                    num_bytes = f4.get_bigram_size(len(bigrams))

                    for j, gram in _enumerate_for_compression(bigrams):
                        #column_compression_dicts[i]["map"][gram] = int2ba(j, length = length).to01()
                        column_compression_dicts[i]["map"][gram] = j.to_bytes(length = num_bytes, byteorder = "big")

                    column_sizes_dict[i] = 0
                    for unique_value in unique_values:
                        compressed_length = len(f4.compress_using_2_grams(unique_value, column_compression_dicts[i]["map"]))
                        column_sizes_dict[i] = max(column_sizes_dict[i], compressed_length)

        return column_sizes_dict, column_types_dict, column_compression_dicts, num_rows

    def _get_line_length(self, delimited_file_path, tmp_dir_path_chunks, delimiter, comment_prefix, compression_type, column_sizes, compression_dicts, num_rows, num_processes, num_rows_per_write):
        self._print_message(f"Parsing chunks of {delimited_file_path} and saving to temp directory ({tmp_dir_path_chunks})")

        if num_processes == 1:
            line_length = self._write_rows_chunk(delimited_file_path, tmp_dir_path_chunks, delimiter, comment_prefix, compression_type, column_sizes, compression_dicts, 0, 0, num_rows, num_rows_per_write)
        else:
            row_chunk_indices = _generate_chunk_ranges(num_rows, math.ceil(num_rows / num_processes) + 1)

            # Find the line length.
            max_line_sizes = Parallel(n_jobs=num_processes)(delayed(self._write_rows_chunk)(delimited_file_path, tmp_dir_path_chunks, delimiter, comment_prefix, compression_type, column_sizes, compression_dicts, i, row_chunk[0], row_chunk[1], num_rows_per_write) for i, row_chunk in enumerate(row_chunk_indices))
            line_length = max(max_line_sizes)

        return line_length

    def _write_rows_chunk(self, delimited_file_path, tmp_dir_path, delimiter, comment_prefix, compression_type, column_sizes, compression_dicts, chunk_number, start_index, end_index, num_rows_per_write):
        max_line_size = 0

        if compression_type == "zstd":
            compressor = zstandard.ZstdCompressor(level = 0)

        # Write the data to output file. Ignore the header line.
        with f4.get_delimited_file_handle(delimited_file_path) as in_file:
            self._exclude_comments_and_header(in_file, comment_prefix)

            with open(f"{tmp_dir_path}{chunk_number}", 'wb') as chunk_file:
                out_lines = []

                line_index = -1
                for line in in_file:
                    # Check whether we should process the specified line.
                    line_index += 1
                    if line_index < start_index:
                        continue
                    if line_index == end_index:
                        break

                    # Parse the data from the input file.
                    line_items = line.rstrip(b"\n").split(delimiter)

                    out_items = []

                    if compression_type == "dictionary":
                        for i, size in enumerate(column_sizes):
                            compressed_value = f4.compress_using_2_grams(line_items[i], compression_dicts[i]["map"])
                            out_items.append(f4.format_string_as_fixed_width(compressed_value, size))
                    else:
                        for i, size in enumerate(column_sizes):
                            out_items.append(f4.format_string_as_fixed_width(line_items[i], size))

                    out_line = b"".join(out_items)

                    if compression_type == "zstd":
                        out_line = compressor.compress(out_line)

                    line_size = len(out_line)
                    max_line_size = max([max_line_size, line_size])

                    out_lines.append(out_line)

                    if len(out_lines) % num_rows_per_write == 0:
                        self._print_message(f"Processed chunk of {delimited_file_path} at line {line_index} (start_index = {start_index}, end_index = {end_index})")
                        chunk_file.write(b"".join(out_lines))
                        out_lines = []

                if len(out_lines) > 0:
                    chunk_file.write(b"".join(out_lines))

        return max_line_size

    def _exclude_comments_and_header(self, in_file, comment_prefix):
        # Ignore the header because we don't need column names here. Also ignore commented lines.
        if comment_prefix:
            for line in in_file:
                if not line.startswith(comment_prefix):
                    break
        else:
            in_file.readline()

    def _write_compression_info(self, tmp_dir_path_outputs, compression_type, column_compression_dicts, column_index_name_dict):
        if not compression_type:
            return

        with open(f"{tmp_dir_path_outputs}cmpr", "wb") as cmpr_file:
            if compression_type == "dictionary":
                column_decompression_dicts = {}
                # To enable decompression, we need to invert the column indices and column names.
                # We also need to invert the values and compressed values.
                for column_index, compression_dict in column_compression_dicts.items():
                    column_name = column_index_name_dict[column_index]
                    column_decompression_dicts[column_name] = column_compression_dicts[column_index]

                    decompression_dict = {}
                    for value, compressed_value in compression_dict["map"].items():
                        decompression_dict[f4.convert_bytes_to_int(compressed_value)] = value

                    column_decompression_dicts[column_name]["map"] = decompression_dict

                cmpr_file.write(f4.serialize(column_decompression_dicts))
            else:
                cmpr_file.write(b"z")

    def _print_message(self, message):
        f4.print_message(message, self.__verbose)

#####################################################
# Class functions (non-public)
#####################################################

def _generate_chunk_ranges(num_cols, num_cols_per_chunk):
    if num_cols_per_chunk:
        last_end_index = 0

        while last_end_index != num_cols:
            last_start_index = last_end_index
            last_end_index = min([last_start_index + num_cols_per_chunk, num_cols])
            yield [last_start_index, last_end_index]
    else:
        yield [0, num_cols]

def _infer_type(value):
    if fastnumbers.isint(value):
        return b"i"
    if fastnumbers.isfloat(value):
        return b"f"
    return b"s"

def _infer_type_for_column(types_dict):
    if len(types_dict) == 0:
        return None

    if len(types_dict[b"s"]) > 0:
        return b"s"
    elif len(types_dict[b"f"]) > 0:
        return b"f"

    return b"i"

def _find_unique_bigrams(values):
    grams = set()

    for value in values:
        for start_i in range(0, len(value), 2):
            end_i = (start_i + 2)
            grams.add(value[start_i:end_i])

    return sorted(list(grams))

# We skip the space character because it causes a problem when we parse from a file.
def _enumerate_for_compression(values):
    ints = []
    capacity = len(values)
    length = f4.get_bigram_size(capacity)

    i = 0
    while len(ints) < capacity:
        if b' ' not in i.to_bytes(length = length, byteorder = "big"):
            ints.append(i)

        i += 1

    for i in ints:
        yield i, values.pop(0)