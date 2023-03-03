from .Parser import *
from .Utilities import *

def convert_delimited_file(delimited_file_path, f4_file_path, index_columns=[], delimiter="\t", comment_prefix="#", compression_type=None, num_processes=1, num_cols_per_chunk=None, num_rows_per_write=100, tmp_dir_path=None, verbose=False):
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

    print_message(f"Converting from {delimited_file_path}", verbose)

    tmp_dir_path_chunks, tmp_dir_path_outputs, tmp_dir_path_indexes = prepare_tmp_dirs(tmp_dir_path)

    # Get column names. Remove any leading or trailing white space around the column names.
    with get_delimited_file_handle(delimited_file_path) as in_file:
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
    print_message(f"Summarizing each column in {delimited_file_path}", verbose)
    if num_processes == 1:
        chunk_results = [_parse_columns_chunk(delimited_file_path, delimiter, comment_prefix, 0, num_cols, num_rows_per_write, compression_type, verbose)]
    else:
        column_chunk_indices = _generate_chunk_ranges(num_cols, num_cols_per_chunk)
        chunk_results = Parallel(n_jobs=num_processes)(delayed(_parse_columns_chunk)(delimited_file_path, delimiter, comment_prefix, column_chunk[0], column_chunk[1], num_rows_per_write, compression_type, verbose) for column_chunk in column_chunk_indices)

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

    print_message(f"Parsing chunks of {delimited_file_path} and saving to temp directory ({tmp_dir_path_chunks})", verbose)
    line_length = _get_line_length(delimited_file_path, tmp_dir_path_chunks, delimiter, comment_prefix, compression_type, column_sizes, column_compression_dicts, num_rows, num_processes, num_rows_per_write, verbose)

    print_message(f"Saving meta files for {f4_file_path}", verbose)
    _write_meta_files(tmp_dir_path_outputs, tmp_dir_path_indexes, column_sizes, line_length, column_names, column_types, compression_type, column_compression_dicts, num_rows)

    print_message(f"Combining all data into a single file for {delimited_file_path}", verbose)
    combine_into_single_file(tmp_dir_path_chunks, tmp_dir_path_outputs, f4_file_path, num_processes, num_rows_per_write)

    if index_columns:
        build_indexes(f4_file_path, index_columns, tmp_dir_path_indexes, verbose)

    remove_tmp_dir(tmp_dir_path_chunks)
    remove_tmp_dir(tmp_dir_path_outputs)
    remove_tmp_dir(tmp_dir_path_indexes)

    print_message(f"Done converting {delimited_file_path} to {f4_file_path}", verbose)

#####################################################
# Non-public functions
#####################################################

def _write_meta_files(tmp_dir_path_outputs, tmp_dir_path_indexes, column_sizes, line_length, column_names=None, column_types=None, compression_type=None, column_compression_dicts=None, num_rows=None):
    # Calculate and write the column coordinates and max length of these coordinates.
    column_start_coords = get_column_start_coords(column_sizes)
    column_coords_string, max_column_coord_length = build_string_map(column_start_coords)
    write_str_to_file(f"{tmp_dir_path_outputs}cc", column_coords_string)
    write_str_to_file(f"{tmp_dir_path_outputs}mccl", str(max_column_coord_length).encode())

    # Find and write the line length.
    write_str_to_file(f"{tmp_dir_path_outputs}ll", str(line_length).encode())

    column_index_name_dict = {}
    column_name_index_dict = {}
    for column_index, column_name in enumerate(column_names):
        column_index_name_dict[column_index] = column_name
        column_name_index_dict[column_name] = column_index

    # Build an index of the column names and write this to a file.
    sorted_column_names = sorted(column_names)
    values_positions = [[x.decode(), column_name_index_dict[x]] for x in sorted_column_names]
    _customize_index_values_positions(values_positions, ["n"], sort_first_column, do_nothing)
    _write_index_files(values_positions, None, f"{tmp_dir_path_outputs}cn")

    if column_types:
        # Build a map of the column types and write this to a file.
        column_types_string, max_col_type_length = build_string_map(column_types)
        write_str_to_file(f"{tmp_dir_path_outputs}ct", column_types_string)
        write_str_to_file(f"{tmp_dir_path_outputs}mctl", str(max_col_type_length).encode())

    _write_compression_info(tmp_dir_path_outputs, compression_type, column_compression_dicts, column_index_name_dict)

def _parse_columns_chunk(delimited_file_path, delimiter, comment_prefix, start_index, end_index, num_rows_per_write, compression_type, verbose):
    with get_delimited_file_handle(delimited_file_path) as in_file:
        _exclude_comments_and_header(in_file, comment_prefix)

        # Initialize the column sizes and types.
        # We will count how many there are of each type.
        column_sizes_dict = {}
        column_types_values_dict = {}

        for i in range(start_index, end_index):
            column_sizes_dict[i] = 0
            column_types_values_dict[i] = {b"i": 0, b"f": 0, b"s": 0}

        # Loop through the file for the specified columns.
        num_rows = 0
        for line in in_file:
            line = line.rstrip(b"\n")

            line_items = line.split(delimiter)
            for i in range(start_index, end_index):
                column_sizes_dict[i] = max([column_sizes_dict[i], len(line_items[i])])

                inferred_type = _infer_type(line_items[i])
                column_types_values_dict[i][inferred_type] += 1

            num_rows += 1

            if num_rows > 0 and num_rows % num_rows_per_write == 0:
                print_message(f"Processed line {num_rows} of {delimited_file_path} for columns {start_index} - {end_index - 1}", verbose)

    column_types_dict = {}
    for i in range(start_index, end_index):
        column_types_dict[i] = _infer_type_for_column(column_types_values_dict[i])

    column_compression_dicts = {}

    if compression_type == "dictionary":
        # Figure out whether we should use categorical compression.
        # If there are more than 100 unique values, do not use categorical compression.
        # This is a rough threshold. It also means that files with few rows will almost always use categorical compression.
        # TODO: Consider refining this approach.
        UNIQUE_THRESHOLD = 100

        # Specify the default compression information for each column.
        # The default is categorical.
        for i in range(start_index, end_index):
            column_compression_dicts[i] = {"compression_type": b"c", "map": {}}

        column_unique_dict = {i: set() for i in range(start_index, end_index)}
        column_max_length_dict = {i: 0 for i in range(start_index, end_index)}
        column_bigrams_dict = {i: set() for i in range(start_index, end_index)}

        with get_delimited_file_handle(delimited_file_path) as in_file:
            _exclude_comments_and_header(in_file, comment_prefix)

            for line in in_file:
                line_items = line.rstrip(b"\n").split(delimiter)

                for i in range(start_index, end_index):
                    value = line_items[i]
                    column_max_length_dict[i] = max(column_max_length_dict[i], len(value))

                    for bigram in _find_unique_bigrams(value):
                        column_bigrams_dict[i].add(bigram)

                    if column_compression_dicts[i]["compression_type"] == b"c":
                        column_unique_dict[i].add(value)

                        if len(column_unique_dict[i]) >= UNIQUE_THRESHOLD:
                            column_compression_dicts[i]["compression_type"] = column_types_dict[i]
                            column_unique_dict[i] = None

        for i in range(start_index, end_index):
            if column_compression_dicts[i]["compression_type"] == b"c":
                unique_values = sorted(column_unique_dict[i])
                num_bytes = get_bigram_size(len(unique_values))

                for j, value in _enumerate_for_compression(unique_values):
                    #column_compression_dicts[i]["map"][value] = int2ba(j, length = length).to01()
                    column_compression_dicts[i]["map"][value] = j.to_bytes(length = num_bytes, byteorder = "big")

                column_sizes_dict[i] = num_bytes
            else:
                bigrams = sorted(column_bigrams_dict[i])
                num_bytes = get_bigram_size(len(bigrams))

                for j, bigram in _enumerate_for_compression(bigrams):
                    #column_compression_dicts[i]["map"][bigram] = int2ba(j, length = length).to01()
                    column_compression_dicts[i]["map"][bigram] = j.to_bytes(length = num_bytes, byteorder = "big")

                #TODO: Does this work? If not, do we need to iterate the file again?
                column_sizes_dict[i] = column_max_length_dict[i] * num_bytes
                # for unique_value in unique_values:
                #     compressed_length = len(compress_using_2_grams(unique_value, column_compression_dicts[i]["map"]))
                #     column_sizes_dict[i] = max(column_sizes_dict[i], compressed_length)

    return column_sizes_dict, column_types_dict, column_compression_dicts, num_rows

def _get_line_length(delimited_file_path, tmp_dir_path_chunks, delimiter, comment_prefix, compression_type, column_sizes, compression_dicts, num_rows, num_processes, num_rows_per_write, verbose):
    if num_processes == 1:
        line_length = _write_rows_chunk(delimited_file_path, tmp_dir_path_chunks, delimiter, comment_prefix, compression_type, column_sizes, compression_dicts, 0, 0, num_rows, num_rows_per_write, verbose)
    else:
        row_chunk_indices = _generate_chunk_ranges(num_rows, math.ceil(num_rows / num_processes) + 1)

        # Find the line length.
        max_line_sizes = Parallel(n_jobs=num_processes)(delayed(_write_rows_chunk)(delimited_file_path, tmp_dir_path_chunks, delimiter, comment_prefix, compression_type, column_sizes, compression_dicts, i, row_chunk[0], row_chunk[1], num_rows_per_write, verbose) for i, row_chunk in enumerate(row_chunk_indices))
        line_length = max(max_line_sizes)

    return line_length

def _write_rows_chunk(delimited_file_path, tmp_dir_path, delimiter, comment_prefix, compression_type, column_sizes, compression_dicts, chunk_number, start_index, end_index, num_rows_per_write, verbose):
    max_line_size = 0

    if compression_type == "zstd":
        compressor = zstandard.ZstdCompressor(level = 0)

    # Write the data to output file. Ignore the header line.
    with get_delimited_file_handle(delimited_file_path) as in_file:
        _exclude_comments_and_header(in_file, comment_prefix)

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
                        if compression_dicts[i]["compression_type"] == b"c":
                            compressed_value = compression_dicts[i]["map"][line_items[i]]
                        else:
                            compressed_value = compress_using_2_grams(line_items[i], compression_dicts[i]["map"])

                        out_items.append(format_string_as_fixed_width(compressed_value, size))
                else:
                    for i, size in enumerate(column_sizes):
                        out_items.append(format_string_as_fixed_width(line_items[i], size))

                out_line = b"".join(out_items)

                if compression_type == "zstd":
                    out_line = compressor.compress(out_line)

                line_size = len(out_line)
                max_line_size = max([max_line_size, line_size])

                out_lines.append(out_line)

                if len(out_lines) % num_rows_per_write == 0:
                    print_message(f"Processed chunk of {delimited_file_path} at line {line_index} (start_index = {start_index}, end_index = {end_index})", verbose)
                    chunk_file.write(b"".join(out_lines))
                    out_lines = []

            if len(out_lines) > 0:
                chunk_file.write(b"".join(out_lines))

    return max_line_size

def _exclude_comments_and_header(in_file, comment_prefix):
    # Ignore the header because we don't need column names here. Also ignore commented lines.
    if comment_prefix:
        for line in in_file:
            if not line.startswith(comment_prefix):
                break
    else:
        in_file.readline()

def _write_compression_info(tmp_dir_path_outputs, compression_type, column_compression_dicts, column_index_name_dict):
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
                    decompression_dict[convert_bytes_to_int(compressed_value)] = value

                column_decompression_dicts[column_name]["map"] = decompression_dict

            cmpr_file.write(serialize(column_decompression_dicts))
        else:
            cmpr_file.write(b"z")

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

    if types_dict[b"s"] > 0:
        return b"s"
    elif types_dict[b"f"] > 0:
        return b"f"

    return b"i"

# def _infer_type_for_column(types_dict):
#     if len(types_dict) == 0:
#         return None
#
#     if len(types_dict[b"s"]) > 0:
#         return b"s"
#     elif len(types_dict[b"f"]) > 0:
#         return b"f"
#
#     return b"i"

def _find_unique_bigrams(value):
    grams = set()

    for start_i in range(0, len(value), 2):
        end_i = (start_i + 2)
        grams.add(value[start_i:end_i])

    return grams

# We skip the space character because it causes a problem when we parse from a file.
def _enumerate_for_compression(values):
    ints = []
    capacity = len(values)
    length = get_bigram_size(capacity)

    i = 0
    while len(ints) < capacity:
        if b' ' not in i.to_bytes(length = length, byteorder = "big"):
            ints.append(i)

        i += 1

    for i in ints:
        yield i, values.pop(0)

def build_indexes(f4_file_path, index_columns, tmp_dir_path, verbose=False):
    if isinstance(index_columns, str):
        _build_one_column_index(f4_file_path, index_columns, tmp_dir_path, verbose)
    elif isinstance(index_columns, list):
        for index_column in index_columns:
            if isinstance(index_column, list):
                if len(index_column) != 2:
                    raise Exception("If you pass a list as an index_column, it must have exactly two elements.")

                _build_two_column_index(f4_file_path, index_column[0], index_column[1], tmp_dir_path, verbose)
            else:
                if not isinstance(index_column, str):
                    raise Exception("When specifying an index column name, it must be a string.")

                _build_one_column_index(f4_file_path, index_column, tmp_dir_path, verbose, do_nothing)
    else:
        raise Exception("When specifying index_columns, it must either be a string or a list.")

# This function is specifically for the EndsWithFilter.
def build_endswith_index(f4_file_path, index_column, tmp_dir_path, verbose=False):
    _build_one_column_index(f4_file_path, index_column, tmp_dir_path, verbose, reverse_string)

def _build_one_column_index(f4_file_path, index_column, tmp_dir_path, verbose, custom_index_function):
    tmp_dir_path = fix_dir_path_ending(tmp_dir_path)
    tmp_dir_path_data = f"{tmp_dir_path}data/"
    tmp_dir_path_other = f"{tmp_dir_path}other/"
    os.makedirs(tmp_dir_path_data, exist_ok=True)
    os.makedirs(tmp_dir_path_other, exist_ok=True)

    # TODO: Add logic to verify that index_column is valid. But where?
    print_message(f"Saving index for {index_column}.", verbose)
    index_column_encoded = index_column.encode()

    with Parser(f4_file_path) as parser:
        print_message(f"Getting column meta information for {index_column} index for {f4_file_path}.", verbose)
        select_columns, column_type_dict, column_coords_dict, decompression_type, decompressor, bigram_size_dict = parser._get_column_meta(set([index_column_encoded]), [])

        line_length = parser._get_stat("ll")
        index_column_type = column_type_dict[index_column_encoded]
        coords = column_coords_dict[index_column_encoded]
        values_positions = []

        decompressor = get_decompressor(decompression_type, decompressor)
        parse_function = parser._get_parse_row_value_function(decompression_type)

        print_message(f"Parsing values and positions for {index_column} index for {f4_file_path}.", verbose)
        for row_index in range(parser.get_num_rows()):
            value = parse_function(row_index, coords, line_length, decompression_type=decompression_type, decompressor=decompressor, bigram_size_dict=bigram_size_dict, column_name=index_column_encoded)
            values_positions.append([value, row_index])

        print_message(f"Building index file for {index_column} index for {f4_file_path}.", verbose)
        _customize_index_values_positions(values_positions, [index_column_type], sort_first_column, custom_index_function)
        _write_index_files(values_positions, tmp_dir_path_data, tmp_dir_path_other)

    index_file_path = get_index_file_path(parser.data_file_path, index_column, custom_index_function)
    combine_into_single_file(tmp_dir_path_data, tmp_dir_path_other, index_file_path)

    print_message(f"Done building index file for {index_column} index for {f4_file_path}.", verbose)

# TODO: Combine this function with the above one and make it generic enough to handle indexes with more columns.
def _build_two_column_index(f4_file_path, index_column_1, index_column_2, tmp_dir_path, verbose):
    tmp_dir_path = fix_dir_path_ending(tmp_dir_path)
    tmp_dir_path_data = f"{tmp_dir_path}data/"
    tmp_dir_path_other = f"{tmp_dir_path}other/"
    os.makedirs(tmp_dir_path_data, exist_ok=True)
    os.makedirs(tmp_dir_path_other, exist_ok=True)

    if not isinstance(index_column_1, str) or not isinstance(index_column_2, str):
        raise Exception("When specifying an index column name, it must be a string.")

    print_message(f"Saving index for {index_column_1} and {index_column_2}.", verbose)

    index_name = "____".join([index_column_1, index_column_2])
    index_column_1_encoded = index_column_1.encode()
    index_column_2_encoded = index_column_2.encode()

    with Parser(f4_file_path) as parser:
        print_message(f"Getting column meta information for {index_name} index.", verbose)
        select_columns, column_type_dict, column_coords_dict, decompression_type, decompressor, bigram_size_dict = parser._get_column_meta(set([index_column_1_encoded, index_column_2_encoded]), [])
        # TODO: Add logic to verify that index_column_1 and index_column_2 are valid.

        line_length = parser._get_stat("ll")
        index_column_1_type = column_type_dict[index_column_1_encoded]
        index_column_2_type = column_type_dict[index_column_2_encoded]
        coords_1 = column_coords_dict[index_column_1_encoded]
        coords_2 = column_coords_dict[index_column_2_encoded]

        decompressor = get_decompressor(decompression_type, decompressor)
        parse_function = parser._get_parse_row_value_function(decompression_type)

        values_positions = []
        print_message(f"Parsing values and positions for {index_name} index.", verbose)
        for row_index in range(parser.get_num_rows()):
            value_1 = parse_function(row_index, coords_1, line_length, decompression_type=decompression_type, decompressor=decompressor, bigram_size_dict=bigram_size_dict, column_name=index_column_1_encoded)
            value_2 = parse_function(row_index, coords_2, line_length, decompression_type=decompression_type, decompressor=decompressor, bigram_size_dict=bigram_size_dict, column_name=index_column_2_encoded)

            values_positions.append([value_1, value_2, row_index])

        print_message(f"Building index file for {index_name}.", verbose)
        _customize_index_values_positions(values_positions, [index_column_1_type, index_column_2_type], sort_first_two_columns, do_nothing)
        _write_index_files(values_positions, tmp_dir_path_data, tmp_dir_path_other)

    index_file_path = get_index_file_path(parser.data_file_path, index_name)
    combine_into_single_file(tmp_dir_path_data, tmp_dir_path_other, index_file_path)

    print_message(f"Done building two-column index file for {index_name}.", verbose)

def _customize_index_values_positions(values_positions, column_types, sort_function, custom_index_function):
    # Iterate through each "column" except the last one (which has row_indices) and convert the data.
    for i in range(len(column_types)):
        conversion_function = get_conversion_function(column_types[i])

        # Iterate through each "row" in the data.
        for j in range(len(values_positions)):
            values_positions[j][i] = conversion_function(values_positions[j][i])
            values_positions[j][i] = custom_index_function(values_positions[j][i])

    # Sort the rows.
    sort_function(values_positions)

def _write_index_files(values_positions, tmp_dir_path_data, tmp_dir_path_prefix_other):
    column_dict = {}
    for i in range(len(values_positions[0])):
        column_dict[i] = [x[i] if isinstance(x[i], bytes) else str(x[i]).encode() for x in values_positions]

    max_lengths = []
    for i in range(len(values_positions[0])):
        max_lengths.append(get_max_string_length(column_dict[i]))

    for i in range(len(values_positions[0])):
        column_dict[i] = format_column_items(column_dict[i], max_lengths[i])

    rows = []
    for row_num in range(len(column_dict[0])):
        row_value = b""

        for col_num in sorted(column_dict.keys()):
            row_value += column_dict[col_num][row_num]

        rows.append(row_value)

    column_coords_string, rows_max_length = build_string_map(rows)

    if tmp_dir_path_data:
        write_str_to_file(f"{tmp_dir_path_data}0", column_coords_string)
    else:
        write_str_to_file(f"{tmp_dir_path_prefix_other}data", column_coords_string)

    column_start_coords = get_column_start_coords(max_lengths)
    column_coords_string, max_column_coord_length = build_string_map(column_start_coords)
    write_str_to_file(f"{tmp_dir_path_prefix_other}cc", column_coords_string)
    write_str_to_file(f"{tmp_dir_path_prefix_other}mccl", str(max_column_coord_length).encode())

    # Find and write the line length.
    write_str_to_file(f"{tmp_dir_path_prefix_other}ll", str(rows_max_length).encode())