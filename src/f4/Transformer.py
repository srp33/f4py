from .Builder import *
from .Parser import *

def transpose(f4_src_file_path, f4_dest_file_path, src_column_for_names, index_columns=[], num_parallel=1, tmp_dir_path=None, verbose=False):
    if src_column_for_names is None or not isinstance(src_column_for_names, str) or len(src_column_for_names) == 0:
        raise Exception(f"The value specified for src_column_for_names was invalid.")

    print_message(f"Finding max column width when transposing {f4_src_file_path} to {f4_dest_file_path}.", verbose)

    with initialize(f4_src_file_path) as src_file_data:
        num_cols = src_file_data.cache_dict["num_cols"]
        max_column_width = get_max_column_width(src_file_data)

    tmp_dir_path2 = prepare_tmp_dir(tmp_dir_path)
    tmp_fw_file_path = f"{tmp_dir_path2}fw"

    if num_parallel == 1:
        transpose_column_chunk(f4_src_file_path, src_column_for_names, 0, range(num_cols), max_column_width, tmp_fw_file_path, verbose)
    else:
        max_cols_per_chunk = 100001

        global joblib
        joblib = __import__('joblib', globals(), locals())

        # Transpose the data in chunks.
        joblib.Parallel(n_jobs=num_parallel)(joblib.delayed(transpose_column_chunk)(
            f4_src_file_path,
            src_column_for_names,
            chunk_number,
            chunk_range,
            max_column_width,
            f"{tmp_dir_path2}chunk_{chunk_number}.fw",
            verbose)
                for chunk_number, chunk_range in enumerate(generate_column_ranges(max_cols_per_chunk, num_cols, num_parallel))
        )

        print_message(f"Assembling chunks when transposing {f4_src_file_path} to {f4_dest_file_path}.", verbose)

        # Put the chunks together.
        with open(tmp_fw_file_path, "wb") as tmp_fw_file:
            for chunk_number, _ignore in enumerate(generate_column_ranges(max_cols_per_chunk, num_cols, num_parallel)):
                chunk_file_path = f"{tmp_dir_path2}chunk_{chunk_number}.fw"

                with open(chunk_file_path, "rb") as chunk_file:
                    for line in chunk_file:
                        tmp_fw_file.write(line)

                remove(chunk_file_path)

    print_message(f"Converting temp file at {tmp_fw_file_path} when transposing {f4_src_file_path} to {f4_dest_file_path}.", verbose)
    convert_delimited_file(tmp_fw_file_path, f4_dest_file_path, comment_prefix=None, compression_type=src_file_data.decompression_type, index_columns=index_columns, num_parallel=num_parallel, verbose=verbose)

    remove(tmp_fw_file_path)
    rmtree(tmp_dir_path2)

def generate_column_ranges(max_cols_per_chunk, num_cols, num_parallel):
    if num_cols > max_cols_per_chunk:
        column_ranges = generate_range_chunks(num_cols, max_cols_per_chunk)
    else:
        column_ranges = generate_range_chunks(num_cols, ceil(num_cols / num_parallel))

    return column_ranges

def get_max_column_width(src_file_data):
    # Find the maximum column size across all columns, including column names.
    max_column_width = 0
    cn_current, cn_end = advance_to_column_names(src_file_data, 0)

    for column_index in range(src_file_data.cache_dict["num_cols"]):
        cn_current, column_name = get_next_column_name(src_file_data, cn_current, cn_end)
        max_column_width = max(max_column_width, len(column_name))

        # Find the max width of values.
        column_coords = parse_data_coord(src_file_data, "", column_index)
        column_size = column_coords[1] - column_coords[0]
        max_column_width = max(max_column_width, column_size)

    return max_column_width

def transpose_column_chunk(f4_src_file_path, src_column_for_names, chunk_number, column_range, max_column_width, tmp_fw_file_path, verbose):
    with initialize(f4_src_file_path) as src_file_data:
        print_message(f"Parsing column coordinates for chunk {chunk_number} when transposing {f4_src_file_path}.", verbose)
        # Get basic meta information.
        num_rows = src_file_data.cache_dict["num_rows"]
        src_column_for_names_index = get_column_index_from_name(src_file_data, src_column_for_names.encode())
        src_column_for_names_coords = parse_data_coord(src_file_data, "", src_column_for_names_index)
        parse_row_value_function = get_parse_row_value_function(src_file_data)
        parse_row_values_function = get_parse_row_values_function(src_file_data)

        all_column_coords = parse_data_coords(src_file_data, "", column_range)

        print_message(f"Filling temp file {tmp_fw_file_path} with empty space for chunk {chunk_number} when transposing {f4_src_file_path}.", verbose)
        # We can't compress this file because we have to navigate around it later.
        with open(tmp_fw_file_path, "wb") as fw_file:
            if chunk_number == 0:
                # Write the value to the top-left cell.
                fw_file.write(format_string_as_fixed_width(src_column_for_names.encode(), max_column_width) + b"\t")

                # Write the transposed column names (except the last one).
                for row_index in range(num_rows - 1):
                    src_column_for_names_value = parse_row_value_function(src_file_data, "", row_index, src_column_for_names_coords)
                    fw_file.write(format_string_as_fixed_width(src_column_for_names_value, max_column_width) + b"\t")

                # Write the last transposed column name.
                src_column_for_names_value = parse_row_value_function(src_file_data, "", num_rows - 1, src_column_for_names_coords)
                fw_file.write(format_string_as_fixed_width(src_column_for_names_value, max_column_width) + b"\n")

            # Populate the rest of the file with empty space, which will later be filled in with values.
            for column_index in column_range:
                if column_index == src_column_for_names_index:
                    continue

                for row_index in range(num_rows):
                    fw_file.write(b" " * max_column_width + b"\t")

                fw_file.write(b" " * max_column_width + b"\n")

        new_row_width = (num_rows + 1) * (max_column_width + 1)

        print_message(f"Saving new row names to temp file {tmp_fw_file_path} for chunk {chunk_number} when transposing {f4_src_file_path}.", verbose)
        cn_current, cn_end = advance_to_column_names(src_file_data, column_range[0] - 1)
        # Skip the first column name because we already wrote it in the top-left corner.
        cn_current, column_name = get_next_column_name(src_file_data, cn_current, cn_end)

        # We can't compress this file because we have to navigate within it.
        with open(tmp_fw_file_path, "rb+") as fw_file:
            # FYI: Using memory mapping is an option, but it can consume a lot of memory
            #      in this context.
            # with mmap(fw_file.fileno(), 0, prot=PROT_WRITE) as fw_handle:
            # Save new row names.
            for chunk_column_index, overall_column_index in enumerate(column_range):
                if overall_column_index == src_column_for_names_index:
                    continue

                cn_current, column_name = get_next_column_name(src_file_data, cn_current, cn_end)
                row_start = chunk_column_index * new_row_width
                # fw_handle[row_start:(row_start + len(column_name))] = column_name
                fw_file.seek(row_start)
                fw_file.write(column_name)

            # Save values in transposed orientation.
            for row_index in range(num_rows):
                if row_index < 10 or (row_index < 100 and row_index % 10 == 0) or row_index % 100 == 0:
                    print_message(f"Saving data to temp file {tmp_fw_file_path} for chunk {chunk_number} and original row {row_index}when transposing {f4_src_file_path}.", verbose)

                #FYI: Retrieving all values in a row is much faster than one at a time.
                values = parse_row_values_function(src_file_data, "", row_index, all_column_coords)

                for chunk_column_index, overall_column_index in enumerate(column_range):
                    if overall_column_index == src_column_for_names_index:
                        continue

                    value = values[chunk_column_index]

                    row_start = chunk_column_index * new_row_width
                    value_start = row_start + (row_index + 1) * (max_column_width + 1)

                    # fw_handle[value_start:(value_start + len(value))] = value
                    fw_file.seek(value_start)
                    fw_file.write(value)

def advance_to_column_names(src_file_data, first_col_index):
    cn_current = src_file_data.file_map_dict["cn"][0]
    cn_end = src_file_data.file_map_dict["cn"][1]

    # Advance to the first column name for this chunk range.
    for column_index in range(first_col_index):
        while src_file_data.file_handle[cn_current:(cn_current + 1)] != b"\n":
            cn_current += 1
        cn_current += 1

    return cn_current, cn_end

def get_next_column_name(src_file_data, cn_current, cn_end):
    # Parse the column name, one character at a time.
    column_name = b""
    while cn_current < cn_end and (next_char := src_file_data.file_handle[cn_current:(cn_current + 1)]) != b"\n":
        column_name += next_char
        cn_current += 1

    return cn_current + 1, column_name

#TODO: This function is not yet designed for files with 1000000+ columns.
def inner_join(f4_left_src_file_path, f4_right_src_file_path, join_column, f4_dest_file_path, index_columns=[], num_parallel=1, tmp_dir_path=None, verbose=False):
    #TODO: Add error checking to make sure join_column is present in left and right.
    print_message(f"Inner joining {f4_left_src_file_path} and {f4_right_src_file_path} based on the {join_column} column, saving to {f4_dest_file_path}.", verbose)

    join_column = join_column.encode()

    if tmp_dir_path:
        makedirs(tmp_dir_path, exist_ok=True)
    else:
        tmp_dir_path = mkdtemp()

    tmp_dir_path = fix_dir_path_ending(tmp_dir_path)
    tmp_tsv_file_path = f"{tmp_dir_path}tmp.tsv.zstd"

    with initialize(f4_left_src_file_path) as left_file_data:
        with initialize(f4_right_src_file_path) as right_file_data:
            # Determine which functions are suitable for parsing the join column info.
            left_parse_row_value_function = get_parse_row_value_function(left_file_data)
            right_parse_row_value_function = get_parse_row_value_function(right_file_data)

            # Find the index of the join column in each file.
            left_join_column_index = get_column_index_from_name(left_file_data, join_column)
            right_join_column_index = get_column_index_from_name(right_file_data, join_column)

            # Find the coordinates of the join column in each file.
            left_join_column_coord = parse_data_coord(left_file_data, "", left_join_column_index)
            right_join_column_coord = parse_data_coord(right_file_data, "", right_join_column_index)

            # Determine which column values overlap for the join column between the two files.
            left_join_column_values = []
            for row_index in range(left_file_data.cache_dict["num_rows"]):
                value = left_parse_row_value_function(left_file_data, "", row_index, left_join_column_coord)
                left_join_column_values.append(value)

            right_join_column_values = []
            for row_index in range(right_file_data.cache_dict["num_rows"]):
                value = right_parse_row_value_function(right_file_data, "", row_index, right_join_column_coord)
                right_join_column_values.append(value)

            common_join_values = set(left_join_column_values) & set(right_join_column_values)

            # Get column names from the left file.
            left_cn_current, left_cn_end = advance_to_column_names(left_file_data, -1)
            left_columns = []
            for column_index in range(left_file_data.cache_dict["num_cols"]):
                left_cn_current, column_name = get_next_column_name(left_file_data, left_cn_current, left_cn_end)
                left_columns.append(column_name)

            # Get column names from the right file (excluding the join column).
            right_cn_current, right_cn_end = advance_to_column_names(right_file_data, -1)
            right_columns = []
            for column_index in range(right_file_data.cache_dict["num_cols"]):
                right_cn_current, column_name = get_next_column_name(right_file_data, right_cn_current, right_cn_end)
                if column_name != join_column:
                    right_columns.append(column_name)

            # Parse the column coordinates for all columns that will be saved.
            left_column_coords = [parse_data_coord(left_file_data, "", get_column_index_from_name(left_file_data, name)) for name in left_columns]
            right_column_coords = [parse_data_coord(right_file_data, "", get_column_index_from_name(right_file_data, name)) for name in right_columns]

            # Rename columns that are duplicated between left (x) and right (y).
            for left_i, column_name in enumerate(left_columns):
                if column_name in right_columns:
                    right_i = right_columns.index(column_name)
                    left_columns[left_i] = (f"{column_name.decode()}.x").encode()
                    right_columns[right_i] = (f"{right_columns[right_i].decode()}.y").encode()

            # Create a cache of the right row index for each join value. This speeds up a later step.
            right_index_dict = {}
            for i, value in enumerate(right_join_column_values):
                if value in common_join_values:
                    right_index_dict.setdefault(value, []).append(i)

            # Determine which functions are suitable for parsing all columns.
            left_parse_row_values_function = get_parse_row_values_function(left_file_data)
            right_parse_row_values_function = get_parse_row_values_function(right_file_data)

            #TODO: Parallelize this?
            with open_temp_file_to_compress(tmp_tsv_file_path) as tmp_tsv_file:
                tmp_tsv_file.write(b"\t".join(left_columns + right_columns) + b"\n")

                for left_row_index, left_value in enumerate(left_join_column_values):
                    if left_value in common_join_values:
                        for right_row_index in right_index_dict[left_value]:
                            left_save_values = left_parse_row_values_function(left_file_data, "", left_row_index, left_column_coords)
                            right_save_values = right_parse_row_values_function(right_file_data, "", right_row_index, right_column_coords)

                            tmp_tsv_file.write(b"\t".join(left_save_values + right_save_values) + b"\n")

            # TODO: Expand this logic for all compression types. Make sure to document it for users.
            compression_type = None
            if left_file_data.decompression_type == "zstd" or right_file_data.decompression_type == "zstd":
                compression_type = "zstd"

            print_message(f"Converting temp file at {tmp_tsv_file_path} to {f4_dest_file_path}.", verbose)
            convert_delimited_file(tmp_tsv_file_path, f4_dest_file_path, compression_type=compression_type, index_columns=index_columns, num_parallel=num_parallel, comment_prefix=None, verbose=verbose)

    remove(tmp_tsv_file_path)
    rmtree(tmp_dir_path)
