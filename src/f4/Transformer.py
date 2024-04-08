from .Builder import *
from .Parser import *

def transpose(f4_src_file_path, f4_dest_file_path, src_column_for_names, num_parallel=1, tmp_dir_path=None, verbose=False):
    if src_column_for_names is None or not isinstance(src_column_for_names, str) or len(src_column_for_names) == 0:
        raise Exception(f"The value specified for src_column_for_names was invalid.")

    print_message(f"Transposing {f4_src_file_path} to {f4_dest_file_path}.", verbose)

    tmp_dir_path2 = prepare_tmp_dir(tmp_dir_path)
    tsv_file_path = f"{tmp_dir_path2}data.tsv.zstd"

    #######################################################
    # Write the new column names to the temporary TSV file.
    #######################################################
    with initialize(f4_src_file_path) as src_file_data:
        num_rows = src_file_data.cache_dict["num_rows"]
        num_cols = src_file_data.cache_dict["num_cols"]

        with open_temp_file_to_compress(tsv_file_path) as tsv_file:
            src_column_for_names_index = get_column_index_from_name(src_file_data, src_column_for_names.encode())
            src_column_for_names_coords = parse_data_coord(src_file_data, "", src_column_for_names_index)
            parse_row_value_function = get_parse_row_value_function(src_file_data)

            tsv_file.write((f"{src_column_for_names}\t").encode())

            for row_index in range(num_rows - 1):
                src_column_for_names_value = parse_row_value_function(src_file_data, "", row_index, src_column_for_names_coords)
                tsv_file.write((src_column_for_names_value + b"\t"))

            src_column_for_names_value = parse_row_value_function(src_file_data, "", num_rows - 1, src_column_for_names_coords)
            tsv_file.write((src_column_for_names_value + b"\n"))

    ###########################################################
    # Read one column at a time and save to temporary TSV file.
    ###########################################################

    #TODO:
    num_parallel = 2
    if num_parallel == 1:
        transpose_chunk(f4_src_file_path, num_rows, src_column_for_names_index, range(num_cols), tsv_file_path, "ab", verbose)
    else:
        global joblib
        joblib = __import__('joblib', globals(), locals())

        # Transpose the data in chunks.
        joblib.Parallel(n_jobs=num_parallel)(joblib.delayed(transpose_chunk)(f4_src_file_path, num_rows, src_column_for_names_index, col_chunk_range, f"{tmp_dir_path2}chunk_{chunk_number}.tsv", "wb", verbose) for chunk_number, col_chunk_range in enumerate(generate_range_chunks(num_cols, ceil(num_cols / num_parallel))))

        # Concatenate the chunks to the header line.
        with open_temp_file_to_compress(tsv_file_path, "ab") as tsv_file:
            for chunk_number, col_chunk_range in enumerate(generate_range_chunks(num_cols, ceil(num_cols / num_parallel))):
                for line in read_compressed_file_line_by_line(f"{tmp_dir_path2}chunk_{chunk_number}.tsv"):
                    tsv_file.write(line + b"\n")

                remove(f"{tmp_dir_path2}chunk_{chunk_number}.tsv")

    print_message(f"Converting temp file at {tsv_file_path} to {f4_dest_file_path}.", verbose)
    convert_delimited_file(tsv_file_path, f4_dest_file_path, comment_prefix=None, compression_type=src_file_data.decompression_type, num_parallel=num_parallel, verbose=verbose)
    remove(tsv_file_path)


    # if num_parallel == 1:
    # chunk_file_path = transpose_lines_to_temp(f4_src_file_path, tmp_dir_path, 0, num_rows, column_names, column_coords, bigram_size_dict, verbose)

    # print_message(f"Extracting transposed values from {chunk_file_path} for {f4_src_file_path}.", verbose)
    # with gzip.open(tmp_tsv_file_path, "w", compresslevel=1) as tmp_tsv_file:
    #     with open(chunk_file_path, "rb") as chunk_file:
    #         for column_index, column_name in enumerate(column_names):
    #             tmp_tsv_file.write(column_name)
    #
    #             column_size = column_coords[column_index][1] - column_coords[column_index][0]
    #             for row_index in range(num_rows):
    #                 tmp_tsv_file.write(b"\t" + chunk_file.read(column_size))
    #
    #             tmp_tsv_file.write(b"\n")
    #
    #     remove(chunk_file_path)
    # else:
    #     row_index_chunks = list(split_integer_list_into_chunks(list(range(num_rows)), num_parallel))
    #
    #     chunk_file_paths = joblib.Parallel(n_jobs=num_parallel)(
    #          joblib.delayed(transpose_lines_to_temp)(f4_src_file_path, tmp_dir_path, row_index_chunk[0], row_index_chunk[-1] + 1, column_names, column_coords, bigram_size_dict, verbose) for row_index_chunk in row_index_chunks)
    #
    #     chunk_file_handles = {}
    #     for row_index_chunk in row_index_chunks:
    #         chunk_file_path = f"{tmp_dir_path}{row_index_chunk[0]}"
    #         chunk_file_handles[chunk_file_path] = open(chunk_file_path, "rb")
    #
    #     try:
    #         with gzip.open(tmp_tsv_file_path, "w", compresslevel=1) as tmp_tsv_file:
    #             for column_index, column_name in enumerate(column_names):
    #                 tmp_tsv_file.write(column_name)
    #
    #                 column_size = column_coords[column_index][1] - column_coords[column_index][0]
    #
    #                 for row_index_chunk in row_index_chunks:
    #                     chunk_file_path = f"{tmp_dir_path}{row_index_chunk[0]}"
    #
    #                     for row_index in range(row_index_chunk[0], row_index_chunk[-1] + 1):
    #                         tmp_tsv_file.write(b"\t" + chunk_file_handles[chunk_file_path].read(column_size))
    #
    #                 tmp_tsv_file.write(b"\n")
    #     finally:
    #         for chunk_file_path in chunk_file_handles:
    #             chunk_file_handles[chunk_file_path].close()
    #             remove(chunk_file_path)

    # print_message(f"Converting temp file at {tmp_tsv_file_path} to {f4_dest_file_path}.", verbose)
    # convert_delimited_file(tmp_tsv_file_path, f4_dest_file_path, compression_type=src_file_data.decompression_type, num_parallel=num_parallel, verbose=verbose)
    # remove(tmp_tsv_file_path)

def transpose_chunk(f4_src_file_path, num_rows, src_column_for_names_index, col_chunk_range, tsv_chunk_file_path, write_mode, verbose):
    with initialize(f4_src_file_path) as src_file_data:
        with open_temp_file_to_compress(tsv_chunk_file_path, write_mode) as tsv_file:
            cn_current = src_file_data.file_map_dict["cn"][0]
            cn_end = src_file_data.file_map_dict["cn"][1]

            # Advance to the first column name for this chunk range.
            for column_index in range(col_chunk_range[0]):
                while src_file_data.file_handle[cn_current:(cn_current + 1)] != b"\n":
                    cn_current += 1
                cn_current += 1

            for column_index in col_chunk_range:
                if len(col_chunk_range) < 100 or column_index % 100 == 0:
                    print_message(f"Transposing {f4_src_file_path} to temporary file {tsv_chunk_file_path} for column {column_index}.", verbose)

                # Parse the column name, one character at a time.
                column_name = b""
                while cn_current < cn_end and (next_char := src_file_data.file_handle[cn_current:(cn_current + 1)]) != b"\n":
                    column_name += next_char
                    cn_current += 1
                cn_current += 1

                if column_index == src_column_for_names_index:
                    continue

                column_coords = parse_data_coord(src_file_data, "", column_index)
                parse_row_value_function = get_parse_row_value_function(src_file_data)

                values = [column_name]
                for row_index in range(num_rows):
                    values.append(parse_row_value_function(src_file_data, "", row_index, column_coords))

                tsv_file.write(b"\t".join(values) + b"\n")

# def transpose_lines_to_temp(data_file_path, tmp_dir_path, start_row_index, end_row_index, column_names, column_coords, verbose):
#     with initialize(data_file_path) as file_data:
#         tmp_file_path = f"{tmp_dir_path}{start_row_index}"
#         print_message(f"Transposing lines to {tmp_file_path} for {data_file_path} - start row {start_row_index}, end row {end_row_index}.", verbose)
#
#         with open(tmp_file_path, "wb+") as tmp_file:
#             num_rows = end_row_index - start_row_index
#
#             for column_index in range(len(column_names)):
#                 # Create placeholder space for column values in temp file
#                 column_coord = column_coords[column_index]
#                 tmp_file.write(b" " * (column_coord[1] - column_coord[0]) * num_rows)
#
#             tmp_file.flush()
#
#             with mmap(tmp_file.fileno(), 0, prot=PROT_WRITE) as mm_handle:
#                 # Iterate through each row and save the values in the correct locations in the temp file
#                 parse_function = get_parse_row_values_function(file_data)
#
#                 # Find the start position of each row
#                 out_line_positions = [0]
#                 for column_index in range(1, len(column_names)):
#                     previous_column_size = column_coords[column_index - 1][1] - column_coords[column_index - 1][0]
#                     this_line_position = out_line_positions[-1] + previous_column_size * num_rows
#                     out_line_positions.append(this_line_position)
#
#                 # Iterate through each row and store the values in the respective column
#                 for row_index in range(start_row_index, end_row_index):
#                     row_values = parse_function(file_data, row_index, column_coords, bigram_size_dict=bigram_size_dict)
#
#                     for column_index, value in enumerate(row_values):
#                         out_position = out_line_positions[column_index]
#                         column_size = column_coords[column_index][1] - column_coords[column_index][0]
#                         value = format_string_as_fixed_width(value, column_size)
#                         mm_handle[out_position:(out_position + len(value))] = value
#                         out_line_positions[column_index] += len(value)
#
#         return tmp_file_path
