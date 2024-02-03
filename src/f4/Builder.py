from .Utilities import *
global joblib
joblib = __import__('joblib', globals(), locals())

#####################################################
# Public function(s)
#####################################################

def convert_delimited_file(delimited_file_path, f4_file_path, index_columns=[], delimiter="\t", comment_prefix="#", compression_type=None, num_parallel=1, tmp_dir_path=None, verbose=False):
    print_message(f"Converting from {delimited_file_path} to {f4_file_path}.", verbose)

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

    # Set constants
    file_read_chunk_size = 100000
    out_items_chunk_size = 10000

    num_cols, max_column_name_length = preview_column_names(delimited_file_path, f4_file_path, comment_prefix, delimiter, file_read_chunk_size, verbose)

    # Determine the number of columns per chunk.
    num_cols_per_chunk = ceil(num_cols / num_parallel)

    # Separate the column indices into chunks.
    column_chunk_indices = generate_column_chunk_ranges(num_cols, num_cols_per_chunk, num_parallel)

    # Create temp directory.
    tmp_dir_path2 = prepare_tmp_dir(tmp_dir_path)

    # Parse column info into a database for each chunk.
    joblib.Parallel(n_jobs=num_parallel)(joblib.delayed(parse_column_info)(delimited_file_path, f4_file_path, comment_prefix, delimiter, file_read_chunk_size, chunk_number, chunk_indices[0], chunk_indices[1], tmp_dir_path2, out_items_chunk_size, verbose) for chunk_number, chunk_indices in enumerate(column_chunk_indices))

    # Save and format data to a temp file for each column chunk.
    joblib.Parallel(n_jobs=num_parallel)(joblib.delayed(save_formatted_data)(delimited_file_path, f4_file_path, comment_prefix, delimiter, file_read_chunk_size, chunk_number, chunk_indices[0], chunk_indices[1], tmp_dir_path2, out_items_chunk_size, verbose) for chunk_number, chunk_indices in enumerate(column_chunk_indices))

    # Combine column databases across the chunks.
    combine_column_databases(delimited_file_path, f4_file_path, column_chunk_indices, tmp_dir_path2, verbose)

    # Create meta files for all columns.
    save_column_name_info(delimited_file_path, f4_file_path, out_items_chunk_size, tmp_dir_path2, verbose)
    save_column_types(delimited_file_path, f4_file_path, out_items_chunk_size, tmp_dir_path2, verbose)
    save_column_coordinates(delimited_file_path, f4_file_path, out_items_chunk_size, tmp_dir_path2, verbose)

    # Merge the saved/formatted data across the column chunks.
    # Get number of rows.
    num_rows, line_length = combine_data_for_column_chunks(delimited_file_path, f4_file_path, column_chunk_indices, tmp_dir_path2, verbose)

    if num_rows == 0:
        raise Exception(f"A header row but no data rows were detected in {delimited_file_path}.")

    if index_columns:
        build_indexes(f4_file_path, tmp_dir_path2, index_columns, num_rows, line_length, num_parallel, verbose)

    remove(get_columns_database_path(tmp_dir_path2))

    #TODO: Parallelize this by row chunks.
    compress_data(tmp_dir_path2, compression_type, num_rows, line_length)

    combine_into_single_file(delimited_file_path, f4_file_path, tmp_dir_path2, file_read_chunk_size, verbose)

    print_message(f"Done converting {delimited_file_path} to {f4_file_path}.", verbose)

#TODO: Move to a different file so we don't need to import Parser?
# def transpose(f4_src_file_path, f4_dest_file_path, num_parallel=1, tmp_dir_path=None, verbose=False):
#     if num_parallel > 1:
#         global joblib
#         joblib = __import__('joblib', globals(), locals())
#
#     print_message(f"Transposing {f4_src_file_path} to {f4_dest_file_path}.", verbose)
#
#     if tmp_dir_path:
#         makedirs(tmp_dir_path, exist_ok=True)
#     else:
#         tmp_dir_path = mkdtemp()
#
#     tmp_dir_path = fix_dir_path_ending(tmp_dir_path)
#     makedirs(tmp_dir_path, exist_ok=True)
#     tmp_tsv_file_path = f"{tmp_dir_path}tmp.tsv.gz"
#
#     with initialize(f4_src_file_path) as src_file_data:
#         column_names, column_type_dict, column_coords_dict, bigram_size_dict = get_column_meta(src_file_data, set(), [])
#         column_coords = [column_coords_dict[name] for name in column_names]
#         num_rows = src_file_data.stat_dict["num_rows"]
#
#     if num_parallel == 1:
#         chunk_file_path = transpose_lines_to_temp(f4_src_file_path, tmp_dir_path, 0, num_rows, column_names, column_coords, bigram_size_dict, verbose)
#
#         print_message(f"Extracting transposed values from {chunk_file_path} for {f4_src_file_path}.", verbose)
#         with gzip.open(tmp_tsv_file_path, "w", compresslevel=1) as tmp_tsv_file:
#             with open(chunk_file_path, "rb") as chunk_file:
#                 for column_index, column_name in enumerate(column_names):
#                     tmp_tsv_file.write(column_name)
#
#                     column_size = column_coords[column_index][1] - column_coords[column_index][0]
#                     for row_index in range(num_rows):
#                         tmp_tsv_file.write(b"\t" + chunk_file.read(column_size))
#
#                     tmp_tsv_file.write(b"\n")
#
#             remove(chunk_file_path)
#     else:
#         row_index_chunks = list(split_integer_list_into_chunks(list(range(num_rows)), num_parallel))
#
#         chunk_file_paths = joblib.Parallel(n_jobs=num_parallel)(
#              joblib.delayed(transpose_lines_to_temp)(f4_src_file_path, tmp_dir_path, row_index_chunk[0], row_index_chunk[-1] + 1, column_names, column_coords, bigram_size_dict, verbose) for row_index_chunk in row_index_chunks)
#
#         chunk_file_handles = {}
#         for row_index_chunk in row_index_chunks:
#             chunk_file_path = f"{tmp_dir_path}{row_index_chunk[0]}"
#             chunk_file_handles[chunk_file_path] = open(chunk_file_path, "rb")
#
#         try:
#             with gzip.open(tmp_tsv_file_path, "w", compresslevel=1) as tmp_tsv_file:
#                 for column_index, column_name in enumerate(column_names):
#                     tmp_tsv_file.write(column_name)
#
#                     column_size = column_coords[column_index][1] - column_coords[column_index][0]
#
#                     for row_index_chunk in row_index_chunks:
#                         chunk_file_path = f"{tmp_dir_path}{row_index_chunk[0]}"
#
#                         for row_index in range(row_index_chunk[0], row_index_chunk[-1] + 1):
#                             tmp_tsv_file.write(b"\t" + chunk_file_handles[chunk_file_path].read(column_size))
#
#                     tmp_tsv_file.write(b"\n")
#         finally:
#             for chunk_file_path in chunk_file_handles:
#                 chunk_file_handles[chunk_file_path].close()
#                 remove(chunk_file_path)
#
#     print_message(f"Converting temp file at {tmp_tsv_file_path} to {f4_dest_file_path}.", verbose)
#     convert_delimited_file(tmp_tsv_file_path, f4_dest_file_path, compression_type=src_file_data.decompression_type, num_parallel=num_parallel, verbose=verbose)
#     remove(tmp_tsv_file_path)

#TODO: Move to a different file so we don't need to import Parser?
# def inner_join(f4_left_src_file_path, f4_right_src_file_path, join_column, f4_dest_file_path, num_parallel=1, tmp_dir_path=None, verbose=False):
#     print_message(f"Inner joining {f4_left_src_file_path} and {f4_right_src_file_path} based on the {join_column} column, saving to {f4_dest_file_path}.", verbose)
#
#     join_column = join_column.encode()
#
#     if tmp_dir_path:
#         makedirs(tmp_dir_path, exist_ok=True)
#     else:
#         tmp_dir_path = mkdtemp()
#
#     tmp_dir_path = fix_dir_path_ending(tmp_dir_path)
#     makedirs(tmp_dir_path, exist_ok=True)
#     tmp_tsv_file_path = tmp_dir_path + "tmp.tsv.gz"
#
#     with initialize(f4_left_src_file_path) as left_file_data:
#         with initialize(f4_right_src_file_path) as right_file_data:
#             left_column_names, left_column_type_dict, left_column_coords_dict, left_bigram_size_dict = get_column_meta(left_file_data, set(), [])
#             right_column_names, right_column_type_dict, right_column_coords_dict, right_bigram_size_dict = get_column_meta(right_file_data, set(), [])
#             #TODO: Add error checking to make sure join_column is present in left and right.
#
#             left_values = parse_values_in_column(left_file_data, join_column, left_column_coords_dict[join_column], left_bigram_size_dict)
#             right_values = parse_values_in_column(right_file_data, join_column, right_column_coords_dict[join_column], right_bigram_size_dict)
#             common_values = set(left_values) & set(right_values)
#
#             right_columns_to_save = [name for name in right_column_names if name != join_column]
#
#             right_index_dict = {}
#             for i, value in enumerate(right_values):
#                 if value in common_values:
#                     right_index_dict.setdefault(value, []).append(i)
#
#             # FYI: It would be faster to save in zstandard format. But the Python package
#             #      doesn't support reading line by line.
#             #TODO: Parallelize this?
#             with gzip.open(tmp_tsv_file_path, "w", compresslevel=1) as tmp_tsv_file:
#                 tmp_tsv_file.write(b"\t".join(left_column_names + right_columns_to_save) + b"\n")
#
#                 left_parse_function = get_parse_row_values_function(left_file_data)
#                 right_parse_function = get_parse_row_values_function(right_file_data)
#                 left_column_coords = [left_column_coords_dict[name] for name in left_column_names]
#                 right_column_coords = [right_column_coords_dict[name] for name in right_columns_to_save]
#
#                 for left_row_index, left_value in enumerate(left_values):
#                     if left_value in common_values:
#                         for right_row_index in right_index_dict[left_value]:
#                             left_save_values = left_parse_function(left_file_data, left_row_index, left_column_coords, bigram_size_dict=left_bigram_size_dict)
#                             right_save_values = right_parse_function(right_file_data, right_row_index, right_column_coords, bigram_size_dict=right_bigram_size_dict)
#
#                             tmp_tsv_file.write(b"\t".join(left_save_values + right_save_values) + b"\n")
#
#             # TODO: Expand this logic for all compression types. Make sure to document it.
#             compression_type = None
#             if left_file_data.decompression_type == "zstd" or right_file_data.decompression_type == "zstd":
#                 compression_type = "zstd"
#
#             print_message(f"Converting temp file at {tmp_tsv_file_path} to {f4_dest_file_path}.", verbose)
#             convert_delimited_file(tmp_tsv_file_path, f4_dest_file_path, compression_type=compression_type, num_parallel=num_parallel, verbose=verbose)
#
#             remove(tmp_tsv_file_path)

#####################################################
# Non-public function(s)
#####################################################

def preview_column_names(delimited_file_path, f4_file_path, comment_prefix, delimiter, file_read_chunk_size, verbose):
    print_message(f"Previewing column names when converting {delimited_file_path} to {f4_file_path}.", verbose)

    with get_delimited_file_handle(delimited_file_path) as in_file:
        skip_comments(in_file, comment_prefix=comment_prefix)

        current_index = in_file.tell()
        previous_text = b""
        current_column_index = -1
        max_column_name_length = -1

        while (newline_index := (next_text := previous_text + in_file.read(file_read_chunk_size)).find(b"\n")) == -1:
            if len(next_text) == 0:
                break

            last_delimiter_index = next_text.rfind(delimiter)

            if last_delimiter_index == -1:
                previous_text = next_text
            else:
                previous_text = next_text[(last_delimiter_index + 1):]

                for column_name in next_text[:last_delimiter_index].split(delimiter):
                    current_column_index += 1
                    if len(column_name) > max_column_name_length:
                        max_column_name_length = len(column_name)

        for column_name in next_text[:newline_index].split(delimiter):
            current_column_index += 1
            if len(column_name) > max_column_name_length:
                max_column_name_length = len(column_name)

        in_file.seek(current_index + newline_index + 1)

    num_cols = current_column_index + 1

    if num_cols == 0:
        raise Exception(f"No data was detected in {delimited_file_path}.")

    return num_cols, max_column_name_length

def get_columns_database_path(tmp_dir_path, chunk_number=-1):
    return f"{tmp_dir_path}{chunk_number}.columns.db" if chunk_number >= 0 else f"{tmp_dir_path}columns.db"

def get_sizes_dict_path(tmp_dir_path, chunk_number):
    return f"{tmp_dir_path}{chunk_number}sizes_dict"

def get_data_path(tmp_dir_path, extension, chunk_number=-1):
    return f"{tmp_dir_path}{chunk_number}.{extension}" if chunk_number >= 0 else f"{tmp_dir_path}{extension}"

def create_column_database(cursor):
    sql = '''CREATE TABLE columns (
                column_index INTEGER PRIMARY KEY,
                column_name TEXT,
                size INTEGER DEFAULT 0,
                num_i INTEGER DEFAULT 0,
                num_f INTEGER DEFAULT 0,
                num_s INTEGER DEFAULT 0,
                inferred_type TEXT DEFAULT 'i'
             )'''

    cursor.execute(sql)

def populate_database_with_column_names(delimited_file_path, comment_prefix, delimiter, file_read_chunk_size, start_column_index, end_column_index, out_items_chunk_size, cursor):
    with get_delimited_file_handle(delimited_file_path) as in_file:
        skip_comments(in_file, comment_prefix=comment_prefix)

        current_index = in_file.tell()
        previous_text = b""
        current_column_index = -1

        sql = '''INSERT INTO columns (column_index, column_name)
                 VALUES (?, ?)'''
        save_tuples = []

        while (newline_index := (next_text := previous_text + in_file.read(file_read_chunk_size)).find(b"\n")) == -1:
            if len(next_text) == 0:
                break

            last_delimiter_index = next_text.rfind(delimiter)

            if last_delimiter_index == -1:
                previous_text = next_text
            else:
                previous_text = next_text[(last_delimiter_index + 1):]

                for column_name in next_text[:last_delimiter_index].split(delimiter):
                    current_column_index += 1

                    if start_column_index <= current_column_index < end_column_index:
                        cursor.execute(sql, (current_column_index, column_name,))

        for column_name in next_text[:newline_index].split(delimiter):
            current_column_index += 1

            if start_column_index <= current_column_index < end_column_index:
                save_tuples.append((current_column_index, column_name,))

                if len(save_tuples) == out_items_chunk_size:
                    cursor.executemany(sql, save_tuples)
                    save_tuples = []

        in_file.seek(current_index + newline_index + 1)

    if len(save_tuples) > 0:
        cursor.executemany(sql, save_tuples)

# This function is executed in parallel.
def parse_column_info(delimited_file_path, f4_file_path, comment_prefix, delimiter, file_read_chunk_size, chunk_number, start_column_index, end_column_index, tmp_dir_path, out_items_chunk_size, verbose):
    print_message(f"Parsing column names, sizes, and types when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose)

    columns_file_path = get_columns_database_path(tmp_dir_path, chunk_number)
    conn = connect_sql(columns_file_path)
    cursor = conn.cursor()
    cursor.execute('BEGIN TRANSACTION')

    create_column_database(cursor)
    populate_database_with_column_names(delimited_file_path, comment_prefix, delimiter, file_read_chunk_size, start_column_index, end_column_index, out_items_chunk_size, cursor)

    sql_update = '''UPDATE columns
                    SET size = MAX(size, ?), num_i = num_i + ?, num_f = num_f + ?, num_s = num_s + ?
                    WHERE column_index = ?'''

    num_rows = 0

    # We will find the max size for each column and count how many there are of each type.
    with get_delimited_file_handle(delimited_file_path) as in_file:
        skip_comments(in_file, comment_prefix)
        skip_line(in_file)

        # Loop through the file for the specified columns and update the dictionaries.
        save_tuples = []
        for column_index, value in iterate_delimited_file_column_indices(in_file, delimiter, file_read_chunk_size, start_column_index, end_column_index):
            if column_index == start_column_index:
                num_rows += 1

            if start_column_index <= column_index < end_column_index:
                this_size = len(value)
                i, f, s = infer_type(value)

                save_tuples.append((this_size, i, f, s, column_index,))

            if len(save_tuples) == out_items_chunk_size:
                cursor.executemany(sql_update, save_tuples)
                save_tuples = []

        if len(save_tuples) > 0:
            cursor.executemany(sql_update, save_tuples)

    # Infer the type based on the counts we have.
    sql_infer_type = '''UPDATE columns
                        SET inferred_type = CASE
                          WHEN num_s > 0 THEN "s"
                          WHEN num_f > 0 THEN "f"
                          ELSE "i"
                        END
                        WHERE column_index = ?'''

    cursor.executemany(sql_infer_type, ((column_index,) for column_index in range(start_column_index, end_column_index)))

    conn.commit()
    cursor.close()
    conn.close()

    print_message(f"Done parsing column names, sizes, and types when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose)

# This function is executed in parallel.
def save_formatted_data(delimited_file_path, f4_file_path, comment_prefix, delimiter, file_read_chunk_size, chunk_number, start_column_index, end_column_index, tmp_dir_path, out_items_chunk_size, verbose):
    data_file_path = get_data_path(tmp_dir_path, "data", chunk_number)
    ll_file_path = get_data_path(tmp_dir_path, "ll", chunk_number)

    print_message(f"Saving formatted data when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose)

    columns_file_path = get_columns_database_path(tmp_dir_path, chunk_number)
    conn = connect_sql(columns_file_path)
    cursor = conn.cursor()

    cursor.execute('''SELECT SUM(size) AS size
                      FROM columns
                      WHERE column_index BETWEEN ? AND ?''', (start_column_index, end_column_index,))
    line_length = cursor.fetchone()["size"]

    write_str_to_file(ll_file_path, str(line_length).encode())

    with get_delimited_file_handle(delimited_file_path) as in_file:
        skip_comments(in_file, comment_prefix)
        skip_line(in_file)  # Header line

        # Save data.
        with open(data_file_path, 'wb') as data_file:
            out_list = []
            num_columns_to_parse = end_column_index - start_column_index

            column_size_cache = {}
            if num_columns_to_parse <= 1000:
                cursor.close()
                cursor = conn.cursor()
                cursor.execute('''SELECT size
                                  FROM columns''')

                for column_index in range(start_column_index, end_column_index):
                    column_size_cache[column_index] = cursor.fetchone()["size"]

            for column_index, value in iterate_delimited_file_column_indices(in_file, delimiter, file_read_chunk_size, start_column_index, end_column_index):
                if len(column_size_cache) == 0:
                    if column_index == start_column_index:
                        cursor.close()
                        cursor = conn.cursor()
                        cursor.execute('''SELECT size
                                          FROM columns''')

                    column_size = cursor.fetchone()["size"]
                else:
                    column_size = column_size_cache[column_index]

                out_list.append(format_string_as_fixed_width(value, column_size))

                if len(out_list) == out_items_chunk_size:
                    data_file.write(b"".join(out_list))
                    out_list = []

            if len(out_list) > 0:
                data_file.write(b"".join(out_list))

    cursor.close()
    conn.close()

    print_message(f"Done saving formatted data when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose)

def combine_column_databases(delimited_file_path, f4_file_path, column_chunk_indices, tmp_dir_path, verbose):
    # Combine the column sizes and types across the chunks.
    # We won't add the num_* columns to the combined file because they are no longer necessary.
    print_message(f"Combining column databases when converting {delimited_file_path} to {f4_file_path}.", verbose)

    conn_0 = connect_sql(get_columns_database_path(tmp_dir_path, 0))
    cursor_0 = conn_0.cursor()

    for chunk_number in range(1, len(column_chunk_indices)):
        chunk_file_path = get_columns_database_path(tmp_dir_path, chunk_number)
        cursor_0.execute(f"ATTACH DATABASE '{chunk_file_path}' AS db_chunk")
        cursor_0.execute(f"INSERT INTO columns (column_index, column_name, size, inferred_type) SELECT column_index, column_name, size, inferred_type FROM db_chunk.columns")
        cursor_0.execute(f"DETACH DATABASE db_chunk")
        remove(chunk_file_path)

    cursor_0.close()
    conn_0.close()

    rename(get_columns_database_path(tmp_dir_path, 0), get_columns_database_path(tmp_dir_path))

def get_max_column_length(cursor, column_name):
    sql = f'''SELECT MAX(LENGTH(CAST({column_name} AS TEXT))) AS max_length
              FROM columns'''

    cursor.execute(sql)
    return cursor.fetchone()["max_length"]

def save_column_name_info(delimited_file_path, f4_file_path, out_items_chunk_size, tmp_dir_path, verbose):
    print_message(f"Saving column name info when converting {delimited_file_path} to {f4_file_path}.", verbose)

    conn = connect_sql(get_columns_database_path(tmp_dir_path))
    cursor = conn.cursor()

    max_column_name_length = get_max_column_length(cursor, "column_name")
    max_column_index_length = get_max_column_length(cursor, "column_index")

    ##########################################################
    # cni = column name first, sorted by column name
    # cin = column index first, sorted by column index
    ##########################################################

    cniccml = max(len(str(max_column_name_length)), len(str(max_column_name_length + max_column_index_length)))
    write_str_to_file(get_data_path(tmp_dir_path, f"cniccml"), str(cniccml).encode())

    cnicc = format_string_as_fixed_width(b"0", cniccml)
    cnicc += format_string_as_fixed_width(str(max_column_name_length).encode(), cniccml)
    cnicc += format_string_as_fixed_width(str(max_column_name_length + max_column_index_length).encode(), cniccml)
    write_str_to_file(get_data_path(tmp_dir_path, f"cnicc"), cnicc)

    with open(get_data_path(tmp_dir_path, f"cni"), "wb") as data_file:
        sql = f'''SELECT CAST(column_name AS TEXT) AS column_name, CAST(column_index AS TEXT) AS column_index
                  FROM columns
                  ORDER BY column_name'''
        cursor.execute(sql)

        while True:
            batch = cursor.fetchmany(out_items_chunk_size)

            if not batch:
                break

            out_list = []
            for row in batch:
                out_list.append(format_string_as_fixed_width(row["column_name"].encode(), max_column_name_length) + format_string_as_fixed_width(row["column_index"].encode(), max_column_index_length))
            data_file.write(b"".join(out_list))

    with open(get_data_path(tmp_dir_path, f"cn"), "wb") as data_file:
        sql = f'''SELECT CAST(column_name AS TEXT) AS column_name
                  FROM columns
                  ORDER BY column_index'''
        cursor.execute(sql)

        is_first_batch = True

        while True:
            batch = cursor.fetchmany(out_items_chunk_size)

            if not batch:
                break

            out_list = []
            for row in batch:
                out_list.append(row["column_name"].encode())

            if is_first_batch:
                is_first_batch = False
            else:
                data_file.write(b"\n")

            data_file.write(b"\n".join(out_list))

    cursor.close()
    conn.close()

def save_column_types(delimited_file_path, f4_file_path, out_items_chunk_size, tmp_dir_path, verbose):
    print_message(f"Saving column type and size info when converting {delimited_file_path} to {f4_file_path}.", verbose)

    conn = connect_sql(get_columns_database_path(tmp_dir_path))
    cursor = conn.cursor()

    max_type_length = 1 # This should always be 1 unless we add a bunch of types.

    with open(get_data_path(tmp_dir_path, f"ct"), "wb") as data_file:
        sql = f'''SELECT CAST(inferred_type AS TEXT) AS inferred_type
                  FROM columns
                  ORDER BY column_index'''
        cursor.execute(sql)

        while True:
            batch = cursor.fetchmany(out_items_chunk_size)

            if not batch:
                break

            out_list = []
            for row in batch:
                out_list.append(format_string_as_fixed_width(row["inferred_type"].encode(), max_type_length))

            data_file.write(b"".join(out_list))

    cursor.close()
    conn.close()

def save_column_coordinates(delimited_file_path, f4_file_path, out_items_chunk_size, tmp_dir_path, verbose):
    print_message(f"Saving column coordinates when converting {delimited_file_path} to {f4_file_path}.", verbose)

    conn = connect_sql(get_columns_database_path(tmp_dir_path))
    cursor = conn.cursor()

    sql = f'''SELECT SUM(size) AS size
              FROM columns'''
    cursor.execute(sql)
    max_coord_length = len(str(cursor.fetchone()["size"]))

    with open(get_data_path(tmp_dir_path, f"cc"), "wb") as data_file:
        sql = f'''SELECT size AS size
                  FROM columns
                  ORDER BY column_index'''
        cursor.execute(sql)

        coord = 0
        data_file.write(format_string_as_fixed_width(str(coord).encode(), max_coord_length))

        while True:
            batch = cursor.fetchmany(out_items_chunk_size)

            if not batch:
                break

            out_list = []
            for row in batch:
                coord += row["size"]
                out_list.append(format_string_as_fixed_width(str(coord).encode(), max_coord_length))

            data_file.write(b"".join(out_list))

    cursor.close()
    conn.close()

    write_str_to_file(get_data_path(tmp_dir_path, f"ccml"), str(max_coord_length).encode())

def combine_data_for_column_chunks(delimited_file_path, f4_file_path, column_chunk_indices, tmp_dir_path, verbose):
    print_message(f"Combining data for column chunks when converting {delimited_file_path} to {f4_file_path}.", verbose)

    file_handles = {}
    for chunk_number in range(len(column_chunk_indices)):
        file_handles[chunk_number] = open(get_data_path(tmp_dir_path, "data", chunk_number), "rb")

    line_lengths = {}
    for chunk_number in range(len(column_chunk_indices)):
        line_length_file_path = get_data_path(tmp_dir_path, "ll", chunk_number)
        line_lengths[chunk_number] = int(read_str_from_file(line_length_file_path))
        remove(line_length_file_path)

    line_length_total = sum(line_lengths.values())

    with open(get_data_path(tmp_dir_path, "data"), "wb") as out_file:
        num_rows = 0
        while line_0 := file_handles[0].read(line_lengths[0]):
            num_rows += 1
            out_file.write(line_0)

            for chunk_number in range(1, len(column_chunk_indices)):
                out_file.write(file_handles[chunk_number].read(line_lengths[chunk_number]))

    for chunk_number in range(len(column_chunk_indices)):
        file_handles[chunk_number].close()
        remove(get_data_path(tmp_dir_path, "data", chunk_number))

    return num_rows, line_length_total

def compress_data(tmp_dir_path, compression_type, num_rows, line_length):
    if not compression_type:
        return

    # For now, we assume z-standard compression.
    compressor = ZstdCompressor(level=1)

    # Compress the data.
    # If necessary, chunk the compression so we can handle extremely wide files.
    compressed_row_lengths = []
    # compressed_row_ends = []
    # current_compressed_row_end = 0
    compressed_lines_to_save = []
    compressed_chars_not_saved = 0

    # Save the non-compressed num_rows and line length for later reference.
    # line_lengths_dict["num_rows"] = num_rows
    # line_lengths_dict["ll"] = line_length

    with open(get_data_path(tmp_dir_path, "data"), 'rb') as file_handle:
        with mmap(file_handle.fileno(), 0, prot=PROT_READ) as mmap_handle:
            with open(get_data_path(tmp_dir_path, "cmpr"), "wb") as cmpr_file:
                for row_i in range(num_rows):
                    row_start = row_i * line_length
                    row_end = (row_i + 1) * line_length

                    compressed_line = compressor.compress(mmap_handle[row_start:row_end])

                    # current_compressed_row_end += len(compressed_line)
                    compressed_row_lengths.append(len(compressed_line))
                    # compressed_row_ends.append(current_compressed_row_end)
                    compressed_lines_to_save.append(compressed_line)
                    compressed_chars_not_saved += len(compressed_line)

                    if compressed_chars_not_saved >= 1000000:
                        cmpr_file.write(b"".join(compressed_lines_to_save))
                        compressed_lines_to_save = []
                        compressed_chars_not_saved = 0

                if compressed_chars_not_saved > 0:
                    cmpr_file.write(b"".join(compressed_lines_to_save))

    rename(get_data_path(tmp_dir_path, "cmpr"), get_data_path(tmp_dir_path, "data"))

    with open(get_data_path(tmp_dir_path, "rl"), "wb") as re_file:
        re_file.write(serialize(compressed_row_lengths))

    with open(get_data_path(tmp_dir_path, "ll"), "wb") as ll_file:
        ll_file.write(str(line_length).encode())

    with open(get_data_path(tmp_dir_path, "nrow"), "wb") as nrow_file:
        nrow_file.write(str(num_rows).encode())

    # Indicate the compression type.
    with open(get_data_path(tmp_dir_path, "cmpr"), "wb") as cmpr_file:
        # if compression_type == "dictionary":
        #     column_decompression_dicts = {}
        #     # To enable decompression, we need to invert the column indices and column names.
        #     # We also need to invert the values and compressed values.
        #     for column_index, compression_dict in column_compression_dicts.items():
        #         column_name = column_index_name_dict[column_index]
        #         column_decompression_dicts[column_name] = column_compression_dicts[column_index]
        #
        #         decompression_dict = {}
        #         for value, compressed_value in compression_dict["map"].items():
        #             decompression_dict[convert_bytes_to_int(compressed_value)] = value
        #
        #         column_decompression_dicts[column_name]["map"] = decompression_dict
        #
        #     cmpr_file.write(serialize(column_decompression_dicts))
        # else:
        cmpr_file.write(b"z")

def build_indexes(f4_file_path, tmp_dir_path, index_columns, num_rows, line_length, num_parallel, verbose=False):
    index_info_dict = {}

    if isinstance(index_columns, str):
        reverse_statuses = build_index(f4_file_path, tmp_dir_path, 0, [index_columns], num_rows, line_length, verbose)
        index_info_dict[(index_columns, reverse_statuses[0])] = 0
    elif isinstance(index_columns, list):
        for i, index_column in enumerate(index_columns):
            if not isinstance(index_column, str) and not isinstance(index_column, list):
                raise Exception("When specifying index columns, they must either be a string or a list.")

        keys = joblib.Parallel(n_jobs=num_parallel)(
            joblib.delayed(build_index_parallel)(f4_file_path, tmp_dir_path, num_rows, line_length, i, index_column, verbose)
            for i, index_column in enumerate(index_columns)
        )

        for i, key in enumerate(keys):
            index_info_dict[key] = i
    else:
        raise Exception("When specifying index columns, they must either be a string or a list.")

    write_str_to_file(f"{tmp_dir_path}i", serialize(index_info_dict))

def build_index(f4_file_path, tmp_dir_path, index_number, index_columns, num_rows, line_length, verbose):
    # TODO: Add logic to verify that index_column is valid. Make sure to check for names ending with $ (reverse).

    print_message(f"Saving index for {', '.join(index_columns)} column(s) in {f4_file_path}.", verbose)

    out_index_file_path_prefix = f"{tmp_dir_path}i{index_number}"
    ccml = fast_int(read_str_from_file(get_data_path(tmp_dir_path, "ccml")))

    reverse_statuses = []
    column_indices = []
    column_types = []
    start_coords = []
    end_coords = []

    for i, index_column in enumerate(index_columns):
        if "|" in index_column:
            raise Exception("You may not index a column with a vertical bar (|) in its name.")

        if index_column.endswith("_endswith"):
            reverse_statuses.append(True)
            index_columns[i] = index_columns[i].rstrip("_endswith")
        else:
            reverse_statuses.append(False)

    for i, index_column in enumerate(index_columns):
        column_index, column_type = get_column_index_and_type(tmp_dir_path, index_column)
        column_indices.append(column_index)
        column_types.append(column_type)

        start_coord, end_coord = get_column_index_coords(tmp_dir_path, column_index, ccml)
        start_coords.append(start_coord)
        end_coords.append(end_coord)

    sql_create_table = f'CREATE TABLE index_data ({index_columns[0]} TEXT NOT NULL'
    for i in range(1, len(index_columns)):
        sql_create_table += f", {index_columns[i]} TEXT NOT NULL"
    sql_create_table += ")"

    index_database_file_path = f"{out_index_file_path_prefix}.db"
    conn = connect_sql(index_database_file_path)
    execute_sql(conn, sql_create_table)

    max_value_lengths = [0 for x in index_columns]
    non_committed_values = []
    cursor = conn.cursor()
    cursor.execute('BEGIN TRANSACTION')

    sql_insert = f'''INSERT INTO index_data ({', '.join(index_columns)})
                     VALUES ({', '.join(['?' for x in index_columns])})'''

    with open(get_data_path(tmp_dir_path, "data"), 'rb') as file_handle:
        with mmap(file_handle.fileno(), 0, prot=PROT_READ) as mmap_handle:
            for row_index in range(num_rows):
                start_pos = row_index * line_length

                values = []
                for i, index_column in enumerate(index_columns):
                    value = mmap_handle[(start_pos + start_coords[i]):(start_pos + end_coords[i])]

                    if reverse_statuses[i]:
                        value = reverse_string(value)

                    values.append(value)
                    max_value_lengths[i] = max(max_value_lengths[i], len(value))

                non_committed_values.append(values)
                if len(non_committed_values) == 10000:
                    cursor.executemany(sql_insert, non_committed_values)
                    non_committed_values = []

            if len(non_committed_values) > 0:
                cursor.executemany(sql_insert, non_committed_values)

    conn.commit()
    cursor.close()
    conn.close()

    print_message(f"Querying temporary database when indexing the {', '.join(index_columns)} column(s) in {f4_file_path}.", verbose)

    conn = connect_sql(index_database_file_path)
    max_row_index_length = len(str(num_rows - 1))

    with open(f"{out_index_file_path_prefix}", "wb") as index_data_file:
        sql_query = f'''SELECT rowid - 1 AS rowid, {', '.join(index_columns)}
                        FROM index_data
                        ORDER BY {index_columns[0]}'''

        for i in range(1, len(column_types)):
            if column_types[i] == "s":
                sql_query += f", {index_columns[i]}"
            elif column_types[i] == "i":
                sql_query += f", CAST({index_columns[i]} AS INTEGER)"
            else:
                sql_query += f", CAST({index_columns[i]} AS REAL)"

        cursor = conn.cursor()
        cursor.execute(sql_query)

        # Fetch rows in batches to prevent using too much memory
        batch_size = 10000
        row_index = 0

        while True:
            batch = cursor.fetchmany(batch_size)

            if not batch:
                break

            batch_out = []
            for row in batch:
                out_row = b""

                for i, index_column in enumerate(index_columns):
                    out_row += format_string_as_fixed_width(row[index_column], max_value_lengths[i])

                batch_out.append(out_row + format_string_as_fixed_width(str(row["rowid"]).encode(), max_row_index_length))
                row_index += 1

            index_data_file.write(b"".join(batch_out))

        cursor.close()
    conn.close()

    print_message(f"Done querying temporary database when indexing the {', '.join(index_columns)} column(s) in {f4_file_path}.", verbose)

    coords = [0]
    for mvl in max_value_lengths:
        coords.append(coords[-1] + mvl)
    coords.append(coords[-1] + max_row_index_length)
    coords = [str(x).encode() for x in coords]

    ccml = max([len(x) for x in coords])
    write_str_to_file(f"{out_index_file_path_prefix}ccml", str(ccml).encode())

    cc = b""
    for x in coords:
        cc += format_string_as_fixed_width(x, ccml)
    write_str_to_file(f"{out_index_file_path_prefix}cc", cc)

    remove(index_database_file_path)

    print_message(f"Done building index file for {index_column} column in {f4_file_path}.", verbose)

    return reverse_statuses

def build_index_parallel(f4_file_path, tmp_dir_path, num_rows, line_length, index_number, index_column, verbose):
    index_column_list = [index_column] if isinstance(index_column, str) else index_column
    reverse_statuses = build_index(f4_file_path, tmp_dir_path, index_number, index_column_list, num_rows, line_length, verbose)

    key = []
    for j, index_column in enumerate(index_column_list):
        key.append((index_column, reverse_statuses[j]))

    return tuple(key)

def get_column_index_and_type(tmp_dir_path, index_column_name):
    columns_file_path = get_columns_database_path(tmp_dir_path)

    conn = connect_sql(columns_file_path)
    cursor = conn.cursor()

    cursor.execute('''SELECT column_index, inferred_type
                      FROM columns
                      WHERE TRIM(column_name) = ?''', (index_column_name, ))

    row = cursor.fetchone()

    if not row:
        raise Exception(f"No column exists with the name '{index_column_name}.'")
        cursor.close()
        conn.close()

    index_column_index = row["column_index"]
    index_column_type = row["inferred_type"]

    cursor.close()
    conn.close()

    return index_column_index, index_column_type

def get_column_index_coords(tmp_dir_path, index_column_index, ccml):
    with open(get_data_path(tmp_dir_path, "cc"), 'rb') as file_handle:
        with mmap(file_handle.fileno(), 0, prot=PROT_READ) as mmap_handle:
            pos_a = index_column_index * ccml
            pos_b = pos_a + ccml
            pos_c = pos_b + ccml

            return fast_int(mmap_handle[pos_a:pos_b]), fast_int(mmap_handle[pos_b:pos_c])

def combine_into_single_file(delimited_file_path, f4_file_path, tmp_dir_path, read_chunk_size, verbose):
    def _create_file_map(start_end_positions):
        start_end_dict = {}

        for x in start_end_positions:
            file_extension = x[0]
            positions = x[1:]
            start_end_dict[file_extension] = positions

        serialized = serialize(start_end_dict)
        return str(len(serialized)).encode() + b"\n" + serialized

    print_message(f"Combining data into a single file for {delimited_file_path} to {f4_file_path}.", verbose)

    write_str_to_file(f"{tmp_dir_path}ver", get_current_version_major().encode())

    start_end_positions = []
    for file_path in sorted(glob(f"{tmp_dir_path}*")):
        extension = path.basename(file_path)
        extension = "" if extension == "data" else extension
        file_size = path.getsize(file_path)

        if len(start_end_positions) == 0:
            start = 0
            end = file_size
        else:
            start = start_end_positions[-1][-1]
            end = start + file_size

        start_end_positions.append([extension, start, end])

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

        for file_start_end in start_end_positions:
            file_path = f"{tmp_dir_path}{file_start_end[0]}"
            if file_start_end[0] == "":
                file_path += "data"

            with open(file_path, "rb") as component_file:
                while chunk := component_file.read(read_chunk_size):
                    f4_file.write(chunk)

            remove(file_path)

                            # if compression_type == "dictionary":
                            #     # Figure out whether we should use categorical compression.
                            #     # If there are more than 100 unique values, do not use categorical compression.
                            #     # This is a rough threshold. It also means that files with few rows will almost always use categorical compression.
                            #     # TODO: Consider refining this approach.
                            #     UNIQUE_THRESHOLD = 100
                            #
                            #     # Specify the default compression information for each column.
                            #     # The default is categorical.
                            #     for i in range(start_column_index, end_column_index):
                            #         column_compression_dicts[i] = {"compression_type": b"c", "map": {}}
                            #
                            #     column_unique_dict = {i: set() for i in range(start_column_index, end_column_index)}
                            #     column_max_length_dict = {i: 0 for i in range(start_column_index, end_column_index)}
                            #     column_bigrams_dict = {i: set() for i in range(start_column_index, end_column_index)}
                            #
                            #     with get_delimited_file_handle(delimited_file_path) as in_file:
                            #         exclude_comments_and_header(in_file, comment_prefix)
                            #
                            #         for line in in_file:
                            #             line_items = line.rstrip(b"\n").split(delimiter)
                            #
                            #             for i in range(start_column_index, end_column_index):
                            #                 value = line_items[i]
                            #                 column_max_length_dict[i] = max(column_max_length_dict[i], len(value))
                            #
                            #                 for bigram in find_unique_bigrams(value):
                            #                     column_bigrams_dict[i].add(bigram)
                            #
                            #                 if column_compression_dicts[i]["compression_type"] == b"c":
                            #                     column_unique_dict[i].add(value)
                            #
                            #                     if len(column_unique_dict[i]) >= UNIQUE_THRESHOLD:
                            #                         column_compression_dicts[i]["compression_type"] = column_types_dict[i]
                            #                         column_unique_dict[i] = None
                            #
                            #     for i in range(start_column_index, end_column_index):
                            #         if column_compression_dicts[i]["compression_type"] == b"c":
                            #             unique_values = sorted(column_unique_dict[i])
                            #             num_bytes = get_bigram_size(len(unique_values))
                            #
                            #             for j, value in enumerate_for_compression(unique_values):
                            #                 #column_compression_dicts[i]["map"][value] = int2ba(j, length = length).to01()
                            #                 column_compression_dicts[i]["map"][value] = j.to_bytes(length = num_bytes, byteorder = "big")
                            #
                            #             column_sizes_dict[i] = num_bytes
                            #         else:
                            #             bigrams = sorted(column_bigrams_dict[i])
                            #             num_bytes = get_bigram_size(len(bigrams))
                            #
                            #             for j, bigram in enumerate_for_compression(bigrams):
                            #                 #column_compression_dicts[i]["map"][bigram] = int2ba(j, length = length).to01()
                            #                 column_compression_dicts[i]["map"][bigram] = j.to_bytes(length = num_bytes, byteorder = "big")
                            #
                            #             column_sizes_dict[i] = column_max_length_dict[i] * num_bytes

def skip_comments(in_file, comment_prefix):
    next_text = in_file.read(len(comment_prefix))

    while next_text == comment_prefix:
        skip_line(in_file)
        next_text = in_file.read(len(comment_prefix))

    in_file.seek(in_file.tell() - len(next_text))

# This function is slow when working with gzipped files because of the backwards seek(). Only use it sparingly.
def skip_line(in_file):
    chunk_size = 100000
    current_index = in_file.tell()

    while (newline_index := (next_text := in_file.read(chunk_size)).find(b"\n")) == -1:
        current_index += len(next_text)

    in_file.seek(current_index + newline_index + 1)

def skip_lines(in_file, num_lines_to_skip):
    if num_lines_to_skip <= 0:
        return

    chunk_size = 100000
    num_lines_skipped = 0

    while next_text := in_file.read(chunk_size):
        how_far_to_reverse = len(next_text)

        while (newline_index := next_text.find(b"\n")) > -1:
            num_lines_skipped += 1
            how_far_to_reverse -= newline_index + 1

            if num_lines_skipped == num_lines_to_skip:
                in_file.seek(in_file.tell() - how_far_to_reverse)
                return
            else:
                next_text = next_text[newline_index + 1:]

def iterate_delimited_file_column_indices(in_file, delimiter, file_read_chunk_size, start_column_index, end_column_index):
    previous_text = b""
    current_column_index = -1

    while next_text := in_file.read(file_read_chunk_size):
        next_text = previous_text + next_text
        lines = next_text.split(b"\n")

        for line_index, line in enumerate(lines):
            line_items = line.split(delimiter)

            if line_index == (len(lines) - 1):
                previous_text = line_items.pop(-1)

            for column_index, item in enumerate(line_items):
                current_column_index += 1

                if start_column_index <= current_column_index < end_column_index:
                    yield current_column_index, item

            if line_index != (len(lines) - 1):
                current_column_index = -1

def generate_column_chunk_ranges(num_cols, num_cols_per_chunk, num_parallel):
    if num_cols <= num_parallel:
        return [[i, i + 1] for i in range(num_cols)]

    last_end_index = 0
    ranges = []

    while last_end_index != num_cols:
        last_start_index = last_end_index
        last_end_index = min([last_start_index + num_cols_per_chunk, num_cols])
        ranges.append([last_start_index, last_end_index])

    return ranges

def infer_type(value):
    if isint(value):
        return 1, 0, 0
    if isfloat(value):
        return 0, 1, 0
    return 0, 0, 1

# def find_unique_bigrams(value):
#     grams = set()
#
#     for start_i in range(0, len(value), 2):
#         end_i = (start_i + 2)
#         grams.add(value[start_i:end_i])
#
#     return grams

# We skip the space character because it causes a problem when we parse from a file.
# def enumerate_for_compression(values):
#     ints = []
#     capacity = len(values)
#     length = get_bigram_size(capacity)
#
#     i = 0
#     while len(ints) < capacity:
#         if b' ' not in i.to_bytes(length = length, byteorder = "big"):
#             ints.append(i)
#
#         i += 1
#
#     for i in ints:
#         yield i, values.pop(0)

def prepare_tmp_dir(tmp_dir_path):
    # Figure out where temp files will be stored and create directory, if needed.
    if tmp_dir_path:
        makedirs(tmp_dir_path, exist_ok=True)
        tmp_dir_path = tmp_dir_path
    else:
        tmp_dir_path = mkdtemp()

    unique_id = uuid4()
    tmp_dir_path = fix_dir_path_ending(tmp_dir_path) + f"f4_{unique_id}/"
    makedirs(tmp_dir_path, exist_ok=True)

    return tmp_dir_path

#TODO: Move to a different file so we don't need to import Parser?
# def transpose_lines_to_temp(data_file_path, tmp_dir_path, start_row_index, end_row_index, column_names, column_coords, bigram_size_dict, verbose):
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