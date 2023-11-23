# from .Parser import *
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
    # max_columns_per_chunk = 10000000
    out_items_chunk_size = 10000

    num_cols, max_column_name_length = preview_column_names(delimited_file_path, f4_file_path, comment_prefix, delimiter, file_read_chunk_size, verbose)

    # Determine the number of columns per chunk.
    # num_cols_per_chunk = min(ceil(num_cols / num_parallel), max_columns_per_chunk)
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
    # Compress as needed. Get number of rows.
    num_rows, line_length = combine_data_for_column_chunks(delimited_file_path, f4_file_path, column_chunk_indices, tmp_dir_path2, verbose)

    if num_rows == 0:
        raise Exception(f"A header row but no data rows were detected in {delimited_file_path}.")

    write_compression_info(tmp_dir_path2, compression_type)

    #TODO: Build each index in parallel
    if index_columns:
        build_indexes(f4_file_path, tmp_dir_path2, index_columns, num_rows, line_length, verbose)

    remove(get_columns_database_path(tmp_dir_path2))

    combine_into_single_file(delimited_file_path, f4_file_path, tmp_dir_path2, file_read_chunk_size, verbose)

    print_message(f"Done converting {delimited_file_path} to {f4_file_path}.", verbose)

#TODO: Move to a different file so we don't need to import Parser?
def transpose(f4_src_file_path, f4_dest_file_path, num_parallel=1, tmp_dir_path=None, verbose=False):
    if num_parallel > 1:
        global joblib
        joblib = __import__('joblib', globals(), locals())

    print_message(f"Transposing {f4_src_file_path} to {f4_dest_file_path}.", verbose)

    if tmp_dir_path:
        makedirs(tmp_dir_path, exist_ok=True)
    else:
        tmp_dir_path = mkdtemp()

    tmp_dir_path = fix_dir_path_ending(tmp_dir_path)
    makedirs(tmp_dir_path, exist_ok=True)
    tmp_tsv_file_path = f"{tmp_dir_path}tmp.tsv.gz"

    with initialize(f4_src_file_path) as src_file_data:
        column_names, column_type_dict, column_coords_dict, bigram_size_dict = get_column_meta(src_file_data, set(), [])
        column_coords = [column_coords_dict[name] for name in column_names]
        num_rows = src_file_data.stat_dict["num_rows"]

    if num_parallel == 1:
        chunk_file_path = transpose_lines_to_temp(f4_src_file_path, tmp_dir_path, 0, num_rows, column_names, column_coords, bigram_size_dict, verbose)

        print_message(f"Extracting transposed values from {chunk_file_path} for {f4_src_file_path}.", verbose)
        with gzip.open(tmp_tsv_file_path, "w", compresslevel=1) as tmp_tsv_file:
            with open(chunk_file_path, "rb") as chunk_file:
                for column_index, column_name in enumerate(column_names):
                    tmp_tsv_file.write(column_name)

                    column_size = column_coords[column_index][1] - column_coords[column_index][0]
                    for row_index in range(num_rows):
                        tmp_tsv_file.write(b"\t" + chunk_file.read(column_size))

                    tmp_tsv_file.write(b"\n")

            remove(chunk_file_path)
    else:
        row_index_chunks = list(split_integer_list_into_chunks(list(range(num_rows)), num_parallel))

        chunk_file_paths = joblib.Parallel(n_jobs=num_parallel)(
             joblib.delayed(transpose_lines_to_temp)(f4_src_file_path, tmp_dir_path, row_index_chunk[0], row_index_chunk[-1] + 1, column_names, column_coords, bigram_size_dict, verbose) for row_index_chunk in row_index_chunks)

        chunk_file_handles = {}
        for row_index_chunk in row_index_chunks:
            chunk_file_path = f"{tmp_dir_path}{row_index_chunk[0]}"
            chunk_file_handles[chunk_file_path] = open(chunk_file_path, "rb")

        try:
            with gzip.open(tmp_tsv_file_path, "w", compresslevel=1) as tmp_tsv_file:
                for column_index, column_name in enumerate(column_names):
                    tmp_tsv_file.write(column_name)

                    column_size = column_coords[column_index][1] - column_coords[column_index][0]

                    for row_index_chunk in row_index_chunks:
                        chunk_file_path = f"{tmp_dir_path}{row_index_chunk[0]}"

                        for row_index in range(row_index_chunk[0], row_index_chunk[-1] + 1):
                            tmp_tsv_file.write(b"\t" + chunk_file_handles[chunk_file_path].read(column_size))

                    tmp_tsv_file.write(b"\n")
        finally:
            for chunk_file_path in chunk_file_handles:
                chunk_file_handles[chunk_file_path].close()
                remove(chunk_file_path)

    print_message(f"Converting temp file at {tmp_tsv_file_path} to {f4_dest_file_path}.", verbose)
    convert_delimited_file(tmp_tsv_file_path, f4_dest_file_path, compression_type=src_file_data.decompression_type, num_parallel=num_parallel, verbose=verbose)
    remove(tmp_tsv_file_path)

#TODO: Move to a different file so we don't need to import Parser?
def inner_join(f4_left_src_file_path, f4_right_src_file_path, join_column, f4_dest_file_path, num_parallel=1, tmp_dir_path=None, verbose=False):
    print_message(f"Inner joining {f4_left_src_file_path} and {f4_right_src_file_path} based on the {join_column} column, saving to {f4_dest_file_path}.", verbose)

    join_column = join_column.encode()

    if tmp_dir_path:
        makedirs(tmp_dir_path, exist_ok=True)
    else:
        tmp_dir_path = mkdtemp()

    tmp_dir_path = fix_dir_path_ending(tmp_dir_path)
    makedirs(tmp_dir_path, exist_ok=True)
    tmp_tsv_file_path = tmp_dir_path + "tmp.tsv.gz"

    with initialize(f4_left_src_file_path) as left_file_data:
        with initialize(f4_right_src_file_path) as right_file_data:
            left_column_names, left_column_type_dict, left_column_coords_dict, left_bigram_size_dict = get_column_meta(left_file_data, set(), [])
            right_column_names, right_column_type_dict, right_column_coords_dict, right_bigram_size_dict = get_column_meta(right_file_data, set(), [])
            #TODO: Add error checking to make sure join_column is present in left and right.

            left_values = parse_values_in_column(left_file_data, join_column, left_column_coords_dict[join_column], left_bigram_size_dict)
            right_values = parse_values_in_column(right_file_data, join_column, right_column_coords_dict[join_column], right_bigram_size_dict)
            common_values = set(left_values) & set(right_values)

            right_columns_to_save = [name for name in right_column_names if name != join_column]

            right_index_dict = {}
            for i, value in enumerate(right_values):
                if value in common_values:
                    right_index_dict.setdefault(value, []).append(i)

            # FYI: It would be faster to save in zstandard format. But the Python package
            #      doesn't support reading line by line.
            #TODO: Parallelize this?
            with gzip.open(tmp_tsv_file_path, "w", compresslevel=1) as tmp_tsv_file:
                tmp_tsv_file.write(b"\t".join(left_column_names + right_columns_to_save) + b"\n")

                left_parse_function = get_parse_row_values_function(left_file_data)
                right_parse_function = get_parse_row_values_function(right_file_data)
                left_column_coords = [left_column_coords_dict[name] for name in left_column_names]
                right_column_coords = [right_column_coords_dict[name] for name in right_columns_to_save]

                for left_row_index, left_value in enumerate(left_values):
                    if left_value in common_values:
                        for right_row_index in right_index_dict[left_value]:
                            left_save_values = left_parse_function(left_file_data, left_row_index, left_column_coords, bigram_size_dict=left_bigram_size_dict)
                            right_save_values = right_parse_function(right_file_data, right_row_index, right_column_coords, bigram_size_dict=right_bigram_size_dict)

                            tmp_tsv_file.write(b"\t".join(left_save_values + right_save_values) + b"\n")

            # TODO: Expand this logic for all compression types. Make sure to document it.
            compression_type = None
            if left_file_data.decompression_type == "zstd" or right_file_data.decompression_type == "zstd":
                compression_type = "zstd"

            print_message(f"Converting temp file at {tmp_tsv_file_path} to {f4_dest_file_path}.", verbose)
            convert_delimited_file(tmp_tsv_file_path, f4_dest_file_path, compression_type=compression_type, num_parallel=num_parallel, verbose=verbose)

            remove(tmp_tsv_file_path)

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

# def calculate_column_coordinates(delimited_file_path, f4_file_path, chunk_number, start_column_index, end_column_index, tmp_dir_path, verbose):
#     print_message(f"Calculating column coordinates when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose)
#
#     sizes_dict_file_path = get_sizes_dict_path(tmp_dir_path, chunk_number)
#
#     # Do I need to do this or can I just use what's in the database?
#     with shelve.open(sizes_dict_file_path, "r") as column_sizes_dict:
#
#     # Calculate the end coordinate for each column.
#     sql_set_end_coord = '''UPDATE columns
#                            SET end_coordinate = ?
#                            WHERE column_index = ?'''
#
#     cursor.execute(sql_set_end_coord, ((column_index,) for column_index in range(start_column_index, end_column_index)))

# This function is executed in parallel.
def save_formatted_data(delimited_file_path, f4_file_path, comment_prefix, delimiter, file_read_chunk_size, chunk_number, start_column_index, end_column_index, tmp_dir_path, out_items_chunk_size, verbose):
    # columns_file_path = get_columns_database_path(tmp_dir_path, chunk_number)
    sizes_dict_file_path = get_sizes_dict_path(tmp_dir_path, chunk_number)
    data_file_path = get_data_path(tmp_dir_path, "data", chunk_number)
    ll_file_path = get_data_path(tmp_dir_path, "ll", chunk_number)

    # Create a lookup dictionary with the sizes (to speed the next steps).
    # save_sizes_lookup_dict(delimited_file_path, f4_file_path, chunk_number, start_column_index, end_column_index, tmp_dir_path, verbose)

    print_message(f"Saving formatted data when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose)

    columns_file_path = get_columns_database_path(tmp_dir_path, chunk_number)
    conn = connect_sql(columns_file_path)
    cursor = conn.cursor()

    cursor.execute('''SELECT SUM(size) AS size
                      FROM columns
                      WHERE column_index BETWEEN ? AND ?''', (start_column_index, end_column_index,))
    line_length = cursor.fetchone()["size"]

    # line_length = 0
    # with shelve.open(sizes_dict_file_path, "r") as column_sizes_dict:
    #     for column_index_str, column_size in column_sizes_dict.items():
    #         line_length += column_size

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

        # with shelve.open(sizes_dict_file_path, "r") as column_sizes_dict:
        #     # Save data.
        #     with open(data_file_path, 'wb') as data_file:
        #         out_list = []
        #
        #         for column_index, value in iterate_delimited_file_column_indices(in_file, delimiter, file_read_chunk_size, start_column_index, end_column_index):
        #             out_list.append(format_string_as_fixed_width(value, column_sizes_dict[str(column_index)]))
        #
        #             if len(out_list) == out_items_chunk_size:
        #                 data_file.write(b"".join(out_list))
        #                 out_list = []
        #
        #         if len(out_list) > 0:
        #             data_file.write(b"".join(out_list))

    cursor.close()
    conn.close()
    # remove_file_with_possible_suffix(sizes_dict_file_path, verbose)

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

    max_column1_length = get_max_column_length(cursor, "column_name")
    max_column2_length = get_max_column_length(cursor, "column_index")

    cnccml = max(len(str(max_column1_length)), len(str(max_column1_length + max_column2_length)))
    write_str_to_file(get_data_path(tmp_dir_path, f"cnccml"), str(cnccml).encode())

    cncc = format_string_as_fixed_width(b"0", cnccml)
    cncc += format_string_as_fixed_width(str(max_column1_length).encode(), cnccml)
    cncc += format_string_as_fixed_width(str(max_column1_length + max_column2_length).encode(), cnccml)
    write_str_to_file(get_data_path(tmp_dir_path, f"cncc"), cncc)

    with open(get_data_path(tmp_dir_path, f"cn"), "wb") as data_file:
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
                out_list.append(format_string_as_fixed_width(row["column_name"].encode(), max_column1_length) + format_string_as_fixed_width(row["column_index"].encode(), max_column2_length))

            data_file.write(b"".join(out_list))

                # if len(out_list) == out_items_chunk_size:
                #     data_file.write(b"".join(out_list))
                #     out_list = []

        # if len(out_list) > 0:
        #     data_file.write(b"".join(out_list))




        # for row in cursor.fetchall():
        #     out_list.append(format_string_as_fixed_width(row["column_name"].encode(), max_column1_length) + format_string_as_fixed_width(row["column_index"].encode(), max_column2_length))
        #
        #     if len(out_list) == out_items_chunk_size:
        #         data_file.write(b"".join(out_list))
        #         out_list = []
        #
        # if len(out_list) > 0:
        #     data_file.write(b"".join(out_list))

    cursor.close()
    conn.close()

def save_column_types(delimited_file_path, f4_file_path, out_items_chunk_size, tmp_dir_path, verbose):
    print_message(f"Saving column type and size info when converting {delimited_file_path} to {f4_file_path}.", verbose)

    conn = connect_sql(get_columns_database_path(tmp_dir_path))
    cursor = conn.cursor()

    max_type_length = 1 # This should always be 1 unless we add a bunch of types.
    #write_str_to_file(get_data_path(tmp_dir_path, f"ctml"), str(max_type_length).encode())

    # ctcc = format_string_as_fixed_width(b"0", max_type_length)
    # ctcc += format_string_as_fixed_width(str(max_type_length).encode(), max_type_length)
    # write_str_to_file(get_data_path(tmp_dir_path, f"ctcc"), ctcc)

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

            # if len(out_list) > 0:
            #     data_file.write(b"".join(out_list))

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

    # sql = f'''SELECT size
    #           FROM columns
    #           ORDER BY column_index'''
    # cursor.execute(sql)
    #
    # coord = 0
    # for row in cursor.fetchall():
    #     coord += row["size"]
    #
    # max_coord_length = len(str(coord))

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

        # if len(out_list) > 0:
        #     data_file.write(b"".join(out_list))

    cursor.close()
    conn.close()

    write_str_to_file(get_data_path(tmp_dir_path, f"ccml"), str(max_coord_length).encode())

def combine_data_for_column_chunks(delimited_file_path, f4_file_path, column_chunk_indices, tmp_dir_path, verbose):
    # if len(column_chunk_indices) == 1:
    #     rename(get_data_path(tmp_dir_path, "data", 0), get_data_path(tmp_dir_path, "data"))
    #     return

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

def write_compression_info(tmp_dir_path, compression_type):
    if not compression_type:
        return

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

def build_indexes(f4_file_path, tmp_dir_path, index_columns, num_rows, line_length, verbose=False):
    index_info_dict = {}

    if isinstance(index_columns, str):
        reverse_statuses = build_index(f4_file_path, tmp_dir_path, 0, [index_columns], num_rows, line_length, verbose)
        index_info_dict[(index_columns, reverse_statuses[0])] = 0
    elif isinstance(index_columns, list):
        for i, index_column in enumerate(index_columns):
            if not isinstance(index_column, str) and not isinstance(index_column, list):
                raise Exception("When specifying index columns, they must either be a string or a list.")

            index_column_list = [index_column] if isinstance(index_column, str) else index_column
            reverse_statuses = build_index(f4_file_path, tmp_dir_path, i, index_column_list, num_rows, line_length, verbose)

            key = []
            for j, index_column in enumerate(index_column_list):
                key.append((index_column, reverse_statuses[j]))
            key = tuple(key)

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

                # if len(batch_out) == batch_size:
            index_data_file.write(b"".join(batch_out))

                # if len(batch_out) == batch_size:
                #     index_data_file.write(b"".join(batch_out))
                #     batch_out = []

        # if len(batch_out) > 0:
        #     index_data_file.write(b"".join(batch_out))

        cursor.close()
    conn.close()

    print_message(f"Done querying temporary database when indexing the {', '.join(index_columns)} column(s) in {f4_file_path}.", verbose)

    coords = []
    for mvl in max_value_lengths:
        coords.append(sum(coords) + mvl)
    coords.append(coords[-1] + max_row_index_length)
    coords = [str(x).encode() for x in coords]

    ccml = max([len(x) for x in coords])
    write_str_to_file(f"{out_index_file_path_prefix}ccml", str(ccml).encode())

    cc = format_string_as_fixed_width(b"0", ccml)
    for x in coords:
        cc += format_string_as_fixed_width(x, ccml)
    write_str_to_file(f"{out_index_file_path_prefix}cc", cc)

    remove(index_database_file_path)

    print_message(f"Done building index file for {index_column} column in {f4_file_path}.", verbose)

    return reverse_statuses

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

# def build_index(f4_file_path, tmp_dir_path, out_index_file_path_prefix, index_column, num_rows, line_length, verbose):
#     # TODO: Add logic to verify that index_column is valid. Make sure to check for names ending with $ (reverse).
#
#     reverse_values = False
#     if index_column.endswith("_endswith"):
#         reverse_values = True
#         index_column = index_column.rstrip("_endswith")
#         print_message(f"Saving reverse index for {index_column} column in {f4_file_path}.", verbose)
#     else:
#         print_message(f"Saving index for {index_column} column in {f4_file_path}.", verbose)
#
#     columns_file_path = get_columns_database_path(tmp_dir_path)
#
#     conn = connect_sql(columns_file_path)
#     cursor = conn.cursor()
#
#     cursor.execute('''SELECT column_index, inferred_type
#                       FROM columns
#                       WHERE TRIM(column_name) = ?''', (index_column, ))
#
#     row = cursor.fetchone()
#     index_column_index = row["column_index"]
#     index_column_type = row["inferred_type"]
#     ccml = fast_int(read_str_from_file(get_data_path(tmp_dir_path, "ccml")))
#
#     with open(get_data_path(tmp_dir_path, "cc"), 'rb') as file_handle:
#         with mmap(file_handle.fileno(), 0, prot=PROT_READ) as mmap_handle:
#             pos_a = index_column_index * ccml
#             pos_b = pos_a + ccml
#             pos_c = pos_b + ccml
#
#             index_start_coord = fast_int(mmap_handle[pos_a:pos_b])
#             index_end_coord = fast_int(mmap_handle[pos_b:pos_c])
#
#     index_database_file_path = f"{out_index_file_path_prefix}.db"
#     conn = connect_sql(index_database_file_path)
#     sql = f'''CREATE TABLE index_data (
#                  value TEXT NOT NULL
#         )'''
#     execute_sql(conn, sql)
#
#     print_message(f"Building temporary database when indexing the {index_column} column in {f4_file_path}.", verbose)
#     max_value_length = 0
#     non_committed_values = []
#     cursor = conn.cursor()
#     cursor.execute('BEGIN TRANSACTION')
#
#     sql = f'''INSERT INTO index_data (value)
#               VALUES (?)'''
#
#     with open(get_data_path(tmp_dir_path, "data"), 'rb') as file_handle:
#         with mmap(file_handle.fileno(), 0, prot=PROT_READ) as mmap_handle:
#             for row_index in range(num_rows):
#                 start_pos = row_index * line_length
#                 value = mmap_handle[(start_pos + index_start_coord):(start_pos + index_end_coord)]
#
#                 if reverse_values:
#                     value = reverse_string(value)
#
#                 max_value_length = max(max_value_length, len(value))
#
#                 non_committed_values.append(value)
#                 if len(non_committed_values) == 100000:
#                     cursor.executemany(sql, [(value,) for value in non_committed_values])
#                     non_committed_values = []
#
#             if len(non_committed_values) > 0:
#                 cursor.executemany(sql, [(value,) for value in non_committed_values])
#
#             cursor.execute('COMMIT')
#
#     print_message(f"Querying temporary database when indexing the {index_column} column in {f4_file_path}.", verbose)
#
#     max_row_index_length = len(str(num_rows - 1))
#
#     with open(f"{out_index_file_path_prefix}.data", "wb") as index_data_file:
#         sql = '''SELECT rowid - 1 AS rowid, value
#                  FROM index_data '''
#
#         if index_column_type == "s":
#             sql += "ORDER BY value"
#         elif index_column_type == "i":
#             sql += "ORDER BY CAST(value AS INTEGER)"
#         else:
#             sql += "ORDER BY CAST(value AS REAL)"
#
#         cursor = conn.cursor()
#         cursor.execute(sql)
#
#         # Fetch rows in batches to prevent using too much memory
#         batch_size = 100000
#         while True:
#             batch = cursor.fetchmany(batch_size)
#
#             if not batch:
#                 break
#
#             for row in batch:
#                 index_data_file.write(format_string_as_fixed_width(str(row['value']).encode(), max_value_length) + format_string_as_fixed_width(str(row['rowid']).encode(), max_row_index_length))
#
#         cursor.close()
#     conn.close()
#
#     print_message(f"Done querying temporary database when indexing the {index_column} column in {f4_file_path}.", verbose)
#
#     coord1 = str(max_value_length).encode()
#     coord2 = str(max_value_length + max_row_index_length).encode()
#
#     ccml = max(len(coord1), len(coord2))
#     write_str_to_file(f"{out_index_file_path_prefix}ccml", str(ccml).encode())
#
#     cc = format_string_as_fixed_width(b"0", ccml)
#     cc += format_string_as_fixed_width(coord1, ccml)
#     cc += format_string_as_fixed_width(coord2, ccml)
#     write_str_to_file(f"{out_index_file_path_prefix}cc", cc)
#
#     remove(index_database_file_path)
#
#     print_message(f"Done building index file for {index_column} column in {f4_file_path}.", verbose)

# TODO: Combine this function with the above one and make it generic enough to handle indexes with more columns.
# def build_two_column_index(f4_file_path, index_columns, index_file_path, tmp_dir_path, num_rows, line_length, verbose):
#     # TODO: Add logic to verify that index_columns is valid.
#     # TODO: Support column names ending with $ (reverse).
#
#     index_description = '_'.join(index_columns)
#     print_message(f"Saving index for {index_description}.", verbose)
#     # table_name = f"table_{index_position}"
#     # index_name = f"index_{index_position}"
#
#     tmp_index_file_path = f"{tmp_dir_path}tmp"
#     if path.exists(tmp_index_file_path):
#         remove(tmp_index_file_path)
#
#     print_message(f"Getting column meta information for {index_file_path} for {index_description}.", verbose)
#     with initialize(f4_file_path) as file_data:
#         index_column_index_1 = get_column_index_from_name(file_data, index_columns[0])
#         index_column_index_2 = get_column_index_from_name(file_data, index_columns[1])
#         index_column_type_1 = get_column_type_from_index(file_data, index_column_index_1)
#         index_column_type_2 = get_column_type_from_index(file_data, index_column_index_2)
#         index_column_coords = parse_data_coords(file_data, [index_column_index_1, index_column_index_2])
#         index_column_coords_1 = index_column_coords[0]
#         index_column_coords_2 = index_column_coords[1]
#         # sql_type_1 = convert_to_sql_type(index_column_type_1)
#         # sql_type_2 = convert_to_sql_type(index_column_type_2)
#         parse_function = get_parse_row_value_function(file_data)
#
#         conn = connect_sql(tmp_index_file_path)
#
#         # sql = f'''CREATE TABLE index_data (
#         #              value1 {sql_type_1} NOT NULL,
#         #              value2 {sql_type_2} NOT NULL
#         #     )'''
#         sql = f'''CREATE TABLE index_data (
#                      value1 TEXT NOT NULL,
#                      value2 TEXT NOT NULL
#             )'''
#         execute_sql(conn, sql)
#
#         max_value1_length = 0
#         max_value2_length = 0
#         num_non_committed = 0
#
#         print_message(f"Populating index file for {index_file_path}.", verbose)
#         for row_index in range(file_data.stat_dict["num_rows"]):
#             value1 = parse_function(file_data, row_index, index_column_coords_1, bigram_size_dict=None, column_name=index_columns[0]).decode()
#             value2 = parse_function(file_data, row_index, index_column_coords_2, bigram_size_dict=None, column_name=index_columns[1]).decode()
#
#             max_value1_length = max(max_value1_length, len(value1))
#             max_value2_length = max(max_value2_length, len(value2))
#
#             sql = f'''INSERT INTO index_data (value1, value2)
#                       VALUES (?, ?)'''
#             execute_sql(conn, sql, (value1, value2), commit=False)
#
#             num_non_committed += 1
#             if num_non_committed == 10000:
#                 conn.commit()
#                 num_non_committed = 0
#
#         if num_non_committed > 0:
#             conn.commit()
#
#         max_row_index_length = len(str(file_data.stat_dict["num_rows"] - 1))
#
#         tmp_dir_path_data = f"{tmp_dir_path}data/"
#         if not path.exists(tmp_dir_path_data):
#             makedirs(tmp_dir_path_data)
#
#         with open(f"{tmp_dir_path_data}0", "wb") as index_data_file:
#             sql = 'SELECT rowid - 1 AS rowid, value1, value2 FROM index_data ORDER BY '
#
#             if index_column_type_1 == "s":
#                 sql += "value1, "
#             elif index_column_type_1 == "i":
#                 sql += "CAST(value1 AS INTEGER), "
#             else:
#                 sql += "CAST(value1 AS REAL), "
#
#             if index_column_type_2 == "s":
#                 sql += "value2"
#             elif index_column_type_2 == "i":
#                 sql += "CAST(value2 AS INTEGER)"
#             else:
#                 sql += "CAST(value2 AS REAL)"
#
#             cursor = conn.cursor()
#             cursor.execute(sql)
#
#             # Fetch rows in batches to prevent using too much memory
#             batch_size = 10000
#             while True:
#                 batch = cursor.fetchmany(batch_size)
#                 if not batch:
#                     break
#
#                 batch_out = b""
#                 for row in batch:
#                     batch_out += format_string_as_fixed_width(str(row['value1']).encode(), max_value1_length) + format_string_as_fixed_width(str(row['value2']).encode(), max_value2_length) + format_string_as_fixed_width(str(row['rowid']).encode(), max_row_index_length)
#
#                 index_data_file.write(batch_out)
#
#             cursor.close()
#
#         conn.close()
#
#         tmp_dir_path_other = f"{tmp_dir_path}other/"
#         if not path.exists(tmp_dir_path_other):
#             makedirs(tmp_dir_path_other)
#
#         # write_str_to_file(f"{tmp_dir_path_other}ver", get_current_version_major().encode())
#         write_str_to_file(f"{tmp_dir_path_other}ll", str(max_value1_length + max_value2_length + max_row_index_length).encode())
#
#         coord1 = str(max_value1_length).encode()
#         coord2 = str(max_value1_length + max_value2_length).encode()
#         coord3 = str(max_value1_length + max_value2_length + max_row_index_length).encode()
#
#         mccl = max(len(coord1), len(coord2), len(coord3))
#         write_str_to_file(f"{tmp_dir_path_other}mccl", str(mccl).encode())
#
#         cc = format_string_as_fixed_width(b"0", mccl)
#         cc += format_string_as_fixed_width(coord1, mccl)
#         cc += format_string_as_fixed_width(coord2, mccl)
#         cc += format_string_as_fixed_width(coord3, mccl)
#
#         write_str_to_file(f"{tmp_dir_path_other}cc", cc)
#
#         remove(tmp_index_file_path)
#
#         combine_into_single_file(tmp_dir_path_data, tmp_dir_path_other, index_file_path)
#
#         print_message(f"Done building index file for {index_file_path} for table {index_description}.", verbose)

    # tmp_dir_path = fix_dir_path_ending(tmp_dir_path)
    # tmp_dir_path_data = f"{tmp_dir_path}data/"
    # tmp_dir_path_other = f"{tmp_dir_path}other/"
    # makedirs(tmp_dir_path_data, exist_ok=True)
    # makedirs(tmp_dir_path_other, exist_ok=True)
    #
    # if not isinstance(index_column_1, str) or not isinstance(index_column_2, str):
    #     raise Exception("When specifying an index column name, it must be a string.")
    #
    # print_message(f"Saving two-column index for {index_column_1} and {index_column_2}.", verbose)
    #
    # index_name = "____".join([index_column_1, index_column_2])
    # index_column_1_encoded = index_column_1.encode()
    # index_column_2_encoded = index_column_2.encode()
    #
    # print_message(f"Getting column meta information for {index_name} index.", verbose)
    # with initialize(f4_file_path) as file_data:
    #     select_columns, column_type_dict, column_coords_dict, bigram_size_dict = get_column_meta(file_data, set([index_column_1_encoded, index_column_2_encoded]), [])
    #     # TODO: Add logic to verify that index_column_1 and index_column_2 are valid.
    #
    #     index_column_1_type = column_type_dict[index_column_1_encoded]
    #     index_column_2_type = column_type_dict[index_column_2_encoded]
    #     coords_1 = column_coords_dict[index_column_1_encoded]
    #     coords_2 = column_coords_dict[index_column_2_encoded]
    #
    #     parse_function = get_parse_row_value_function(file_data)
    #
    #     values_positions = []
    #     print_message(f"Parsing values and positions for {index_name} index.", verbose)
    #     for row_index in range(file_data.stat_dict["num_rows"]):
    #         value_1 = parse_function(file_data, row_index, coords_1, bigram_size_dict=bigram_size_dict, column_name=index_column_1_encoded)
    #         value_2 = parse_function(file_data, row_index, coords_2, bigram_size_dict=bigram_size_dict, column_name=index_column_2_encoded)
    #
    #         values_positions.append([value_1, value_2, row_index])
    #
    #     print_message(f"Building index file for {index_name}.", verbose)
    #     customize_index_values_positions(values_positions, [index_column_1_type, index_column_2_type], sort_first_two_columns, do_nothing)
    #     write_index_files(values_positions, tmp_dir_path_data, tmp_dir_path_other)
    #
    #     write_str_to_file(f"{tmp_dir_path_other}ver", get_current_version_major().encode())
    #
    #     index_file_path = get_index_file_path(f4_file_path, index_name)
    #     combine_into_single_file(tmp_dir_path_data, tmp_dir_path_other, index_file_path)
    #
    #     print_message(f"Done building two-column index file for {index_name}.", verbose)

# def write_index_files(dict_file_path, tmp_dir_path_data, tmp_dir_path_prefix_other):
#     out_file_path = f"{tmp_dir_path_prefix_other}data"
#     if tmp_dir_path_data:
#         out_file_path = f"{tmp_dir_path_data}0"
#
#     max_key_length = 0
#     max_value_length = 0
#
#     with shelve.open(dict_file_path, "r") as the_dict:
#         for key, value in the_dict.items():
#             key_length = len(key)
#             if key_length > max_key_length:
#                 max_key_length = key_length
#
#             value_length = len(value)
#             if value_length > max_value_length:
#                 max_value_length = value_length
#
#     with shelve.open(dict_file_path, "r") as the_dict:
#         with open(out_file_path, "wb") as out_file:
#             for key, value in sorted(the_dict.items()):
#                 out_line = format_string_as_fixed_width(key.encode(), max_key_length) + format_string_as_fixed_width(value.encode(), max_value_length)
#                 out_file.write(out_line)
#
#     write_str_to_file(f"{tmp_dir_path_prefix_other}ll", str(len(out_line)).encode())
#
#     mccl = max(len(str(max_key_length)), len(str(max_key_length + max_value_length)))
#     write_str_to_file(f"{tmp_dir_path_prefix_other}mccl", str(mccl).encode())
#
#     cc = format_string_as_fixed_width(b"0", mccl)
#     cc += format_string_as_fixed_width(str(max_key_length).encode(), mccl)
#     cc += format_string_as_fixed_width(str(max_key_length + max_value_length).encode(), mccl)
#
#     write_str_to_file(f"{tmp_dir_path_prefix_other}cc", cc)

def combine_into_single_file(delimited_file_path, f4_file_path, tmp_dir_path, read_chunk_size, verbose):
    # print(tmp_dir_path)
    # import sys
    # sys.exit(1)
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

# def combine_into_single_file(delimited_file_path, f4_file_path, tmp_dir_path, index_file_path_prefixes, read_chunk_size, verbose):
#     def _create_file_map(start_end_positions):
#         start_end_dict = {}
#
#         for x in start_end_positions:
#             file_name = x[0]
#
#             if file_name in FILE_KEY_ABBREVIATIONS_STATS:
#                 start_end_dict[FILE_KEY_ABBREVIATIONS_STATS[file_name]] = x[1:]
#             elif file_name in FILE_KEY_ABBREVIATIONS_OTHER:
#                 start_end_dict[FILE_KEY_ABBREVIATIONS_OTHER[file_name]] = x[1:]
#             else:
#                 start_end_dict[FILE_KEY_ABBREVIATIONS_NOCACHE[file_name]] = x[1:]
#
#         serialized = serialize(start_end_dict)
#
#         return str(len(serialized)).encode() + b"\n" + serialized
#
#     print_message(f"Combining data into a single file for {delimited_file_path} to {f4_file_path}.", verbose)
#
#     # Build a list that indicates the file names in according to the dictionary values.
#     component_files = []
#     for extension, index in FILE_KEY_ABBREVIATIONS_NOCACHE.items():
#         component_files.append([index, extension])
#     for extension, index in FILE_KEY_ABBREVIATIONS_STATS.items():
#         component_files.append([index, extension])
#     for extension, index in FILE_KEY_ABBREVIATIONS_OTHER.items():
#         component_files.append([index, extension])
#     sort_first_column(component_files)
#
#     start_end_positions = []
#     for f in component_files:
#         extension = f[1]
#         file_path = f"{tmp_dir_path}{extension}"
#
#         if path.exists(file_path):
#             file_size = path.getsize(file_path)
#
#             if len(start_end_positions) == 0:
#                 start = 0
#                 end = file_size
#             else:
#                 start = start_end_positions[-1][-1]
#                 end = start + file_size
#
#             start_end_positions.append([extension, start, end])
#
#     # Adjust the start and end positions based on the size of the values in this list.
#     # The size of the file map can change as we advance the positions of the other file contents.
#     original_start_end_positions = deepcopy(start_end_positions)
#     file_map_serialized = _create_file_map(start_end_positions)
#     previous_file_map_length = 0
#
#     while previous_file_map_length != (len(file_map_serialized)):
#         previous_file_map_length = len(file_map_serialized)
#
#         start_end_positions = deepcopy(original_start_end_positions)
#         for i in range(len(start_end_positions)):
#             start_end_positions[i][1] += previous_file_map_length
#             start_end_positions[i][2] += previous_file_map_length
#
#         file_map_serialized = _create_file_map(start_end_positions)
#
#     # Write the output file
#     with open(f4_file_path, "wb") as f4_file:
#         f4_file.write(file_map_serialized)
#
#         for file_start_end in start_end_positions:
#             with open(f"{tmp_dir_path}{file_start_end[0]}", "rb") as component_file:
#                 while chunk := component_file.read(read_chunk_size):
#                     f4_file.write(chunk)

# def save_sizes_lookup_dict(delimited_file_path, f4_file_path, chunk_number, start_column_index, end_column_index, tmp_dir_path, verbose):
#     print_message(f"Saving lookup dictionary for sizes when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose)
#
#     columns_file_path = get_columns_database_path(tmp_dir_path, chunk_number)
#     sizes_dict_file_path = get_sizes_dict_path(tmp_dir_path, chunk_number)
#
#     conn = connect_sql(columns_file_path)
#     cursor = conn.cursor()
#
#     # Put the sizes in a shelve dictionary so it is faster to read them.
#     cursor.execute('''SELECT column_index, size
#                       FROM columns
#                       WHERE column_index BETWEEN ? AND ?
#                       ORDER BY column_index''', (start_column_index, end_column_index, ))
#
#     with shelve.open(sizes_dict_file_path, "n", writeback=True) as column_sizes_dict:
#         for row in cursor:
#             column_sizes_dict[str(row["column_index"])] = row["size"]
#
#     cursor.close()
#     conn.close()
#
#     print_message(f"Done saving lookup dictionary for sizes when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose)

# def write_rows(delimited_file_path, tmp_dir_path_rowinfo, tmp_dir_path_chunks, delimiter, comment_prefix, compression_type, column_sizes_types_temp_file_path, compression_dicts_dict_file_path, num_rows, num_parallel, verbose):
#     line_lengths_file_path = f"{tmp_dir_path_rowinfo}0"
#
#     if num_parallel == 1:
#         line_length_result = write_rows_chunk(delimited_file_path, tmp_dir_path_rowinfo, tmp_dir_path_chunks, delimiter, comment_prefix, compression_type, column_sizes_types_temp_file_path, compression_dicts_dict_file_path, 0, 0, num_rows, verbose)
#
#         if not compression_type:
#             write_str_to_file(line_lengths_file_path, str(line_length_result).encode())
#     else:
#         row_chunk_indices = generate_column_chunk_ranges(num_rows, ceil(num_rows / num_parallel) + 1)
#
#         line_length_results = joblib.Parallel(n_jobs=num_parallel)(joblib.delayed(write_rows_chunk)(delimited_file_path, tmp_dir_path_rowinfo, tmp_dir_path_chunks, delimiter, comment_prefix, compression_type, column_sizes_types_temp_file_path, compression_dicts_dict_file_path, i, row_chunk[0], row_chunk[1], verbose) for i, row_chunk in enumerate(row_chunk_indices))
#
#         if compression_type:
#             with shelve.open(f"{line_lengths_file_path}", "w", writeback=True) as main_line_lengths_dict:
#                 for chunk_number in range(1, num_parallel):
#                     row_info_file_path = f"{tmp_dir_path_rowinfo}{chunk_number}"
#
#                     if path.exists(get_path_with_possible_suffix(row_info_file_path)):
#                         with shelve.open(row_info_file_path, "r") as chunk_line_lengths_dict:
#                             item_count = 0
#
#                             for key, value in chunk_line_lengths_dict.items():
#                                 item_count += 1
#                                 print_message(f"Merging line length information {delimited_file_path} - chunk {chunk_number}.", verbose, item_count)
#
#                                 main_line_lengths_dict[key] = value
#         else:
#             write_str_to_file(line_lengths_file_path, str(line_length_results[0]).encode())
#
#     return line_lengths_file_path
#
# def write_rows_chunk(delimited_file_path, tmp_dir_path_rowinfo, tmp_dir_path_chunks, delimiter, comment_prefix, compression_type, column_sizes_types_temp_file_path, compression_dicts_dict_file_path, chunk_number, start_row_index, end_row_index, verbose):
#     with get_delimited_file_handle(delimited_file_path) as in_file:
#         skip_comments(in_file, comment_prefix)
#         skip_line(in_file)  # Header line
#         skip_lines(in_file, start_row_index)
#
#         with open(f"{tmp_dir_path_chunks}{chunk_number}", 'wb') as chunk_file:
#             previous_text = b""
#
#             if compression_type:
#                 if compression_type == "zstd":
#                     compressor = ZstdCompressor(level=1)
#                 else:
#                     raise Exception(f"You specified a compression type ({compression_type}) that is not supported.")
#
#                 line_lengths_dict_file_path = f"{tmp_dir_path_rowinfo}{chunk_number}"
#
#                 with shelve.open(line_lengths_dict_file_path, "n", writeback=True) as line_lengths_dict:
#                     for line_index in range(start_row_index, end_row_index):
#                         line_length, previous_text = save_fixed_width_line(in_file, previous_text, chunk_file, delimiter, column_sizes_types_temp_file_path, compressor)
#
#                         # elif compression_type == "dictionary":
#                             # TODO: Implement this logic
#                             # raise Exception("Not yet supported")
#                             # for i, size in enumerate(column_sizes):
#                             #     if compression_dicts[i]["compression_type"] == b"c":
#                             #         compressed_value = compression_dicts[i]["map"][line_items[i]]
#                             #     else:
#                             #         compressed_value = compress_using_2_grams(line_items[i], compression_dicts[i]["map"])
#                             #
#                             #     out_items.append(format_string_as_fixed_width(compressed_value, size))
#
#                         line_lengths_dict[str(line_index)] = line_length
#
#                         print_message(f"Writing rows chunk {chunk_number} for {delimited_file_path} (start row index = {start_row_index}, end row index = {end_row_index}).", verbose, line_index)
#
#                     print_message(f"Done writing rows chunk {chunk_number} for {delimited_file_path} (start row index = {start_row_index}, end row index = {end_row_index})/", verbose)
#
#                     return line_lengths_dict_file_path
#             else:
#                 for line_index in range(start_row_index, end_row_index):
#                     line_length, previous_text = save_fixed_width_line(in_file, previous_text, chunk_file, delimiter, column_sizes_types_temp_file_path, None)
#
#                     print_message(f"Writing rows chunk {chunk_number} for {delimited_file_path} (start row index = {start_row_index}, end row index = {end_row_index}).", verbose, line_index)
#
#                 print_message(f"Done writing rows chunk {chunk_number} for {delimited_file_path} (start row index = {start_row_index}, end row index = {end_row_index}).", verbose)
#
#                 return line_length
                
        # with get_delimited_file_handle(delimited_file_path) as in_file:
        #     skip_comments(in_file, comment_prefix)
        #     skip_line(in_file)  # Header line
        #     skip_lines(in_file, start_row_index)
        # 
        #     with open(f"{tmp_dir_path_chunks}{chunk_number}", 'wb') as chunk_file:
        #         previous_text = b""
        # 
        #         if compression_type:
        #             if compression_type == "zstd":
        #                 compressor = ZstdCompressor(level=1)
        #             else:
        #                 raise Exception(f"You specified a compression type ({compression_type}) that is not supported.")
        # 
        #             line_lengths_dict_file_path = f"{tmp_dir_path_rowinfo}{chunk_number}"
        # 
        #             with shelve.open(line_lengths_dict_file_path, "n", writeback=True) as line_lengths_dict:
        #                 for line_index in range(start_row_index, end_row_index):
        #                     line_length, previous_text = save_fixed_width_line(in_file, previous_text, chunk_file, delimiter, column_sizes_dict, compressor)
        # 
        #                     # elif compression_type == "dictionary":
        #                         # TODO: Implement this logic
        #                         # raise Exception("Not yet supported")
        #                         # for i, size in enumerate(column_sizes):
        #                         #     if compression_dicts[i]["compression_type"] == b"c":
        #                         #         compressed_value = compression_dicts[i]["map"][line_items[i]]
        #                         #     else:
        #                         #         compressed_value = compress_using_2_grams(line_items[i], compression_dicts[i]["map"])
        #                         #
        #                         #     out_items.append(format_string_as_fixed_width(compressed_value, size))
        # 
        #                     line_lengths_dict[str(line_index)] = line_length
        # 
        #                     print_message(f"Writing rows chunk {chunk_number} for {delimited_file_path} (start row index = {start_row_index}, end row index = {end_row_index})", verbose, line_index)
        # 
        #                 print_message(f"Done writing rows chunk {chunk_number} for {delimited_file_path} (start row index = {start_row_index}, end row index = {end_row_index})", verbose)
        # 
        #                 return line_lengths_dict_file_path
        #         else:
        #             for line_index in range(start_row_index, end_row_index):
        #                 line_length, previous_text = save_fixed_width_line(in_file, previous_text, chunk_file, delimiter, column_sizes_dict, None)
        # 
        #                 print_message(f"Writing rows chunk {chunk_number} for {delimited_file_path} (start row index = {start_row_index}, end row index = {end_row_index})", verbose, line_index)
        # 
        #             print_message(f"Done writing rows chunk {chunk_number} for {delimited_file_path} (start row index = {start_row_index}, end row index = {end_row_index})", verbose)
        # 
        #             return line_length

# def write_rows_chunk(delimited_file_path, tmp_dir_path_rowinfo, tmp_dir_path_chunks, delimiter, comment_prefix, compression_type, column_sizes_dict_file_path, compression_dicts_dict_file_path, chunk_number, start_row_index, end_row_index, num_cols, verbose):
#     line_lengths_dict_file_path = f"{tmp_dir_path_rowinfo}{chunk_number}"
#
#     with shelve.open(line_lengths_dict_file_path, "n", writeback=True) as line_lengths_dict:
#         with shelve.open(column_sizes_dict_file_path, "r") as column_sizes_dict:
#             # Write the data to output file. Ignore the header line.
#             with get_delimited_file_handle(delimited_file_path) as in_file:
#                 skip_comments(in_file, comment_prefix)
#                 skip_line(in_file) # Header line
#                 skip_lines(in_file, start_row_index)
#
#                 compressor = None
#                 if compression_type == "zstd":
#                     compressor = ZstdCompressor(level=1)
#
#                 with open(f"{tmp_dir_path_chunks}{chunk_number}", 'wb') as chunk_file:
#                     previous_text = b""
#
#                     for line_index in range(start_row_index, end_row_index):
#                         line_length, previous_text = save_fixed_width_line(in_file, previous_text, chunk_file, delimiter, column_sizes_dict, compressor)
#
#                         # elif compression_type == "dictionary":
#                             # TODO: Implement this logic
#                             # raise Exception("Not yet supported")
#                             # for i, size in enumerate(column_sizes):
#                             #     if compression_dicts[i]["compression_type"] == b"c":
#                             #         compressed_value = compression_dicts[i]["map"][line_items[i]]
#                             #     else:
#                             #         compressed_value = compress_using_2_grams(line_items[i], compression_dicts[i]["map"])
#                             #
#                             #     out_items.append(format_string_as_fixed_width(compressed_value, size))
#
#                         line_lengths_dict[str(line_index)] = line_length
#
#                         print_message(f"Writing rows chunk {chunk_number} for {delimited_file_path} (start row index = {start_row_index}, end row index = {end_row_index})", verbose, line_index)
#
#                     print_message(f"Done writing rows chunks {chunk_number} for {delimited_file_path} (start row index = {start_row_index}, end row index = {end_row_index})", verbose)

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

# def save_column_names(in_file, column_names_dict_file_path, delimiter):
#     chunk_size = 100000
#     current_index = in_file.tell()
#     previous_text = b""
#     current_column_index = -1
#
#     with shelve.open(column_names_dict_file_path, "n", writeback=True) as column_names_dict:
#         while (newline_index := (next_text := previous_text + in_file.read(chunk_size)).find(b"\n")) == -1:
#             if len(next_text) == 0:
#                 break
#
#             last_delimiter_index = next_text.rfind(delimiter)
#
#             if last_delimiter_index == -1:
#                 previous_text = next_text
#             else:
#                 previous_text = next_text[(last_delimiter_index + 1):]
#
#                 for item in next_text[:last_delimiter_index].split(delimiter):
#                     current_column_index += 1
#                     column_names_dict[str(current_column_index)] = item
#
#         for item in next_text[:newline_index].split(delimiter):
#             current_column_index += 1
#             column_names_dict[str(current_column_index)] = item
#
#         in_file.seek(current_index + newline_index + 1)
#
#         return len(column_names_dict)

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

# def save_fixed_width_line(in_file, text, out_file, delimiter, file_chunk_size, cursor):
#     cursor.execute('''SELECT size
#                       FROM columns
#                       WHERE column_index BETWEEN ? AND ?''')
#
#     while (newline_index := text.find(b"\n")) == -1:
#         next_text = in_file.read(file_chunk_size)
#
#         # See if we are at the end of the file
#         if next_text:
#             text = text + next_text
#         else:
#             break
#
#     out_text = b""
#
#     for item in text[:newline_index].split(delimiter):
#         out_text += format_string_as_fixed_width(item, cursor.fetchone()["size"])
#
#         if len(out_text) >= file_chunk_size:
#             text_to_write = out_text[:file_chunk_size]
#             out_text = out_text[file_chunk_size:]
#
#     if len(out_text) > 0:
#         out_file.write(out_text)
#
#     return text[(newline_index + 1):]

# def save_fixed_width_line(in_file, text, out_file, delimiter, column_sizes_types_temp_file_path, compressor):
#     conn = connect_sql(column_sizes_types_temp_file_path)
#     cursor = conn.cursor()
#     cursor.execute('''SELECT size
#                       FROM sizes''')
#
#     size_to_read = 100000
#     size_to_write = 100000
#     # column_index = 0
#     line_length = 0
#
#     while (newline_index := text.find(b"\n")) == -1:
#         next_text = in_file.read(size_to_read)
#
#         # See if we are at the end of the file
#         if next_text:
#             text = text + next_text
#         else:
#             break
#
#     out_text = b""
#
#     for item in text[:newline_index].split(delimiter):
#         # out_text += format_string_as_fixed_width(item, column_sizes_dict[str(column_index)])
#         # column_index += 1
#         out_text += format_string_as_fixed_width(item, cursor.fetchone()["size"])
#
#         if len(out_text) >= size_to_write:
#             text_to_write = out_text[:size_to_write]
#             out_text = out_text[size_to_write:]
#
#             if compressor:
#                 text_to_write = compressor.compress(text_to_write)
#
#             line_length += out_file.write(text_to_write)
#
#     if len(out_text) > 0:
#         if compressor:
#             out_text = compressor.compress(out_text)
#
#         line_length += out_file.write(out_text)
#
#     cursor.close()
#     conn.close()
#
#     return line_length, text[(newline_index + 1):]

    # while (newline_index := (next_text := previous_text + in_file.read(chunk_size)).find(b"\n")) == -1:
    #     last_delimiter_index = next_text.rfind(delimiter)
    #
    #     if last_delimiter_index == -1:
    #         previous_text = next_text
    #     else:
    #         previous_text = next_text[(last_delimiter_index + 1):]
    #
    #         for item in next_text[:last_delimiter_index].split(delimiter):
    #             items.append(format_string_as_fixed_width(item, column_sizes_dict[str(len(items))]))
    #
    # for item in next_text[:newline_index].split(delimiter):
    #     items.append(format_string_as_fixed_width(item, column_sizes_dict[str(len(items))]))
    #
    # out_text = b"".join(items)
    # if compressor:
    #     out_text = compressor.compress(out_text)
    #
    # out_file.write(out_text)
    #
    # return len(out_text), next_text[(newline_index + 1):]

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

# def write_index_files(dict_file_path, tmp_dir_path_data, tmp_dir_path_prefix_other):
#     out_file_path = f"{tmp_dir_path_prefix_other}data"
#     if tmp_dir_path_data:
#         out_file_path = f"{tmp_dir_path_data}0"
#
#     max_key_length = 0
#     max_value_length = 0
#
#     with shelve.open(dict_file_path, "r") as the_dict:
#         for key, value in the_dict.items():
#             key_length = len(key)
#             if key_length > max_key_length:
#                 max_key_length = key_length
#
#             value_length = len(value)
#             if value_length > max_value_length:
#                 max_value_length = value_length
#
#     with shelve.open(dict_file_path, "r") as the_dict:
#         with open(out_file_path, "wb") as out_file:
#             for key, value in sorted(the_dict.items()):
#                 out_line = format_string_as_fixed_width(key.encode(), max_key_length) + format_string_as_fixed_width(value.encode(), max_value_length)
#                 out_file.write(out_line)
#
#     write_str_to_file(f"{tmp_dir_path_prefix_other}ll", str(len(out_line)).encode())
#
#     mccl = max(len(str(max_key_length)), len(str(max_key_length + max_value_length)))
#     write_str_to_file(f"{tmp_dir_path_prefix_other}mccl", str(mccl).encode())
#
#     cc = format_string_as_fixed_width(b"0", mccl)
#     cc += format_string_as_fixed_width(str(max_key_length).encode(), mccl)
#     cc += format_string_as_fixed_width(str(max_key_length + max_value_length).encode(), mccl)
#
#     write_str_to_file(f"{tmp_dir_path_prefix_other}cc", cc)

# def write_index_files(dict_file_path, tmp_dir_path_data, tmp_dir_path_prefix_other):
#     out_file_path = f"{tmp_dir_path_prefix_other}data"
#     if tmp_dir_path_data:
#         out_file_path = f"{tmp_dir_path_data}0"
#
#     max_key_length = 0
#     max_value_length = 0
#
#     with shelve.open(dict_file_path, "r") as the_dict:
#         for key, value in the_dict.items():
#             key_length = len(key)
#             if key_length > max_key_length:
#                 max_key_length = key_length
#
#             value_length = len(value)
#             if value_length > max_value_length:
#                 max_value_length = value_length
#
#     with shelve.open(dict_file_path, "r") as the_dict:
#         with open(out_file_path, "wb") as out_file:
#             for key, value in sorted(the_dict.items()):
#                 out_line = format_string_as_fixed_width(key.encode(), max_key_length) + format_string_as_fixed_width(value.encode(), max_value_length)
#                 out_file.write(out_line)
#
#     write_str_to_file(f"{tmp_dir_path_prefix_other}ll", str(len(out_line)).encode())
#
#     mccl = max(len(str(max_key_length)), len(str(max_key_length + max_value_length)))
#     write_str_to_file(f"{tmp_dir_path_prefix_other}mccl", str(mccl).encode())
#
#     cc = format_string_as_fixed_width(b"0", mccl)
#     cc += format_string_as_fixed_width(str(max_key_length).encode(), mccl)
#     cc += format_string_as_fixed_width(str(max_key_length + max_value_length).encode(), mccl)
#
#     write_str_to_file(f"{tmp_dir_path_prefix_other}cc", cc)

# def write_index_files(values_positions, tmp_dir_path_data, tmp_dir_path_prefix_other):
#     column_dict = {}
#     for i in range(len(values_positions[0])):
#         column_dict[i] = [x[i] if isinstance(x[i], bytes) else str(x[i]).encode() for x in values_positions]
#
#     max_lengths = []
#     for i in range(len(values_positions[0])):
#         max_lengths.append(get_max_string_length(column_dict[i]))
#
#     for i in range(len(values_positions[0])):
#         column_dict[i] = format_column_items(column_dict[i], max_lengths[i])
#
#     rows = []
#     for row_num in range(len(column_dict[0])):
#         row_value = b""
#
#         for col_num in sorted(column_dict.keys()):
#             row_value += column_dict[col_num][row_num]
#
#         rows.append(row_value)
#
#     column_coords_string, rows_max_length = save_string_map(rows)
#
#     if tmp_dir_path_data:
#         write_str_to_file(f"{tmp_dir_path_data}0", column_coords_string)
#     else:
#         write_str_to_file(f"{tmp_dir_path_prefix_other}data", column_coords_string)
#
#     column_start_coords = save_column_coords(max_lengths)
#     column_coords_string, max_column_coord_length = save_string_map(column_start_coords)
#     write_str_to_file(f"{tmp_dir_path_prefix_other}cc", column_coords_string)
#     write_str_to_file(f"{tmp_dir_path_prefix_other}mccl", str(max_column_coord_length).encode())
#
#     # Find and write the line length.
#     write_str_to_file(f"{tmp_dir_path_prefix_other}ll", str(rows_max_length).encode())

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
def transpose_lines_to_temp(data_file_path, tmp_dir_path, start_row_index, end_row_index, column_names, column_coords, bigram_size_dict, verbose):
    with initialize(data_file_path) as file_data:
        tmp_file_path = f"{tmp_dir_path}{start_row_index}"
        print_message(f"Transposing lines to {tmp_file_path} for {data_file_path} - start row {start_row_index}, end row {end_row_index}.", verbose)

        with open(tmp_file_path, "wb+") as tmp_file:
            num_rows = end_row_index - start_row_index

            for column_index in range(len(column_names)):
                # Create placeholder space for column values in temp file
                column_coord = column_coords[column_index]
                tmp_file.write(b" " * (column_coord[1] - column_coord[0]) * num_rows)

            tmp_file.flush()

            with mmap(tmp_file.fileno(), 0, prot=PROT_WRITE) as mm_handle:
                # Iterate through each row and save the values in the correct locations in the temp file
                parse_function = get_parse_row_values_function(file_data)

                # Find the start position of each row
                out_line_positions = [0]
                for column_index in range(1, len(column_names)):
                    previous_column_size = column_coords[column_index - 1][1] - column_coords[column_index - 1][0]
                    this_line_position = out_line_positions[-1] + previous_column_size * num_rows
                    out_line_positions.append(this_line_position)

                # Iterate through each row and store the values in the respective column
                for row_index in range(start_row_index, end_row_index):
                    row_values = parse_function(file_data, row_index, column_coords, bigram_size_dict=bigram_size_dict)

                    for column_index, value in enumerate(row_values):
                        out_position = out_line_positions[column_index]
                        column_size = column_coords[column_index][1] - column_coords[column_index][0]
                        value = format_string_as_fixed_width(value, column_size)
                        mm_handle[out_position:(out_position + len(value))] = value
                        out_line_positions[column_index] += len(value)

        return tmp_file_path

# def save_transposed_line_to_temp(data_file_path, tmp_dir_path, chunk_number, col_indices, col_names, col_coords, bigram_size_dict, verbose):
#     col_names = list(col_names)
#
#     with initialize(data_file_path) as file_data:
#         # Initialize the temporary files with each column name
#         print_message(f"Initializing temp files to enable transposing {data_file_path} - chunk {chunk_number}", verbose)
#         for col_index in col_indices:
#             # TODO: It would be better to save in zstandard format. But the Python package
#             #      doesn't support reading line by line.
#             #with gzip.open(f"{tmp_dir_path}{col_index}", "w", compresslevel=1) as tmp_file:
#             with open(f"{tmp_dir_path}{col_index}", "wb") as tmp_file:
#                 tmp_file.write(col_names.pop(0))
#
#         parse_function = get_parse_row_values_function(file_data)
#
#         for row_index in range(file_data.stat_dict["num_rows"]):
#             row_values = parse_function(file_data, row_index, col_coords, bigram_size_dict=bigram_size_dict)
#             # print_message(row_index, verbose)
#             for col_index in col_indices:
#                 #with gzip.open(f"{tmp_dir_path}{col_index}", "a", compresslevel=1) as tmp_file:
#                 with open(f"{tmp_dir_path}{col_index}", "ab") as tmp_file:
#                     tmp_file.write(b"\t" + row_values.pop(0))
#
#         for col_index in col_indices:
#             #with gzip.open(f"{tmp_dir_path}{col_index}", "a", compresslevel=1) as tmp_file:
#             with open(f"{tmp_dir_path}{col_index}", "ab") as tmp_file:
#                 tmp_file.write(b"\n")