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
    tmp_dir_path2, use_checkpoints = prepare_tmp_dir(tmp_dir_path)

    # Compare the arguments passed to this function against any that
    # were saved. This code gives me a dictionary with the parameter
    # names and values.
    if use_checkpoints:
        checkpoint_info = locals().copy()
        checkpoint_file_path = f"{tmp_dir_path2}checkpoint__info"
        if path.exists(checkpoint_file_path):
            prior_checkpoint_info = deserialize(read_str_from_file(checkpoint_file_path))

            if prior_checkpoint_info != checkpoint_info:
                print_message(f"Checkpoint information was found at {checkpoint_file_path}, but it cannot be used because it does not match the arguments specified here.", verbose)
                for file_path in glob(f"{tmp_dir_path2}checkpoint__*"):
                    remove_tmp_file(file_path)

                print_message(f"Saving checkpoint information to {checkpoint_file_path}.")
                write_str_to_file(checkpoint_file_path, serialize(checkpoint_info), False)

    # Parse column info into a database for each chunk.
    joblib.Parallel(n_jobs=num_parallel)(joblib.delayed(parse_column_info)(delimited_file_path, f4_file_path, comment_prefix, delimiter, file_read_chunk_size, chunk_number, chunk_indices[0], chunk_indices[1], tmp_dir_path2, out_items_chunk_size, use_checkpoints, verbose) for chunk_number, chunk_indices in enumerate(column_chunk_indices))

    # Save and format data to a temp file for each column chunk.
    joblib.Parallel(n_jobs=num_parallel)(joblib.delayed(save_formatted_data)(delimited_file_path, f4_file_path, comment_prefix, delimiter, file_read_chunk_size, chunk_number, chunk_indices[0], chunk_indices[1], tmp_dir_path2, out_items_chunk_size, use_checkpoints, verbose) for chunk_number, chunk_indices in enumerate(column_chunk_indices))

    # Combine column databases across the chunks.
    combine_column_databases(delimited_file_path, f4_file_path, column_chunk_indices, tmp_dir_path2, use_checkpoints, verbose)

    # Create meta files for all columns.
    save_column_name_info(delimited_file_path, f4_file_path, out_items_chunk_size, tmp_dir_path2, use_checkpoints, verbose)
    save_column_types(delimited_file_path, f4_file_path, out_items_chunk_size, tmp_dir_path2, use_checkpoints, verbose)
    save_column_coordinates(delimited_file_path, f4_file_path, out_items_chunk_size, tmp_dir_path2, use_checkpoints, verbose)

    # Merge the saved/formatted data across the column chunks.
    combine_data_for_column_chunks(delimited_file_path, f4_file_path, column_chunk_indices, tmp_dir_path2, use_checkpoints, verbose)

    num_rows = int(read_str_from_file(f"{tmp_dir_path2}num_rows"))
    line_length_total = int(read_str_from_file(f"{tmp_dir_path2}line_length_total"))

    if num_rows == 0:
        raise Exception(f"A header row but no data rows were detected in {delimited_file_path}.")

    if index_columns:
        build_indexes(f4_file_path, tmp_dir_path2, index_columns, num_rows, line_length_total, num_parallel, get_columns_database_file_path(tmp_dir_path2), use_checkpoints, verbose)

    #TODO: Parallelize this by row chunks.
    if compression_type:
        compress_data(delimited_file_path, f4_file_path, tmp_dir_path2, compression_type, num_rows, line_length_total, use_checkpoints, verbose)
    # else:
    #     # The combined file will be compressed, so we need to decompress it.
    #     rename(get_data_path(tmp_dir_path2, "data"), get_data_path(tmp_dir_path2, "datacmpr"))
    #
    #     with open_temp_file(get_data_path(tmp_dir_path2, "datacmpr"), "rb") as cmpr_file:
    #         with open(get_data_path(tmp_dir_path2, "data"), "wb") as data_file:
    #             chunk_size = 1000000
    #             while True:
    #                 content = cmpr_file.read(chunk_size)
    #                 # If the content is empty, end of file has been reached
    #                 if not content:
    #                     break
    #
    #                 data_file.write(content)
    combine_into_single_file(delimited_file_path, f4_file_path, tmp_dir_path2, file_read_chunk_size, verbose)

    remove_tmp_file(get_columns_database_file_path(tmp_dir_path2))

    # Remove checkpoint files because we successfully built the file.
    for file_path in glob(f"{tmp_dir_path2}checkpoint__*"):
        remove_tmp_file(file_path)

    remove_tmp_file(f"{tmp_dir_path2}num_rows")
    remove_tmp_file(f"{tmp_dir_path2}line_length_total")

    # Only remove the temp directory if we created it.
    if not tmp_dir_path:
        rmtree(tmp_dir_path2)

    print_message(f"Done converting {delimited_file_path} to {f4_file_path}.", verbose)

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

        # in_file.seek(current_index + newline_index + 1)

    num_cols = current_column_index + 1

    if num_cols == 0:
        raise Exception(f"No data was detected in {delimited_file_path}.")

    return num_cols, max_column_name_length

def get_columns_database_file_path(tmp_dir_path, chunk_number=-1):
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

        # in_file.seek(current_index + newline_index + 1)

    if len(save_tuples) > 0:
        cursor.executemany(sql, save_tuples)

# This function is executed in parallel.
def parse_column_info(delimited_file_path, f4_file_path, comment_prefix, delimiter, file_read_chunk_size, chunk_number, start_column_index, end_column_index, tmp_dir_path, out_items_chunk_size, use_checkpoints, verbose):
    if has_checkpoint_been_reached_previously(use_checkpoints, tmp_dir_path, chunk_number, verbose):
        return

    print_message(f"Parsing column names, sizes, and types when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose)

    columns_file_path = get_columns_database_file_path(tmp_dir_path, chunk_number)
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
        skip_line(in_file) # Header line

        # Loop through the file for the specified columns and update the dictionaries.
        save_tuples = []
        for column_index, value in iterate_delimited_file_column_indices(in_file, delimiter, file_read_chunk_size, start_column_index, end_column_index):
            if column_index == start_column_index:
                num_rows += 1

                print_message(f"Parsing column names, sizes, and types when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose, num_rows)

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

    record_checkpoint_reached(use_checkpoints, tmp_dir_path, chunk_number)

    print_message(f"Done parsing column names, sizes, and types when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose)

# This function is executed in parallel.
def save_formatted_data(delimited_file_path, f4_file_path, comment_prefix, delimiter, file_read_chunk_size, chunk_number, start_column_index, end_column_index, tmp_dir_path, out_items_chunk_size, use_checkpoints, verbose):
    if has_checkpoint_been_reached_previously(use_checkpoints, tmp_dir_path, chunk_number, verbose):
        return

    print_message(f"Saving formatted data when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose)

    columns_file_path = get_columns_database_file_path(tmp_dir_path, chunk_number)
    conn = connect_sql(columns_file_path)
    cursor = conn.cursor()

    cursor.execute('''SELECT SUM(size) AS size
                      FROM columns
                      WHERE column_index BETWEEN ? AND ?''', (start_column_index, end_column_index,))
    line_length = cursor.fetchone()["size"]

    write_str_to_file(get_data_path(tmp_dir_path, "ll", chunk_number), str(line_length).encode(), False)

    num_columns_to_parse = end_column_index - start_column_index
    data_value_count = 0

    with get_delimited_file_handle(delimited_file_path) as in_file:
        skip_comments(in_file, comment_prefix)
        skip_line(in_file)  # Header line

        # Save data.
        with open_temp_file_to_compress(get_data_path(tmp_dir_path, "data", chunk_number)) as data_file:
            out_list = []

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

                data_value_count += 1
                print_message(f"Saving formatted data for when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose, data_value_count / num_columns_to_parse)

                if len(out_list) == out_items_chunk_size:
                    data_file.write(b"".join(out_list))
                    out_list = []

            if len(out_list) > 0:
                data_file.write(b"".join(out_list))

    cursor.close()
    conn.close()

    record_checkpoint_reached(use_checkpoints, tmp_dir_path, chunk_number)

    print_message(f"Done saving formatted data when converting {delimited_file_path} to {f4_file_path} for columns {start_column_index} - {end_column_index - 1}.", verbose)

def combine_column_databases(delimited_file_path, f4_file_path, column_chunk_indices, tmp_dir_path, use_checkpoints, verbose):
    if has_checkpoint_been_reached_previously(use_checkpoints, tmp_dir_path, 0, verbose):
        return

    # Combine the column sizes and types across the chunks.
    # We won't add the num_* columns to the combined file because they are no longer necessary.
    print_message(f"Combining column databases when converting {delimited_file_path} to {f4_file_path}.", verbose)

    conn_0 = connect_sql(get_columns_database_file_path(tmp_dir_path, 0))
    cursor_0 = conn_0.cursor()

    for chunk_number in range(1, len(column_chunk_indices)):
        chunk_file_path = get_columns_database_file_path(tmp_dir_path, chunk_number)
        cursor_0.execute(f"ATTACH DATABASE '{chunk_file_path}' AS db_chunk")
        cursor_0.execute(f"INSERT INTO columns (column_index, column_name, size, inferred_type) SELECT column_index, column_name, size, inferred_type FROM db_chunk.columns")
        cursor_0.execute("DETACH DATABASE db_chunk")

    cursor_0.close()
    conn_0.close()

    record_checkpoint_reached(use_checkpoints, tmp_dir_path, 0)

    rename(get_columns_database_file_path(tmp_dir_path, 0), get_columns_database_file_path(tmp_dir_path))

    for chunk_number in range(1, len(column_chunk_indices)):
        remove_tmp_file(get_columns_database_file_path(tmp_dir_path, chunk_number))

def get_max_column_length(cursor, column_name):
    sql = f'''SELECT MAX(LENGTH(CAST({column_name} AS TEXT))) AS max_length
              FROM columns'''

    cursor.execute(sql)
    return cursor.fetchone()["max_length"]

def save_column_name_info(delimited_file_path, f4_file_path, out_items_chunk_size, tmp_dir_path, use_checkpoints, verbose):
    if has_checkpoint_been_reached_previously(use_checkpoints, tmp_dir_path, 0, verbose):
        return

    print_message(f"Saving column name info when converting {delimited_file_path} to {f4_file_path}.", verbose)

    conn = connect_sql(get_columns_database_file_path(tmp_dir_path))
    cursor = conn.cursor()

    max_column_name_length = get_max_column_length(cursor, "column_name")
    max_column_index_length = get_max_column_length(cursor, "column_index")

    ##########################################################
    # cni = column name first, sorted by column name
    ##########################################################

    cniccml = max(len(str(max_column_name_length)), len(str(max_column_name_length + max_column_index_length)))
    write_str_to_file(get_data_path(tmp_dir_path, "cniccml"), str(cniccml).encode())

    cnicc = format_string_as_fixed_width(b"0", cniccml)
    cnicc += format_string_as_fixed_width(str(max_column_name_length).encode(), cniccml)
    cnicc += format_string_as_fixed_width(str(max_column_name_length + max_column_index_length).encode(), cniccml)
    write_str_to_file(get_data_path(tmp_dir_path, "cnicc"), cnicc)

    cni_file_original_size = 0
    with open_temp_file_to_compress(get_data_path(tmp_dir_path, "cni")) as data_file:
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
            cni_file_original_size += data_file.write(b"".join(out_list))

    write_temp_file_original_size(get_data_path(tmp_dir_path, "cni"), cni_file_original_size)

    cn_file_original_size = 0
    with open_temp_file_to_compress(get_data_path(tmp_dir_path, "cn")) as data_file:
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
                cn_file_original_size += data_file.write(b"\n")

            cn_file_original_size += data_file.write(b"\n".join(out_list))

    cursor.close()
    conn.close()

    write_temp_file_original_size(get_data_path(tmp_dir_path, "cn"), cn_file_original_size)

    record_checkpoint_reached(use_checkpoints, tmp_dir_path, 0)

def save_column_types(delimited_file_path, f4_file_path, out_items_chunk_size, tmp_dir_path, use_checkpoints, verbose):
    if has_checkpoint_been_reached_previously(use_checkpoints, tmp_dir_path, 0, verbose):
        return

    print_message(f"Saving column type and size info when converting {delimited_file_path} to {f4_file_path}.", verbose)

    conn = connect_sql(get_columns_database_file_path(tmp_dir_path))
    cursor = conn.cursor()

    max_type_length = 1 # This should always be 1 unless we add a bunch of types.

    ct_file_original_size = 0
    with open_temp_file_to_compress(get_data_path(tmp_dir_path, "ct")) as data_file:
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

            ct_file_original_size += data_file.write(b"".join(out_list))

    cursor.close()
    conn.close()

    write_temp_file_original_size(get_data_path(tmp_dir_path, "ct"), ct_file_original_size)

    record_checkpoint_reached(use_checkpoints, tmp_dir_path, 0)

def save_column_coordinates(delimited_file_path, f4_file_path, out_items_chunk_size, tmp_dir_path, use_checkpoints, verbose):
    if has_checkpoint_been_reached_previously(use_checkpoints, tmp_dir_path, 0, verbose):
        return

    print_message(f"Saving column coordinates when converting {delimited_file_path} to {f4_file_path}.", verbose)

    conn = connect_sql(get_columns_database_file_path(tmp_dir_path))
    cursor = conn.cursor()

    sql = f'''SELECT SUM(size) AS size
              FROM columns'''
    cursor.execute(sql)
    max_coord_length = len(str(cursor.fetchone()["size"]))

    cc_file_original_size = 0
    with open_temp_file_to_compress(get_data_path(tmp_dir_path, "cc")) as data_file:
        sql = f'''SELECT size AS size
                  FROM columns
                  ORDER BY column_index'''
        cursor.execute(sql)

        coord = 0

        cc_file_original_size += data_file.write(format_string_as_fixed_width(str(coord).encode(), max_coord_length))

        while True:
            batch = cursor.fetchmany(out_items_chunk_size)

            if not batch:
                break

            out_list = []
            for row in batch:
                coord += row["size"]
                out_list.append(format_string_as_fixed_width(str(coord).encode(), max_coord_length))

            cc_file_original_size += data_file.write(b"".join(out_list))

    cursor.close()
    conn.close()

    write_temp_file_original_size(get_data_path(tmp_dir_path, "cc"), cc_file_original_size)

    write_str_to_file(get_data_path(tmp_dir_path, "ccml"), str(max_coord_length).encode())

    record_checkpoint_reached(use_checkpoints, tmp_dir_path, 0)

def combine_data_for_column_chunks(delimited_file_path, f4_file_path, column_chunk_indices, tmp_dir_path, use_checkpoints, verbose):
    if has_checkpoint_been_reached_previously(use_checkpoints, tmp_dir_path, 0, verbose):
        return

    print_message(f"Combining data for column chunks when converting {delimited_file_path} to {f4_file_path}.", verbose)

    file_handles = {}
    for chunk_number in range(len(column_chunk_indices)):
        file_handles[chunk_number] = open_temp_file_compressed(get_data_path(tmp_dir_path, "data", chunk_number))

    line_lengths = {}
    for chunk_number in range(len(column_chunk_indices)):
        line_length_file_path = get_data_path(tmp_dir_path, "ll", chunk_number)
        line_lengths[chunk_number] = int(read_str_from_file(line_length_file_path))

    line_length_total = sum(line_lengths.values())

    out_file_original_size = 0
    with open_temp_file_to_compress(get_data_path(tmp_dir_path, "data")) as out_file:
        num_rows = 0
        while line_0 := file_handles[0].read(line_lengths[0]):
            num_rows += 1
            out_file_original_size += out_file.write(line_0)

            for chunk_number in range(1, len(column_chunk_indices)):
                out_file_original_size += out_file.write(file_handles[chunk_number].read(line_lengths[chunk_number]))

    write_temp_file_original_size(get_data_path(tmp_dir_path, "data"), out_file_original_size)

    record_checkpoint_reached(use_checkpoints, tmp_dir_path, 0)

    for chunk_number in range(len(column_chunk_indices)):
        remove_tmp_file(get_data_path(tmp_dir_path, "ll", chunk_number))
        remove_tmp_file(get_data_path(tmp_dir_path, "data", chunk_number))

    # We save these numbers to files so we can retrieve them when checkpoints are used.
    write_str_to_file(f"{tmp_dir_path}num_rows", str(num_rows).encode(), False)
    write_str_to_file(f"{tmp_dir_path}line_length_total", str(line_length_total).encode(), False)

def build_indexes(f4_file_path, tmp_dir_path, index_columns, num_rows, line_length, num_parallel, columns_database_file_path, use_checkpoints, verbose=False):
    if has_checkpoint_been_reached_previously(use_checkpoints, tmp_dir_path, 0, verbose):
        return

    index_info_dict = {}

    if isinstance(index_columns, str):
        index_columns, reverse_status_dict = check_index_column_reverse_status([index_columns])

        build_index(f4_file_path, tmp_dir_path, 0, index_columns, reverse_status_dict, num_rows, line_length, columns_database_file_path, verbose)
        index_info_dict[(index_columns, reverse_status_dict[index_columns])] = 0
    elif isinstance(index_columns, list):
        # Verify whether the index_columns are valid.
        # TODO: Move this logic earlier in the overall process so it fails sooner.
        for i, index_column in enumerate(index_columns):
            if not isinstance(index_column, str) and not isinstance(index_column, list):
                raise Exception("When specifying index columns, they must either be a string or a list.")

            if "|" in index_column:
                raise Exception("You may not index a column with a vertical bar (|) in its name.")

        keys = joblib.Parallel(n_jobs=num_parallel)(
            joblib.delayed(build_index_parallel)(f4_file_path, tmp_dir_path, num_rows, line_length, i, index_column, columns_database_file_path, verbose)
            for i, index_column in enumerate(index_columns)
        )

        for i, key in enumerate(keys):
            index_info_dict[key] = i
    else:
        raise Exception("When specifying index columns, they must either be a string or a list.")

    write_str_to_file(f"{tmp_dir_path}i", serialize(index_info_dict))

    record_checkpoint_reached(use_checkpoints, tmp_dir_path, 0)

def build_index(f4_file_path, tmp_dir_path, index_number, index_columns, reverse_status_dict, num_rows, line_length, columns_database_file_path, verbose):
    out_index_file_path_prefix = f"{tmp_dir_path}i{index_number}"
    ccml = fast_int(read_str_from_file(get_data_path(tmp_dir_path, "ccml")))

    print_message(f"Saving index information for {', '.join(index_columns)} column(s) in {f4_file_path}.", verbose)

    # Collect other information about each column.
    conn = connect_sql(columns_database_file_path)
    sql = f'''SELECT column_index, TRIM(column_name) AS column_name, inferred_type
    FROM columns
    WHERE TRIM(column_name) IN ("{'", "'.join(index_columns)}")
    ORDER BY column_index'''

    index_columns_index_dict = {}
    index_columns_name_dict = {}

    for row in query_sql(conn, sql):
        column_name = row["column_name"]
        column_index = row["column_index"]

        index_columns_index_dict[column_index] = {}
        index_columns_index_dict[column_index]["column_name"] = column_name
        index_columns_index_dict[column_index]["reverse_status"] = reverse_status_dict[column_name]
        index_columns_index_dict[column_index]["type"] = row["inferred_type"]

        start_coord, end_coord = get_column_index_coords(tmp_dir_path, column_index, ccml)
        index_columns_index_dict[column_index]["start_coord"] = start_coord
        index_columns_index_dict[column_index]["end_coord"] = end_coord
        index_columns_index_dict[column_index]["max_value_length"] = 0

        index_columns_name_dict[column_name] = column_index

    conn.close()

    index_column_indexes_sorted = sorted(list(index_columns_index_dict.keys()))

    sql_create_table = f'CREATE TABLE index_data (index_column{index_column_indexes_sorted[0]} TEXT NOT NULL'
    for i in range(1, len(index_column_indexes_sorted)):
        sql_create_table += f", index_column{index_column_indexes_sorted[i]} TEXT NOT NULL"
    sql_create_table += ")"

    index_database_file_path = f"{out_index_file_path_prefix}.db"
    conn = connect_sql(index_database_file_path)
    execute_sql(conn, sql_create_table)

    non_committed_values = []
    cursor = conn.cursor()
    cursor.execute('BEGIN TRANSACTION')

    sql_insert = f'''INSERT INTO index_data ({', '.join([f"index_column{index_column_indexes_sorted[i]}" for i in range(len(index_column_indexes_sorted))])})
                     VALUES ({', '.join(['?' for x in index_column_indexes_sorted])})'''

    with open_temp_file_compressed(get_data_path(tmp_dir_path, "data")) as data_file:
        for row_index in range(num_rows):
            start_pos = row_index * line_length

            values = []
            for index_column_index in index_column_indexes_sorted:
                this_start_pos = start_pos + index_columns_index_dict[index_column_index]["start_coord"]
                this_end_pos = start_pos + index_columns_index_dict[index_column_index]["end_coord"]
                data_file.seek(this_start_pos)
                value = data_file.read(this_end_pos - this_start_pos)

                if index_columns_index_dict[index_column_index]["reverse_status"]:
                    value = reverse_string(value)

                values.append(value)
                index_columns_index_dict[index_column_index]["max_value_length"] = max(index_columns_index_dict[index_column_index]["max_value_length"], len(value))

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

    index_data_file_original_size = 0
    with open_temp_file_to_compress(out_index_file_path_prefix) as index_data_file:
        sql_query = f'''SELECT rowid - 1 AS rowid, {', '.join([f"index_column{index_columns_name_dict[x]}" for x in index_columns])}
                        FROM index_data
                        ORDER BY '''

        for i, index_column in enumerate(index_columns):
            index_column_index = index_columns_name_dict[index_column]
            index_column_type = index_columns_index_dict[index_column_index]["type"]

            if i > 0:
                sql_query += ", "

            if index_column_type == "s":
                sql_query += f"index_column{index_column_index}"
            elif index_column_type == "i":
                sql_query += f"CAST(index_column{index_column_index} AS INTEGER)"
            else:
                sql_query += f"CAST(index_column{index_column_index} AS REAL)"

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

                for index_column in index_columns:
                    index_column_index = index_columns_name_dict[index_column]
                    out_row += format_string_as_fixed_width(row[f"index_column{index_column_index}"], index_columns_index_dict[index_column_index]["max_value_length"])

                batch_out.append(out_row + format_string_as_fixed_width(str(row["rowid"]).encode(), max_row_index_length))
                row_index += 1

            index_data_file_original_size += index_data_file.write(b"".join(batch_out))

        cursor.close()
    conn.close()

    write_temp_file_original_size(out_index_file_path_prefix, index_data_file_original_size)

    print_message(f"Done querying temporary database when indexing the {', '.join(index_columns)} column(s) in {f4_file_path}.", verbose)

    coords = [0]
    for index_column in index_columns:
        index_column_index = index_columns_name_dict[index_column]
        coords.append(coords[-1] + index_columns_index_dict[index_column_index]["max_value_length"])
    coords.append(coords[-1] + max_row_index_length)
    coords = [str(x).encode() for x in coords]

    ccml = max([len(x) for x in coords])
    write_str_to_file(f"{out_index_file_path_prefix}ccml", str(ccml).encode())

    cc = b""
    for x in coords:
        cc += format_string_as_fixed_width(x, ccml)
    write_str_to_file(f"{out_index_file_path_prefix}cc", cc)

    remove_tmp_file(index_database_file_path)

    print_message(f"Done building index for {', '.join(index_columns)} column(s) in {f4_file_path}.", verbose)

def build_index_parallel(f4_file_path, tmp_dir_path, num_rows, line_length, index_number, index_column, columns_database_file_path, verbose):
    index_column_list = [index_column] if isinstance(index_column, str) else index_column
    index_column_list, reverse_status_dict = check_index_column_reverse_status(index_column_list)

    build_index(f4_file_path, tmp_dir_path, index_number, index_column_list, reverse_status_dict, num_rows, line_length, columns_database_file_path, verbose)

    key = []
    for index_column in index_column_list:
        key.append((index_column, reverse_status_dict[index_column]))

    return tuple(key)

def check_index_column_reverse_status(index_columns):
    reverse_status_dict = {}

    for i, index_column in enumerate(index_columns):
        if index_column.endswith("_endswith"):
            index_columns[i] = index_column.rstrip("_endswith")
            reverse_status_dict[index_columns[i]] = True
        else:
            reverse_status_dict[index_column] = False

    return index_columns, reverse_status_dict

def get_column_index_coords(tmp_dir_path, index_column_index, ccml):
    with open_temp_file_compressed(get_data_path(tmp_dir_path, "cc")) as file_handle:
        # with mmap(file_handle.fileno(), 0, prot=PROT_READ) as mmap_handle:
        pos_a = index_column_index * ccml
        pos_b = pos_a + ccml
        pos_c = pos_b + ccml

        # return fast_int(mmap_handle[pos_a:pos_b]), fast_int(mmap_handle[pos_b:pos_c])
        file_handle.seek(pos_a)
        coord1 = file_handle.read(pos_b - pos_a)
        file_handle.seek(pos_b)
        coord2 = file_handle.read(pos_c - pos_b)

        return fast_int(coord1), fast_int(coord2)

def compress_data(delimited_file_path, f4_file_path, tmp_dir_path, compression_type, num_rows, line_length, use_checkpoints, verbose):
    if has_checkpoint_been_reached_previously(use_checkpoints, tmp_dir_path, 0, verbose):
        return

    print_message(f"Compressing data for {delimited_file_path} to {f4_file_path}.", verbose)

    # For now, we assume z-standard compression.
    compressor = ZstdCompressor(level=1)

    # Compress the data.
    # TODO: If necessary, chunk the compression so we can handle extremely wide files.
    compressed_row_ends = []
    current_compressed_row_end = 0
    compressed_lines_to_save = []
    compressed_chars_not_saved = 0

    with open_temp_file_compressed(get_data_path(tmp_dir_path, "data")) as file_handle:
        cmpr_file_original_size = 0

        with open_temp_file_to_compress(get_data_path(tmp_dir_path, "cmpr")) as cmpr_file:
            with open_temp_file_to_compress(get_data_path(tmp_dir_path, "re_tmp")) as re_tmp_file:
                for row_i in range(num_rows):
                    row_start = row_i * line_length
                    row_end = (row_i + 1) * line_length

                    file_handle.seek(row_start)
                    row = file_handle.read(row_end - row_start)
                    compressed_line = compressor.compress(row)
                    compressed_row_length = len(compressed_line)

                    current_compressed_row_end += compressed_row_length
                    compressed_row_ends.append(current_compressed_row_end)
                    compressed_lines_to_save.append(compressed_line)
                    compressed_chars_not_saved += compressed_row_length

                    if compressed_chars_not_saved >= 1000000:
                        cmpr_file_original_size += cmpr_file.write(b"".join(compressed_lines_to_save))

                        re_tmp_file.write(b"\n".join([str(rl).encode() for rl in compressed_row_ends]))
                        if row_i != (num_rows - 1):
                            re_tmp_file.write(b"\n")

                        mrel = len(str(compressed_row_ends[-1]))
                        compressed_row_ends = []
                        compressed_lines_to_save = []
                        compressed_chars_not_saved = 0

                if compressed_chars_not_saved > 0:
                    cmpr_file_original_size += cmpr_file.write(b"".join(compressed_lines_to_save))
                    re_tmp_file.write(b"\n".join([str(rl).encode() for rl in compressed_row_ends]))

    if len(compressed_row_ends) > 0:
        mrel = len(str(compressed_row_ends[-1]))

    write_str_to_file(get_data_path(tmp_dir_path, "mrel"), str(mrel).encode())

    re_file_original_size = 0
    with open_temp_file_to_compress(get_data_path(tmp_dir_path, "re")) as re_file:
        for line in read_compressed_file_line_by_line(get_data_path(tmp_dir_path, "re_tmp")):
            row_end = line.rstrip(b"\n")
            re_file_original_size += re_file.write(format_string_as_fixed_width(row_end, mrel))

    write_temp_file_original_size(get_data_path(tmp_dir_path, "re"), re_file_original_size)

    record_checkpoint_reached(use_checkpoints, tmp_dir_path, 0)

    rename(get_data_path(tmp_dir_path, "cmpr"), get_data_path(tmp_dir_path, "data"))
    write_temp_file_original_size(get_data_path(tmp_dir_path, "data"), cmpr_file_original_size)

    write_str_to_file(get_data_path(tmp_dir_path, "ll"), str(line_length).encode())
    write_str_to_file(get_data_path(tmp_dir_path, "nrow"), str(num_rows).encode())
    write_str_to_file(get_data_path(tmp_dir_path, "cmpr"), b"z")

    remove_tmp_file(get_data_path(tmp_dir_path, "re_tmp"))

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
        file_name = path.basename(file_path)
        if file_path.endswith("__original_size") or file_name.startswith("checkpoint__") or file_name == "columns.db" or file_name == "num_rows" or file_name == "line_length_total":
            continue

        extension = path.basename(file_path)
        extension = "" if extension == "data" else extension
        # file_size = path.getsize(file_path)
        file_size = get_temp_file_original_size(file_path)

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

            with open_temp_file_compressed(file_path) as component_file:
                while chunk := component_file.read(read_chunk_size):
                    f4_file.write(chunk)

            remove_tmp_file(file_path)
            remove_tmp_file(f"{file_path}__original_size")

def skip_comments(in_file, comment_prefix):
    if comment_prefix is None:
        return

    next_text = in_file.read(len(comment_prefix))

    while next_text == comment_prefix:
        skip_line(in_file)
        next_text = in_file.read(len(comment_prefix))

    in_file.seek(in_file.tell() - len(next_text))

def skip_line(in_file):
    while True:
        next_char = in_file.read(1)
        if next_char == b"\n":
            break

# This function is slow when working with gzipped files because of the backwards seek(). Only use it sparingly.
# def skip_line(in_file):
#     chunk_size = 100000
#     current_index = in_file.tell()
#
#     while (newline_index := (next_text := in_file.read(chunk_size)).find(b"\n")) == -1:
#         current_index += len(next_text)
#
#     in_file.seek(current_index + newline_index + 1)

# def skip_lines(in_file, num_lines_to_skip):
#     if num_lines_to_skip <= 0:
#         return
#
#     chunk_size = 100000
#     num_lines_skipped = 0
#
#     while next_text := in_file.read(chunk_size):
#         how_far_to_reverse = len(next_text)
#
#         while (newline_index := next_text.find(b"\n")) > -1:
#             num_lines_skipped += 1
#             how_far_to_reverse -= newline_index + 1
#
#             if num_lines_skipped == num_lines_to_skip:
#                 in_file.seek(in_file.tell() - how_far_to_reverse)
#                 return
#             else:
#                 next_text = next_text[newline_index + 1:]

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
                    yield current_column_index, item.strip(b" ")

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
