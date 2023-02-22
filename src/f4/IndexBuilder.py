import f4
import os

class IndexBuilder:
    #####################################################
    # Class (static) functions
    #####################################################
    # index_columns should be a list. Elements within it can be two-element lists.

    def build_indexes(f4_file_path, index_columns, tmp_dir_path, verbose=False):
        if isinstance(index_columns, str):
            IndexBuilder._build_one_column_index(f4_file_path, index_columns, tmp_dir_path, verbose)
        elif isinstance(index_columns, list):
            for index_column in index_columns:
                if isinstance(index_column, list):
                    if len(index_column) != 2:
                        raise Exception("If you pass a list as an index_column, it must have exactly two elements.")

                    IndexBuilder._build_two_column_index(f4_file_path, index_column[0], index_column[1], tmp_dir_path, verbose)
                else:
                    if not isinstance(index_column, str):
                        raise Exception("When specifying an index column name, it must be a string.")

                    IndexBuilder._build_one_column_index(f4_file_path, index_column, tmp_dir_path, verbose, f4.do_nothing)
        else:
            raise Exception("When specifying index_columns, it must either be a string or a list.")

    # This function is specifically for the EndsWithFilter.
    def build_endswith_index(f4_file_path, index_column, tmp_dir_path, verbose=False):
        IndexBuilder._build_one_column_index(f4_file_path, index_column, tmp_dir_path, verbose, f4.reverse_string)

    def _build_one_column_index(f4_file_path, index_column, tmp_dir_path, verbose, custom_index_function):
        tmp_dir_path = f4.fix_dir_path_ending(tmp_dir_path)
        tmp_dir_path_data = f"{tmp_dir_path}data/"
        tmp_dir_path_other = f"{tmp_dir_path}other/"
        os.makedirs(tmp_dir_path_data, exist_ok=True)
        os.makedirs(tmp_dir_path_other, exist_ok=True)

        # TODO: Add logic to verify that index_column is valid. But where?
        f4.print_message(f"Saving index for {index_column}.", verbose)
        index_column_encoded = index_column.encode()

        with f4.Parser(f4_file_path) as parser:
            f4.print_message(f"Getting column meta information for {index_column} index for {f4_file_path}.", verbose)
            select_columns, column_type_dict, column_coords_dict, decompression_type, decompressor, bigram_size_dict = parser._get_column_meta(set([index_column_encoded]), [])

            line_length = parser._get_stat("ll")
            index_column_type = column_type_dict[index_column_encoded]
            coords = column_coords_dict[index_column_encoded]
            values_positions = []

            decompressor = f4.get_decompressor(decompression_type, decompressor)
            parse_function = parser._get_parse_row_value_function(decompression_type)

            f4.print_message(f"Parsing values and positions for {index_column} index for {f4_file_path}.", verbose)
            for row_index in range(parser.get_num_rows()):
                value = parse_function(row_index, coords, line_length, decompression_type=decompression_type, decompressor=decompressor, bigram_size_dict=bigram_size_dict, column_name=index_column_encoded)
                values_positions.append([value, row_index])

            f4.print_message(f"Building index file for {index_column} index for {f4_file_path}.", verbose)
            IndexBuilder._customize_values_positions(values_positions, [index_column_type], f4.sort_first_column, custom_index_function)

            IndexBuilder._write_index_files(values_positions, tmp_dir_path_data, tmp_dir_path_other)

        index_file_path = IndexBuilder._get_index_file_path(parser.data_file_path, index_column, custom_index_function)
        f4.combine_into_single_file(tmp_dir_path_data, tmp_dir_path_other, index_file_path)

        f4.print_message(f"Done building index file for {index_column} index for {f4_file_path}.", verbose)

    # TODO: Combine this function with the above one and make it generic enough to handle indexes with more columns.
    def _build_two_column_index(f4_file_path, index_column_1, index_column_2, tmp_dir_path, verbose):
        tmp_dir_path = f4.fix_dir_path_ending(tmp_dir_path)
        tmp_dir_path_data = f"{tmp_dir_path}data/"
        tmp_dir_path_other = f"{tmp_dir_path}other/"
        os.makedirs(tmp_dir_path_data, exist_ok=True)
        os.makedirs(tmp_dir_path_other, exist_ok=True)

        if not isinstance(index_column_1, str) or not isinstance(index_column_2, str):
            raise Exception("When specifying an index column name, it must be a string.")

        f4.print_message(f"Saving index for {index_column_1} and {index_column_2}.", verbose)

        index_name = "____".join([index_column_1, index_column_2])
        index_column_1_encoded = index_column_1.encode()
        index_column_2_encoded = index_column_2.encode()

        with f4.Parser(f4_file_path) as parser:
            f4.print_message(f"Getting column meta information for {index_name} index.", verbose)
            select_columns, column_type_dict, column_coords_dict, decompression_type, decompressor, bigram_size_dict = parser._get_column_meta(set([index_column_1_encoded, index_column_2_encoded]), [])
            # TODO: Add logic to verify that index_column_1 and index_column_2 are valid.

            line_length = parser._get_stat("ll")
            index_column_1_type = column_type_dict[index_column_1_encoded]
            index_column_2_type = column_type_dict[index_column_2_encoded]
            coords_1 = column_coords_dict[index_column_1_encoded]
            coords_2 = column_coords_dict[index_column_2_encoded]

            decompressor = f4.get_decompressor(decompression_type, decompressor)
            parse_function = parser._get_parse_row_value_function(decompression_type)

            values_positions = []
            f4.print_message(f"Parsing values and positions for {index_name} index.", verbose)
            for row_index in range(parser.get_num_rows()):
                value_1 = parse_function(row_index, coords_1, line_length, decompression_type=decompression_type, decompressor=decompressor, bigram_size_dict=bigram_size_dict, column_name=index_column_1_encoded)
                value_2 = parse_function(row_index, coords_2, line_length, decompression_type=decompression_type, decompressor=decompressor, bigram_size_dict=bigram_size_dict, column_name=index_column_2_encoded)

                values_positions.append([value_1, value_2, row_index])

            f4.print_message(f"Building index file for {index_name}.", verbose)
            IndexBuilder._customize_values_positions(values_positions, [index_column_1_type, index_column_2_type], f4.sort_first_two_columns, f4.do_nothing)

            IndexBuilder._write_index_files(values_positions, tmp_dir_path_data, tmp_dir_path_other)

        index_file_path = IndexBuilder._get_index_file_path(parser.data_file_path, index_name)
        f4.combine_into_single_file(tmp_dir_path_data, tmp_dir_path_other, index_file_path)

        f4.print_message(f"Done building two-column index file for {index_name}.", verbose)

    def _customize_values_positions(values_positions, column_types, sort_function, custom_index_function):
        # Iterate through each "column" except the last one (which has row_indices) and convert the data.
        for i in range(len(column_types)):
            conversion_function = f4.get_conversion_function(column_types[i])

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
            max_lengths.append(f4.get_max_string_length(column_dict[i]))

        for i in range(len(values_positions[0])):
            column_dict[i] = f4.format_column_items(column_dict[i], max_lengths[i])

        rows = []
        for row_num in range(len(column_dict[0])):
            row_value = b""

            for col_num in sorted(column_dict.keys()):
                row_value += column_dict[col_num][row_num]

            rows.append(row_value)

        column_coords_string, rows_max_length = f4.build_string_map(rows)

        if tmp_dir_path_data:
            f4.write_str_to_file(f"{tmp_dir_path_data}0", column_coords_string)
        else:
            f4.write_str_to_file(f"{tmp_dir_path_prefix_other}data", column_coords_string)

        column_start_coords = f4.get_column_start_coords(max_lengths)
        column_coords_string, max_column_coord_length = f4.build_string_map(column_start_coords)
        f4.write_str_to_file(f"{tmp_dir_path_prefix_other}cc", column_coords_string)
        f4.write_str_to_file(f"{tmp_dir_path_prefix_other}mccl", str(max_column_coord_length).encode())

        # Find and write the line length.
        f4.write_str_to_file(f"{tmp_dir_path_prefix_other}ll", str(rows_max_length).encode())

    def _get_index_file_path(data_file_path, index_name, custom_index_function=f4.do_nothing):
        index_file_path_extension = f".idx_{index_name}"

        if custom_index_function != f4.do_nothing:
            index_file_path_extension = f"{index_file_path_extension}_{custom_index_function.__name__}"

        return f"{data_file_path}{index_file_path_extension}"