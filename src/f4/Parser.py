from .Utilities import *

class Parser:
    """
    This class is used for querying F4 files and saving the output to new files.

    Args:
        data_file_path (str): The path to an existing F4 file.

    Attributes:
        data_file_path (str): The path to an existing F4 file.
    """
    def __init__(self, data_file_path):
        if not isinstance(data_file_path, str):
            raise Exception("You must specify data_file_path as an str value.")

        self.data_file_path = data_file_path
        self.__file_handle = open_read_file(self.data_file_path)

        file_map_length_string = self.__file_handle.readline()
        file_map_length = fastnumbers.fast_int(file_map_length_string.rstrip(b"\n"))
        self.__file_map_dict = deserialize(self.__file_handle[len(file_map_length_string):(len(file_map_length_string) + file_map_length)])

        # Invert the abbreviations used for file keys in the dictionary so they are more human readable.
        for file_name, abbreviation in FILE_KEY_ABBREVIATIONS.items():
            if abbreviation in self.__file_map_dict:
                self.__file_map_dict[file_name] = self.__file_map_dict[abbreviation]
                del self.__file_map_dict[abbreviation]

        # Cache statistics in a dictionary.
        self.__stats = {}

        data_size =  self.__file_map_dict["data"][1] - self.__file_map_dict["data"][0]
        self.__stats["nrow"] = fastnumbers.fast_int(data_size / self._get_stat("ll"))

        cc_size = self.__file_map_dict["cc"][1] - self.__file_map_dict["cc"][0]
        self.__stats["ncol"] = fastnumbers.fast_int(cc_size / self._get_stat("mccl")) - 1

    def __enter__(self):
        return self

    def __exit__(self, the_type, value, traceback):
        self.__file_handle.close()

    def query_and_write(self, fltr, select_columns, out_file_path=None, out_file_type="tsv", num_processes=1, lines_per_chunk=10):
        """
        Query the data file using zero or more filters.

        This function accepts filtering criteria, identifies matching rows,
        and writes the output (for select columns) to an output file or standard output.

        Args:
            fltr (BaseFilter): A filter.
            select_columns (list): A list of strings that indicate the names of columns that should be selected. If this is an empty list, all columns will be selected.
            out_file_path(str): A path to a file that will store the output data. If None is specified, the data will be directed to standard output.
            out_file_type (str): The output file type. Currently, the only supported value is tsv.
        """
        if not fltr:
            raise Exception("A filter must be specified.")

        if not isinstance(fltr, NoFilter):
            raise Exception("An object that inherits from NoFilter must be specified.")

        if out_file_type != "tsv":
            raise Exception("The only out_file_type currently supported is tsv.")

        if select_columns:
            if not isinstance(select_columns, list):
                raise Exception("You must specify select_column as a list.")
        else:
            select_columns = []

        # Store column indices and types in dictionaries so we only have to retrieve
        # each once, even if we use the same column in multiple filters.
        select_columns, column_type_dict, column_coords_dict, decompression_type, decompressor, bigram_size_dict = self._get_column_meta(fltr._get_column_name_set(), select_columns)

        fltr._check_types(column_type_dict)

        has_index = len(glob.glob(self.data_file_path + ".idx_*")) > 0

        if has_index:
#TODO: Remove this stuff if we don't need it after testing on huge files.
#            sub_filters = fltr.get_sub_filters()

#            if num_processes == 1 or len(sub_filters) == 1:
            keep_row_indices = sorted(fltr._filter_indexed_column_values(self.data_file_path, self.get_num_rows(), num_processes))
#            else:
#                fltr_results_dict = {}

##                for f in sub_filters:
##                    fltr_results_dict[str(f)] = f.filter_indexed_column_values(self.data_file_path, self.compression_level, column_index_dict, column_type_dict, column_coords_dict, self.get_num_rows(), num_processes)

                # This is a parallelization of the above code.
                # At least in some cases, it slows things down more than it speeds things up.
#                fltr_results = Parallel(n_jobs = num_processes)(delayed(f.filter_indexed_column_values)(self.data_file_path, self.compression_level, column_index_dict, column_type_dict, column_coords_dict, self.get_num_rows(), num_processes) for f in sub_filters)
#                for i in range(len(sub_filters)):
#                    fltr_results_dict[str(sub_filters[i])] = fltr_results[i]
#
#                keep_row_indices = sorted(fltr.filter_indexed_column_values_parallel(fltr_results_dict))
        else:
            if num_processes == 1:
                row_indices = set(range(self.get_num_rows()))
                keep_row_indices = sorted(fltr._filter_column_values(self.data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict))
            else:
                # Loop through the rows in parallel and find matching row indices.
                keep_row_indices = sorted(chain.from_iterable(Parallel(n_jobs = num_processes)(delayed(fltr._filter_column_values)(self.data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict) for row_indices in self._generate_row_chunks(num_processes))))

        select_column_coords = [column_coords_dict[name] for name in select_columns]

        # This avoids having to check the decompression type each time we parse a value.
        decompressor = get_decompressor(decompression_type, decompressor)
        parse_function = self._get_parse_row_values_function(decompression_type)

        if out_file_path:
            # Write output (in chunks)
            with open(out_file_path, 'wb') as out_file:
                out_file.write(b"\t".join(select_columns) + b"\n") # Header line

                out_lines = []
                for row_index in keep_row_indices:
                    out_values = parse_function(row_index, select_column_coords, decompression_type=decompression_type, decompressor=decompressor, bigram_size_dict=bigram_size_dict, column_names=select_columns)
                    out_lines.append(b"\t".join(out_values))

                    if len(out_lines) % lines_per_chunk == 0:
                        out_file.write(b"\n".join(out_lines) + b"\n")
                        out_lines = []

                if len(out_lines) > 0:
                    out_file.write(b"\n".join(out_lines) + b"\n")
        else:
            sys.stdout.buffer.write(b"\t".join(select_columns) + b"\n") # Header line

            for row_index in keep_row_indices:
                out_values = parse_function(row_index, select_column_coords, decompression_type=decompression_type, decompressor=decompressor, bigram_size_dict=bigram_size_dict, column_names=select_columns)
                sys.stdout.buffer.write(b"\t".join(out_values))

                if row_index != keep_row_indices[-1]:
                    sys.stdout.buffer.write(b"\n")

    def head(self, n = 10, select_columns=None, out_file_path=None, out_file_type="tsv"):
        if not select_columns:
            select_columns = []
        self.query_and_write(HeadFilter(n, select_columns), select_columns, out_file_path=out_file_path, out_file_type=out_file_type)

    def tail(self, n = 10, select_columns=None, out_file_path=None, out_file_type="tsv"):
        if not select_columns:
            select_columns = []
        self.query_and_write(TailFilter(n, select_columns), select_columns, out_file_path=out_file_path, out_file_type=out_file_type)

    def get_num_rows(self):
        return self._get_stat("nrow")

    def get_num_cols(self):
        return self._get_stat("ncol")

    def get_column_type_from_name(self, column_name):
        try:
            return self._get_column_type_from_index(self._get_column_index_from_name(column_name))
        except:
            raise Exception(f"A column with the name {column_name} does not exist.")

    ##############################################
    # Non-public functions
    ##############################################

    def _get_column_index_from_name(self, column_name):
        position = IndexSearcher._get_identifier_row_index(self, column_name.encode(), self.get_num_cols(), data_prefix="cn")

        if position < 0:
            raise Exception(f"Could not retrieve index because column named {column_name} was not found.")

        return position

    def _get_stat(self, stat):
        if stat not in self.__stats:
            self.__stats[stat] = fastnumbers.fast_int(self._get_stat_from_file(stat))

        return self.__stats[stat]

    def _get_stat_from_file(self, key):
        coordinates = self.__file_map_dict[key]
        return self.__file_handle[coordinates[0]:coordinates[1]]

    def _get_column_type_from_index(self, column_index):
        return next(self._parse_data_values_from_file(column_index, 1, [[0, 1]], "", "ct")).decode()

    def _get_column_meta(self, filter_column_set, select_columns):
        column_type_dict = {}
        column_coords_dict = {}
        column_index_name_dict = {}

        if len(select_columns) == 0:
            coords = self._parse_data_coords([0, 1], data_prefix="cn")

            for row_index in range(self.get_num_cols()):
                values = self._parse_row_values(row_index, coords, data_prefix="cn")
                column_name = values[0]
                column_index = fastnumbers.fast_int(values[1])

                column_index_name_dict[column_index] = column_name

                if column_name in filter_column_set:
                    column_type_dict[column_name] = row_index

            all_coords = self._parse_data_coords(range(self.get_num_cols()))
            for row_index in range(self.get_num_cols()):
                column_coords_dict[column_index_name_dict[row_index]] = all_coords[row_index]

            all_columns = [x[1] for x in sorted(column_index_name_dict.items())]
            select_columns = all_columns
        else:
            select_columns = [name.encode() for name in select_columns]
            all_columns = list(filter_column_set | set(select_columns))

            column_name_index_dict = {}
            for column_name in all_columns:
                column_index = self._get_column_index_from_name(column_name.decode())
                column_name_index_dict[column_name] = column_index
                column_index_name_dict[column_index] = column_name

            for column_name in filter_column_set:
                column_type_dict[column_name] = self._get_column_type_from_index(column_name_index_dict[column_name])

            all_column_indices = [column_name_index_dict[name] for name in all_columns]
            all_coords = self._parse_data_coords(all_column_indices)

            for i, column_name in enumerate(all_columns):
                column_coords_dict[column_name] = all_coords[i]

        decompression_type = None
        decompressor = None
        bigram_size_dict = {}

        if "cmpr" in self.__file_map_dict:
            decompression_text_coords = self.__file_map_dict["cmpr"]
            decompression_text = self.__file_handle[decompression_text_coords[0]:decompression_text_coords[1]]

            if decompression_text == b"z":
                decompression_type = "zstd"
            else:
                decompression_type = "dictionary"
                decompressor = deserialize(decompression_text)

                for column_name in all_columns:
                    bigram_size_dict[column_name] = get_bigram_size(len(decompressor[column_name]["map"]))

        return select_columns, column_type_dict, column_coords_dict, decompression_type, decompressor, bigram_size_dict

    def _generate_row_chunks(self, num_processes):
        rows_per_chunk = math.ceil(self.get_num_rows() / num_processes)

        row_indices = set()

        for row_index in range(self.get_num_rows()):
            row_indices.add(row_index)

            if len(row_indices) == rows_per_chunk:
                yield row_indices
                row_indices = set()

        if len(row_indices) > 0:
            yield row_indices

    def _parse_data_coords(self, indices, data_prefix=""):
        #TODO: Store coordinates in an object-level dictionary after retrieving them?
        file_key = "cc"
        data_coords = []
        out_dict = {}
        mccl = self._get_stat(data_prefix + "mccl")

        for index in indices:
            start_pos = index * mccl
            next_start_pos = start_pos + mccl
            further_next_start_pos = next_start_pos + mccl

            # See if we already have cached the start position.
            if index in out_dict:
                data_start_pos = out_dict[index]
            # If not, retrieve the start position from the cc file and then cache it.
            else:
                data_start_pos = fastnumbers.fast_int(self._parse_data_value_from_file(0, 0, [start_pos, next_start_pos], data_prefix, file_key).rstrip(b" "))
                out_dict[index] = data_start_pos

            # See if we already have cached the end position.
            if (index + 1) in out_dict:
                data_end_pos = out_dict[index + 1]
            # If not, retrieve the end position from the cc file and then cache it.
            else:
                data_end_pos = fastnumbers.fast_int(self._parse_data_value_from_file(0, 0, [next_start_pos, further_next_start_pos], data_prefix, file_key).rstrip(b" "))
                out_dict[index + 1] = data_end_pos

            data_coords.append([data_start_pos, data_end_pos])

        return data_coords

    def _parse_data_value_from_file(self, start_element, segment_length, coords, data_prefix, file_key):
        start_pos = start_element * segment_length + self.__file_map_dict[data_prefix + file_key][0]
        return self.__file_handle[(start_pos + coords[0]):(start_pos + coords[1])]

    def _parse_data_values_from_string(self, data_coords, string):
        for coords in data_coords:
            yield string[(coords[0]):(coords[1])].rstrip(b" ")

    def _parse_data_values_from_file(self, start_element, segment_length, data_coords, data_prefix, file_key):
        start_pos = start_element * segment_length + self.__file_map_dict[data_prefix + file_key][0]

        for coords in data_coords:
            yield self.__file_handle[(start_pos + coords[0]):(start_pos + coords[1])].rstrip(b" ")

    def _get_parse_row_value_function(self, decompression_type):
        if not decompression_type:
            return self._parse_row_value
        elif decompression_type == "zstd":
            return self._parse_zstd_compressed_row_value
        else:
            return self._parse_dictionary_compressed_row_value

    def _parse_row_value(self, row_index, column_coords, line_length, data_prefix="", decompression_type=None, decompressor=None, bigram_size_dict=None, column_name=None):
        return self._parse_data_value_from_file(row_index, line_length, column_coords, data_prefix, "data").rstrip(b" ")

    def _parse_zstd_compressed_row_value(self, row_index, column_coords, line_length, data_prefix="", decompression_type=None, decompressor=None, bigram_size_dict=None, column_name=None):
        line = self._parse_data_value_from_file(row_index, line_length, [0, line_length], data_prefix, "data")
        line = decompressor.decompress(line)

        return line[column_coords[0]:column_coords[1]].rstrip(b" ")

    def _parse_dictionary_compressed_row_value(self, row_index, column_coords, line_length, data_prefix="", decompression_type=None, decompressor=None, bigram_size_dict=None, column_name=None):
        value = self._parse_data_value_from_file(row_index, line_length, column_coords, data_prefix, "data").rstrip(b" ")
        return decompress(value, decompressor[column_name], bigram_size_dict[column_name])

    def _get_parse_row_values_function(self, decompression_type):
        if not decompression_type:
            return self._parse_row_values
        elif decompression_type == "zstd":
            return self._parse_zstd_compressed_row_values
        else:
            return self._parse_dictionary_compressed_row_values

    def _parse_row_values(self, row_index, column_coords, data_prefix="", decompression_type=None, decompressor=None, bigram_size_dict=None, column_names=None):
        return list(self._parse_data_values_from_file(row_index, self._get_stat(data_prefix + "ll"), column_coords, data_prefix, "data"))

    def _parse_zstd_compressed_row_values(self, row_index, column_coords, data_prefix="", decompression_type=None, decompressor=None, bigram_size_dict=None, column_names=None):
        line_length = self._get_stat("ll")
        line = self._parse_data_value_from_file(row_index, line_length, [0, line_length], data_prefix, "data")
        line = decompressor.decompress(line)

        return list(self._parse_data_values_from_string(column_coords, line))

    def _parse_dictionary_compressed_row_values(self, row_index, column_coords, data_prefix="", decompression_type=None, decompressor=None, bigram_size_dict=None, column_names=None):
            values = list(self._parse_data_values_from_file(row_index, self._get_stat("ll"), column_coords, data_prefix, "data"))

            return [decompress(values.pop(0), decompressor[column_name], bigram_size_dict[column_name]) for column_name in column_names]

    # def _get_decompression_dict(self, file_path, column_index_name_dict):
    #     with open(file_path, "rb") as cmpr_file:
    #         return deserialize(cmpr_file.read())

    #     compression_dict = {}
    #     with open(file_path, "rb") as cmpr_file:
    #         for line in cmpr_file:
    #             line_items = line.rstrip(b"\n").split(b"\t")
    #             column_index = fastnumbers.fast_int(line_items[0])

    #             if column_index in column_index_name_dict:
    #                 column_name = column_index_name_dict[column_index]
    #                 compression_dict[column_name] = deserialize(line_items[1])

    #     #for column_index in column_index_name_dict.keys():
    #     #     compression_dict[column_index_name_dict[column_index]] = {}
    #     #     compression_dict[column_index_name_dict[column_index]]["map"] = {}

    #     # with Parser(file_path, fixed_file_extensions=[".data", ".cc"], stats_file_extensions=[".ll", ".mccl"]) as parser:
    #     #     coords = parser._parse_data_coords([0, 1, 2])
    #     #     num_rows = fastnumbers.fast_int((len(parser.get_file_handle(".data")) + 1) / parser.get_stat(".ll"))

    #     #     # Use a set for performance reasons
    #     #     column_indices_set = set(column_index_name_dict.keys())

    #     #     with Parser(f"{self.data_file_path}.cmpr", fixed_file_extensions=[".data", ".cc"], stats_file_extensions=[".ll", ".mccl"]) as parser:
    #     #         for row_index in range(num_rows):
    #     #             values = parser.__parse_row_values(row_index, coords)
    #     #             column_index = fastnumbers.fast_int(values[0])

    #     #             if column_index in column_indices_set:
    #     #                 compressed_value = convert_bytes_to_int(values[2])

    #     #                 compression_dict[column_index_name_dict[column_index]]["map"][compressed_value] = values[1]

    #     # # # We need column names as keys rather than indices.
    #     # # compression_dict2 = {}
    #     # # for i, column_name in enumerate(column_names):
    #     # #     compression_dict2[column_name] = compression_dict[column_indices[i]]

    #     # with Parser(f"{self.data_file_path}.cmprtype", fixed_file_extensions=[".data"], stats_file_extensions=[".ll"]) as parser:
    #     #     coords = [[0, 1]]

    #     #     for column_index in column_index_name_dict.keys():
    #     #         compression_dict[column_index_name_dict[column_index]]["compression_type"] = parser.__parse_row_values(column_index, coords)[0]

    #     return compression_dict

    # def _invert_decompression_dict(self, decompression_dict, select_columns):
    #     inverted_dict = {}
    #
    #     for select_column in select_columns:
    #         inverted_dict[select_column] = {"compression_type": decompression_dict[select_column]["compression_type"]}
    #         inverted_dict[select_column]["map"] = {}
    #
    #         for compressed_value, value in decompression_dict[select_column]["map"].items():
    #             inverted_dict[select_column]["map"][value] = compressed_value
    #
    #     return inverted_dict

"""
This class is used to indicate that no filtering should be performed.
"""
class NoFilter:
    def _check_types(self, column_type_dict):
        pass

    def _get_column_name_set(self):
        return set()

    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict):
        return row_indices

    def _filter_indexed_column_values(self, data_file_path, end_index, num_processes):
        return set(range(end_index))

class __SimpleBaseFilter(NoFilter):
    def __init__(self, column_name, value):
        self.check_argument(column_name, "column_name", str)
        self.column_name = column_name.encode()
        self.value = value

    def check_argument(self, x, argument_name, expected_value_type):
        if x == None:
            raise Exception(f"A value of None was specified for the {argument_name} argument of the {type(self).__name__} class.")

        if type(x) != expected_value_type:
            raise Exception(f"A variable of {expected_value_type.__name__} type is required for the {argument_name} argument of the {type(self).__name__} class, but the type was {type(x).__name__}.")

    def _get_column_name_set(self):
        return set([self.column_name])

    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict):
        with Parser(data_file_path) as parser:
            line_length = parser._get_stat("ll")
            coords = column_coords_dict[self.column_name]

            # This avoids having to check the decompression type each time we parse a value.
            decompressor = get_decompressor(decompression_type, decompressor)
            parse_function = parser._get_parse_row_value_function(decompression_type)

            passing_row_indices = set()
            for i in row_indices:
                if self.passes(parse_function(i, coords, line_length, decompression_type=decompression_type, decompressor=decompressor, bigram_size_dict=bigram_size_dict, column_name=self.column_name)):
                    passing_row_indices.add(i)

            return passing_row_indices

    def _get_conversion_function(self):
        return do_nothing

#    def __str__(self):
#        return f"{type(self).__name__}____{self.column_name.decode()}____{self.value}"

class __OperatorFilter(__SimpleBaseFilter):
    def __init__(self, column_name, oper, value):
        super().__init__(column_name, value)

        self.oper = oper

    def _filter_indexed_column_values(self, data_file_path, end_index, num_processes):
        index_file_path = get_index_file_path(data_file_path, self.column_name.decode())

        return IndexSearcher._filter_using_operator(index_file_path, self, end_index, num_processes)

    def _check_column_types(self, column_index_dict, column_type_dict, expected_column_type, expected_column_type_description):
        if column_type_dict[column_index_dict[self.column_name]] != expected_column_type:
            raise Exception(f"A {type(self).__name__} may only be used with {expected_column_type_description} columns, and {self.column_name.decode()} is not a {expected_column_type_description}.")

    def passes(self, value):
        return self.oper(self._get_conversion_function()(value), self.value)

class StringFilter(__OperatorFilter):
    def __init__(self, column_name, oper, value):
        self.check_argument(value, "value", str)
        super().__init__(column_name, oper, value.encode())

    def _check_types(self, column_type_dict):
        if column_type_dict[self.column_name] != "s":
            raise Exception(f"A StringFilter may only be used with string columns, and {self.column_name.decode()} is not a string.")

class FloatFilter(__OperatorFilter):
    def __init__(self, column_name, oper, value):
        self.check_argument(value, "value", float)
        super().__init__(column_name, oper, value)

    def _check_types(self, column_type_dict):
        if column_type_dict[self.column_name] != "f":
            raise Exception(f"A float filter may only be used with float columns, but {self.column_name.decode()} is not a float.")

    def _get_conversion_function(self):
        return fastnumbers.fast_float

class IntFilter(__OperatorFilter):
    def __init__(self, column_name, oper, value):
        self.check_argument(value, "value", int)
        super().__init__(column_name, oper, value)

    def _check_types(self, column_type_dict):
        if column_type_dict[self.column_name] != "i":
            raise Exception(f"An integer filter may only be used with integer columns, but {self.column_name.decode()} is not an integer.")

    def _get_conversion_function(self):
        return fastnumbers.fast_int

class StartsWithFilter(__SimpleBaseFilter):
    def __init__(self, column_name, value):
        self.check_argument(value, "value", str)
        super().__init__(column_name, value.encode())

    def _filter_indexed_column_values(self, data_file_path, end_index, num_processes):
        index_file_path = get_index_file_path(data_file_path, self.column_name.decode())

        return IndexSearcher._get_passing_row_indices_with_filter(index_file_path, self, end_index, num_processes)

    def passes(self, value):
        return value.startswith(self.value)

class EndsWithFilter(StartsWithFilter):
    def passes(self, value):
        return value.endswith(self.value)

    def _filter_indexed_column_values(self, data_file_path, end_index, num_processes):
        custom_index_function = reverse_string
        custom_index_file_path = get_index_file_path(data_file_path, self.column_name.decode(), custom_index_function)

        if os.path.exists(custom_index_file_path):
            custom_fltr = StartsWithFilter(self.column_name.decode(), custom_index_function(self.value).decode())

            return IndexSearcher._get_passing_row_indices_with_filter(custom_index_file_path, custom_fltr, end_index, num_processes)
        else:
            index_file_path = get_index_file_path(data_file_path, self.column_name.decode())

            with Parser(index_file_path) as index_parser:
                line_length = index_parser._get_stat("ll")
                coords = index_parser._parse_data_coords([0, 1])

                return IndexSearcher._get_passing_row_indices(self, index_parser, line_length, coords[0], coords[1], 0, end_index)

class LikeFilter(__SimpleBaseFilter):
    def __init__(self, column_name, regular_expression):
        super().__init__(column_name, regular_expression)

        self.check_argument(regular_expression, "regular_expression", str)
        self.value = re.compile(self.value)

    def _filter_indexed_column_values(self, data_file_path, end_index, num_processes):
        index_file_path = get_index_file_path(data_file_path, self.column_name.decode())

        with Parser(index_file_path) as index_parser:
            line_length = index_parser._get_stat("ll")
            coords = index_parser._parse_data_coords([0, 1])

            return IndexSearcher._get_passing_row_indices(self, index_parser, line_length, coords[0], coords[1], 0, end_index)

    def passes(self, value):
        return self.value.search(value.decode())

class NotLikeFilter(LikeFilter):
    def passes(self, value):
        return not self.value.search(value.decode())

class HeadFilter(NoFilter):
    def __init__(self, n, select_columns):
        self.n = n
        self.select_columns_set = set([x.encode() for x in select_columns])

    def _get_column_name_set(self):
        return self.select_columns_set

    def _get_num_rows(self, data_file_path):
        with Parser(data_file_path) as parser:
            return parser.get_num_rows()

    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict):
        return set(range(min(self._get_num_rows(data_file_path), self.n))) & row_indices

    def _filter_indexed_column_values(self, data_file_path, end_index, num_processes):
        return set(range(min(self._get_num_rows(data_file_path), self.n)))

class TailFilter(HeadFilter):
    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict):
        num_rows = self._get_num_rows(data_file_path)
        return set(range(num_rows - self.n, num_rows)) & row_indices

    def _filter_indexed_column_values(self, data_file_path, end_index, num_processes):
        num_rows = self._get_num_rows(data_file_path)
        return set(range(num_rows - self.n, num_rows))

class __CompositeFilter(NoFilter):
    def __init__(self, filter1, filter2):
        self.filter1 = filter1
        self.filter2 = filter2

    def _check_types(self, column_type_dict):
        self.filter1._check_types(column_type_dict)
        self.filter2._check_types(column_type_dict)

    def _get_column_name_set(self):
        return self.filter1._get_column_name_set() | self.filter2._get_column_name_set()

#    def get_sub_filters(self):
#        return self.filter1.get_sub_filters() + self.filter2.get_sub_filters()

#    def get_sub_filter_row_indices(self, fltr_results_dict):
#        if len(self.filter1.get_sub_filters()) == 1:
#            row_indices_1 = fltr_results_dict[str(self.filter1)]
#        else:
#            row_indices_1 = self.filter1.filter_indexed_column_values_parallel(fltr_results_dict)
#
#        if len(self.filter2.get_sub_filters()) == 1:
#            row_indices_2 = fltr_results_dict[str(self.filter2)]
#        else:
#            row_indices_2 = self.filter2.filter_indexed_column_values_parallel(fltr_results_dict)
#
#        return row_indices_1, row_indices_2

class AndFilter(__CompositeFilter):
    """
    This class is used to construct a filter with multiple sub-filters that must all evaluate to True.
    Order does matter; filter1 is applied first. Any rows that remain after filter1 has been applied
    will be sent to filter2.

    Args:
        filter1: The first filter to be evaluated.
        filter2: The second filter to be evaluated.
    """
    def __init__(self, filter1, filter2):
        super().__init__(filter1, filter2)

    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict):
        row_indices_1 = self.filter1._filter_column_values(data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict)
        return self.filter2._filter_column_values(data_file_path, row_indices_1, column_coords_dict, decompression_type, decompressor, bigram_size_dict)

    def _filter_indexed_column_values(self, data_file_path, end_index, num_processes):
        # Currently, this combination of two-column filters is supported. Add more later.
        if isinstance(self.filter1, StringFilter) and self.filter1.oper == operator.eq:
            if isinstance(self.filter2, IntRangeFilter):
                two_column_index_name = IndexSearcher._get_two_column_index_name(self.filter1, self.filter2.filter1)
                two_column_index_file_path = get_index_file_path(data_file_path, two_column_index_name)

                if os.path.exists(two_column_index_file_path):
                    with Parser(two_column_index_file_path) as index_parser:
                        coords = index_parser._parse_data_coords([0, 1, 2])

                        # Find range for string column
                        lower_position, upper_position = IndexSearcher._find_bounds_for_range(index_parser, coords[0], self.filter1, self.filter1, end_index, num_processes)

                        # Find range for int column
                        lower_position, upper_position = IndexSearcher._find_bounds_for_range(index_parser, coords[1], self.filter2.filter1, self.filter2.filter2, upper_position, num_processes, lower_position)

                        # Get row indices for the overlapping range
                        return IndexSearcher._retrieve_matching_row_indices(index_parser, coords[2], (lower_position, upper_position), num_processes)

        row_indices_1 = self.filter1._filter_indexed_column_values(data_file_path, end_index, num_processes)
        row_indices_2 = self.filter2._filter_indexed_column_values(data_file_path, end_index, num_processes)

        return row_indices_1 & row_indices_2

#    def filter_indexed_column_values_parallel(self, fltr_results_dict):
#        row_indices_1, row_indices_2 = self.get_sub_filter_row_indices(fltr_results_dict)
#        return row_indices_1 & row_indices_2

class OrFilter(__CompositeFilter):
    """
    This class is used to construct a filter with multiple sub-filters. At least one must evaluate to True.
    Order does matter; filter1 is applied first. Any rows that did not pass after filter1 has been
    applied will be sent to filter2.

    Args:
        *args (list): A variable number of filters that should be evaluated. At least two filters must be specified.
    """
    def __init__(self, filter1, filter2):
        super().__init__(filter1, filter2)

    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict):
        row_indices_1 = self.filter1._filter_column_values(data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict)
        row_indices_2 = self.filter2._filter_column_values(data_file_path, row_indices - row_indices_1, column_coords_dict, decompression_type, decompressor, bigram_size_dict)

        return row_indices_1 | row_indices_2

    def _filter_indexed_column_values(self, data_file_path, end_index, num_processes):
        row_indices_1 = self.filter1._filter_indexed_column_values(data_file_path, end_index, num_processes)
        row_indices_2 = self.filter2._filter_indexed_column_values(data_file_path, end_index, num_processes)

        return row_indices_1 | row_indices_2

#    def filter_indexed_column_values_parallel(self, fltr_results_dict):
#        row_indices_1, row_indices_2 = self.get_sub_filter_row_indices(fltr_results_dict)
#        return row_indices_1 | row_indices_2

class __RangeFilter(__CompositeFilter):
    def __init__(self, filter1, filter2):
        super().__init__(filter1, filter2)

        if filter1.value > filter2.value:
            raise Exception("The lower_bound_value must be less than or equal to the upper_bound_value.")

    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict):
        return AndFilter(self.filter1, self.filter2)._filter_column_values(data_file_path, row_indices, column_coords_dict, decompression_type, decompressor, bigram_size_dict)

    def _filter_indexed_column_values(self, data_file_path, end_index, num_processes):
        index_file_path = get_index_file_path(data_file_path, self.filter1.column_name.decode())

        with Parser(index_file_path) as index_parser:
            coords = index_parser._parse_data_coords([0, 1])

            return IndexSearcher._find_row_indices_for_range(index_parser, coords[0], coords[1], self.filter1, self.filter2, end_index, num_processes)

    def _get_conversion_function(self):
        return do_nothing

class FloatRangeFilter(__RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        filter1 = FloatFilter(column_name, operator.ge, lower_bound_value)
        filter2 = FloatFilter(column_name, operator.le, upper_bound_value)

        super().__init__(filter1, filter2)

    def _get_conversion_function(self):
        return fastnumbers.fast_float

class IntRangeFilter(__RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        filter1 = IntFilter(column_name, operator.ge, lower_bound_value)
        filter2 = IntFilter(column_name, operator.le, upper_bound_value)

        super().__init__(filter1, filter2)

    def _get_conversion_function(self):
        return fastnumbers.fast_int

class StringRangeFilter(__RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        filter1 = StringFilter(column_name, operator.ge, lower_bound_value)
        filter2 = StringFilter(column_name, operator.le, upper_bound_value)

        super().__init__(filter1, filter2)

class IndexSearcher:
    def _get_identifier_row_index(parser, query_value, end_index, data_prefix=""):
        if end_index == 0:
            return -1

        line_length = parser._get_stat(data_prefix + "ll")
        value_coords = parser._parse_data_coords([0], data_prefix=data_prefix)[0]
        position_coords = parser._parse_data_coords([1], data_prefix=data_prefix)[0]

        matching_position = IndexSearcher._binary_identifier_search(parser, data_prefix, line_length, value_coords, query_value, 0, end_index)

        if matching_position == -1:
            return -1

        return fastnumbers.fast_int(parser._parse_row_value(matching_position, position_coords, line_length, data_prefix=data_prefix))

    # Searches for a single matching value.
    def _binary_identifier_search(parser, data_prefix, line_length, value_coords, value_to_find, l, r):
        if r == -1 or l > r:
            return -1

        mid = l + (r - l) // 2
        mid_value = parser._parse_row_value(mid, value_coords, line_length, data_prefix=data_prefix)

        if mid_value == value_to_find:
            # If element is present at the middle itself
            return mid
        elif mid_value > value_to_find:
            return IndexSearcher._binary_identifier_search(parser, data_prefix, line_length, value_coords, value_to_find, l, mid - 1)
        else:
            # Else the element can only be present in right subarray
            return IndexSearcher._binary_identifier_search(parser, data_prefix, line_length, value_coords, value_to_find, mid + 1, r)

    def _filter_using_operator(index_file_path, fltr, end_index, num_processes):
        if end_index == 0:
            return set()

        with Parser(index_file_path) as index_parser:
            line_length = index_parser._get_stat("ll")
            coords = index_parser._parse_data_coords([0, 1])

            if fltr.oper == operator.eq:
                return IndexSearcher._find_row_indices_for_range(index_parser, coords[0], coords[1], fltr, fltr, end_index, num_processes)
            else:
                if fltr.oper == operator.ne:
                    lower_position, upper_position = IndexSearcher._find_bounds_for_range(index_parser, coords[0], fltr, fltr, end_index, num_processes)

                    lower_positions = (0, lower_position)
                    upper_positions = (upper_position, end_index)

                    lower_row_indices = IndexSearcher._retrieve_matching_row_indices(index_parser, coords[1], lower_positions, num_processes)
                    upper_row_indices = IndexSearcher._retrieve_matching_row_indices(index_parser, coords[1], upper_positions, num_processes)

                    return lower_row_indices | upper_row_indices
                else:
                    if fltr.oper == operator.gt:
                        positions = IndexSearcher._find_positions_g(index_parser, line_length, coords[0], fltr, 0, end_index, operator.le)
                    elif fltr.oper == operator.ge:
                        positions = IndexSearcher._find_positions_g(index_parser, line_length, coords[0], fltr, 0, end_index, operator.lt)
                    elif fltr.oper == operator.lt:
                        positions = IndexSearcher._find_positions_l(index_parser, line_length, coords[0], fltr, 0, end_index, fltr.oper)
                    elif fltr.oper == operator.le:
                        positions = IndexSearcher._find_positions_l(index_parser, line_length, coords[0], fltr, 0, end_index, fltr.oper)

                    return IndexSearcher._retrieve_matching_row_indices(index_parser, coords[1], positions, num_processes)

    def _find_positions_g(index_parser, line_length, value_coords, fltr, start_index, end_index, all_false_operator):
        smallest_value = index_parser._parse_row_value(start_index, value_coords, line_length)
        if smallest_value == b"":
            return start_index, end_index

        if not all_false_operator(fltr._get_conversion_function()(smallest_value), fltr.value):
            return start_index, end_index

        largest_value = index_parser._parse_row_value(end_index - 1, value_coords, line_length)
        if largest_value == b"":
            return start_index, start_index

        matching_position = IndexSearcher._search(index_parser, line_length, value_coords, fltr, 0, end_index, end_index, all_false_operator)

        return matching_position + 1, end_index

    def _find_positions_l(index_parser, line_length, value_coords, fltr, start_index, end_index, all_true_operator):
        smallest_value = index_parser._parse_row_value(start_index, value_coords, line_length)
        if smallest_value == b"":
            return start_index, start_index

        if not all_true_operator(fltr._get_conversion_function()(smallest_value), fltr.value):
            return start_index, start_index

        largest_value = index_parser._parse_row_value(end_index - 1, value_coords, line_length)
        if largest_value == b"":
            return start_index, end_index

        if all_true_operator(fltr._get_conversion_function()(largest_value), fltr.value):
            return start_index, end_index

        matching_position = IndexSearcher._search(index_parser, line_length, value_coords, fltr, 0, end_index, end_index, all_true_operator)

        return start_index, matching_position + 1

    # TODO: It might make sense to combine this function with _search_with_filter
    #      to avoid duplicating similar code.
    def _search(index_parser, line_length, value_coords, fltr, left_index, right_index, overall_end_index, search_operator):
        mid_index = left_index + (right_index - left_index) // 2

        conversion_function = fltr._get_conversion_function()
        mid_value = conversion_function(index_parser._parse_row_value(mid_index, value_coords, line_length))

        if search_operator(mid_value, fltr.value):
            next_index = mid_index + 1

            if next_index == overall_end_index:
                return mid_index

            next_value = index_parser._parse_row_value(mid_index + 1, value_coords, line_length)

            # TODO: Does this work if we have a blank data value? Perhaps we can remove it?
            #       Modify to be like _search_with_filter?
            if next_value == b"":
               return mid_index
            elif not search_operator(conversion_function(next_value), fltr.value):
                return mid_index
            else:
                return IndexSearcher._search(index_parser, line_length, value_coords, fltr, mid_index, right_index, overall_end_index, search_operator)
        else:
            return IndexSearcher._search(index_parser, line_length, value_coords, fltr, left_index, mid_index, overall_end_index, search_operator)

    def _search_with_filter(index_parser, line_length, value_coords, left_index, right_index, overall_end_index, fltr):
        mid_index = (left_index + right_index) // 2

        if mid_index == 0:
            return 0

        conversion_function = fltr._get_conversion_function()
        mid_value = conversion_function(index_parser._parse_row_value(mid_index, value_coords, line_length))

        if fltr.passes(mid_value):
            if mid_index == right_index:
                return mid_index

            next_index = mid_index + 1

            if next_index == overall_end_index:
                return next_index

            next_value = conversion_function(index_parser._parse_row_value(next_index, value_coords, line_length))

            if fltr.passes(next_value):
                return IndexSearcher._search_with_filter(index_parser, line_length, value_coords, next_index, right_index, overall_end_index, fltr)
            else:
                return mid_index + 1
        else:
            if left_index == mid_index:
                return mid_index

            return IndexSearcher._search_with_filter(index_parser, line_length, value_coords, left_index, mid_index, overall_end_index, fltr)

    def _find_matching_row_indices(index_file_path, position_coords, positions):
        # To make this paralellizable, we pass just a file path rather than index_parser.
        with Parser(index_file_path) as index_parser:
            line_length = index_parser._get_stat("ll")

            matching_row_indices = set()
            for i in range(positions[0], positions[1]):
                matching_row_indices.add(fastnumbers.fast_int(index_parser._parse_row_value(i, position_coords, line_length)))

            return matching_row_indices

    def _retrieve_matching_row_indices(index_parser, position_coords, positions, num_processes):
        # This is a rough threshold for determine whether it is worth the overhead to parallelize.
        num_indices = positions[1] - positions[0]

        if num_processes == 1 or num_indices < 100:
            return IndexSearcher._find_matching_row_indices(index_parser.data_file_path, position_coords, positions)
        else:
            chunk_size = math.ceil(num_indices / num_processes)
            position_chunks = []
            for i in range(positions[0], positions[1], chunk_size):
                position_chunks.append((i, min(positions[1], i + chunk_size)))

            return set(chain.from_iterable(Parallel(n_jobs=num_processes)(
                delayed(IndexSearcher._find_matching_row_indices)(index_parser.data_file_path, position_coords, position_chunk)
                for position_chunk in position_chunks)))

    def _find_bounds_for_range(index_parser, value_coords, filter1, filter2, end_index, num_processes, start_index=0):
        line_length = index_parser._get_stat("ll")

        lower_positions = IndexSearcher._find_positions_g(index_parser, line_length, value_coords, filter1, start_index, end_index, operator.lt)
        upper_positions = IndexSearcher._find_positions_l(index_parser, line_length, value_coords, filter2, lower_positions[0], lower_positions[1], operator.le)

        lower_position = max(lower_positions[0], upper_positions[0])
        upper_position = min(lower_positions[1], upper_positions[1])

        return lower_position, upper_position

    def _find_row_indices_for_range(index_parser, value_coords, position_coords, filter1, filter2, end_index, num_processes):
        lower_position, upper_position = IndexSearcher._find_bounds_for_range(index_parser, value_coords, filter1, filter2, end_index, num_processes)

        return IndexSearcher._retrieve_matching_row_indices(index_parser, position_coords, (lower_position, upper_position), num_processes)

    def _get_passing_row_indices(fltr, parser, line_length, coords_value, coords_position, start_index, end_index):
        passing_row_indices = set()

        for i in range(start_index, end_index):
            if fltr.passes(parser._parse_row_value(i, coords_value, line_length)):
                passing_row_indices.add(fastnumbers.fast_int(parser._parse_row_value(i, coords_position, line_length)))

        return passing_row_indices

    def _get_passing_row_indices_with_filter(index_file_path, fltr, end_index, num_processes):
        with Parser(index_file_path) as index_parser:
            line_length = index_parser._get_stat("ll")
            coords = index_parser._parse_data_coords([0, 1])

            lower_range = IndexSearcher._find_positions_g(index_parser, line_length, coords[0], fltr, 0, end_index, operator.lt)

            if lower_range[0] == end_index:
                return set()

            upper_position = IndexSearcher._search_with_filter(index_parser, line_length, coords[0], lower_range[0], lower_range[1], end_index, fltr)

            return IndexSearcher._retrieve_matching_row_indices(index_parser, coords[1], (lower_range[0], upper_position), num_processes)

    def _get_two_column_index_name(filter1, filter2):
        return "____".join([filter1.column_name.decode(), filter2.column_name.decode()])