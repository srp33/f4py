import f4
import fastnumbers
import glob
from itertools import chain
from joblib import Parallel, delayed
import math
import os
import sys

class Parser:
    """
    This class is used for querying F4 files and saving the output to new files.

    Args:
        data_file_path (str): The path to an existing F4 file.

    Attributes:
        data_file_path (str): The path to an existing F4 file.
    """
    #def __init__(self, data_file_path, fixed_file_extensions=[".data", ".cc", ".ct"], stats_file_extensions=[".ll", ".mccl"]):
    def __init__(self, data_file_path):
        #TODO: expand this out for the other parameters.
        if not isinstance(data_file_path, str):
            raise Exception("You must specify data_file_path as an str value.")

        self.data_file_path = data_file_path
        # Cache file handles in a dictionary.
        # self.__file_handles = {}
        # for ext in fixed_file_extensions:
        #     self.__file_handles[ext] = self._set_file_handle(ext)
        self.__file_handle = f4.open_read_file(self.data_file_path)

        file_map_length_string = self.__file_handle.readline()
        file_map_length = fastnumbers.fast_int(file_map_length_string.rstrip(b"\n"))
        self.__file_map_dict = f4.deserialize(self.__file_handle[len(file_map_length_string):(len(file_map_length_string) + file_map_length)])

        # Invert the abbreviations used for file keys in the dictionary so they are more human readable.
        for file_name, abbreviation in f4.FILE_KEY_ABBREVIATIONS.items():
            if abbreviation in self.__file_map_dict:
                self.__file_map_dict[file_name] = self.__file_map_dict[abbreviation]
                del self.__file_map_dict[abbreviation]

        # Cache statistics in a dictionary.
        self.__stats = {}

        data_size =  self.__file_map_dict["data"][1] - self.__file_map_dict["data"][0]
        self.__stats["nrow"] = fastnumbers.fast_int(data_size / self._get_stat("ll"))

        cc_size = self.__file_map_dict["cc"][1] - self.__file_map_dict["cc"][0]
        self.__stats["ncol"] = fastnumbers.fast_int(cc_size / self._get_stat("mccl")) - 1
        # for ext in stats_file_extensions:
        #     self.__stats[ext] = f4.read_int_from_file(data_file_path, ext)

    def __enter__(self):
        return self

    def __exit__(self, the_type, value, traceback):
        # for handle in self.__file_handles.values():
        #     handle.close()
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

        if not isinstance(fltr, f4.NoFilter):
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
        decompressor = f4.get_decompressor(decompression_type, decompressor)
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
        self.query_and_write(f4.HeadFilter(n, select_columns), select_columns, out_file_path=out_file_path, out_file_type=out_file_type)

    def tail(self, n = 10, select_columns=None, out_file_path=None, out_file_type="tsv"):
        if not select_columns:
            select_columns = []
        self.query_and_write(f4.TailFilter(n, select_columns), select_columns, out_file_path=out_file_path, out_file_type=out_file_type)

    def get_num_rows(self):
        #return int(len(self.__file_handles[".data"]) / self._get_stat("ll"))
        return self._get_stat("nrow")

    def get_num_cols(self):
        #return int(len(self.__file_handles[".cc"]) / self._get_stat("mccl")) - 1
        return self._get_stat("ncol")

    def get_column_type_from_name(self, column_name):
        try:
            #with f4.IndexSearcher._get_index_parser(f"{self.data_file_path}.cn") as index_parser:
            #    return self._get_column_type_from_index(self._get_column_index_from_name(index_parser, column_name))
            return self._get_column_type_from_index(self._get_column_index_from_name(column_name))

            # #TODO: Cache this?
            # cncc = self._parse_data_coords([0, 1], file_key = "cncc", max_length_file_key = "cnmccl")
            # print(cncc)
            # print(self._parse_data_value_from_file(self.__file_map_dict["cndata"][0], 0, cncc, "cndata"))
            # import sys
            # sys.exit()
        except:
            raise Exception(f"A column with the name {column_name} does not exist.")

    ##############################################
    # Non-public functions
    ##############################################

    #def _get_column_index_from_name(self, index_parser, column_name):
    def _get_column_index_from_name(self, column_name):
        position = f4.IndexSearcher._get_identifier_row_index(self, column_name.encode(), self.get_num_cols(), data_prefix="cn")

        if position < 0:
            raise Exception(f"Could not retrieve index because column named {column_name} was not found.")

        return position

    # def _get_file_handle(self, ext):
    #     return self.__file_handles[ext]

    # def _set_file_handle(self, ext):
    #     if ext not in self.__file_handles:
    #         self.__file_handles[ext] = f4.open_read_file(self.data_file_path, ext)
    #
    #     return self._get_file_handle(ext)

    def _get_stat(self, stat):
        if stat not in self.__stats:
            self.__stats[stat] = fastnumbers.fast_int(self._get_stat_from_file(stat))

        return self.__stats[stat]

    def _get_stat_from_file(self, key):
        coordinates = self.__file_map_dict[key]
        return self.__file_handle[coordinates[0]:coordinates[1]]

    def _get_column_type_from_index(self, column_index):
        #return next(self._parse_data_values(column_index, 1, [[0, 1]], self.__file_handles[".ct"])).decode()
        return next(self._parse_data_values_from_file(column_index, 1, [[0, 1]], "", "ct")).decode()

    def _get_column_meta(self, filter_column_set, select_columns):
        column_type_dict = {}
        column_coords_dict = {}
        column_index_name_dict = {}
        #column_name_index_dict = {}

        # if len(select_columns) == 0:
        #     with f4.Parser(self.data_file_path + ".cn", fixed_file_extensions=[".data", ".cc"], stats_file_extensions=[".ll", ".mccl"]) as cn_parser:
        #         coords = cn_parser._parse_data_coords([0, 1])
        #
        #         for row_index in range(self.get_num_cols()):
        #             values = cn_parser._parse_row_values(row_index, coords)
        #             column_name = values[0]
        #             column_index = fastnumbers.fast_int(values[1])
        #
        #             column_index_name_dict[column_index] = column_name
        #             #column_name_index_dict[column_name] = column_index
        #
        #             if column_name in filter_column_set:
        #                 column_type_dict[column_name] = row_index
        #
        #         all_coords = self._parse_data_coords(range(self.get_num_cols()))
        #         for row_index in range(self.get_num_cols()):
        #             column_coords_dict[column_index_name_dict[row_index]] = all_coords[row_index]
        #
        #     all_columns = [x[1] for x in sorted(column_index_name_dict.items())]
        #     select_columns = all_columns
        # else:
        #     with f4.IndexSearcher._get_index_parser(f"{self.data_file_path}.cn") as index_parser:
        #         select_columns = [name.encode() for name in select_columns]
        #         all_columns = list(filter_column_set | set(select_columns))
        #
        #         column_name_index_dict = {}
        #         for column_name in all_columns:
        #             column_index = self._get_column_index_from_name(index_parser, column_name.decode())
        #             column_name_index_dict[column_name] = column_index
        #             column_index_name_dict[column_index] = column_name
        #
        #         for column_name in filter_column_set:
        #             column_type_dict[column_name] = self._get_column_type_from_index(column_name_index_dict[column_name])
        #
        #     all_column_indices = [column_name_index_dict[name] for name in all_columns]
        #     all_coords = self._parse_data_coords(all_column_indices)
        #
        #     for i, column_name in enumerate(all_columns):
        #         column_coords_dict[column_name] = all_coords[i]

        if len(select_columns) == 0:
            coords = self._parse_data_coords([0, 1], data_prefix="cn")

            for row_index in range(self.get_num_cols()):
                values = self._parse_row_values(row_index, coords, data_prefix="cn")
                column_name = values[0]
                column_index = fastnumbers.fast_int(values[1])

                column_index_name_dict[column_index] = column_name
                #column_name_index_dict[column_name] = column_index

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
        # decompressor_file_path = f"{self.data_file_path}.cmpr"

        #if os.path.exists(decompressor_file_path):
        if "cmpr" in self.__file_map_dict:
            #decompression_text = f4.read_str_from_file(decompressor_file_path)
            #decompression_text = self._parse_data_value_from_file(0, 0, self.__file_map_dict["cmpr"])
            decompression_text_coords = self.__file_map_dict["cmpr"]
            decompression_text = self.__file_handle[decompression_text_coords[0]:decompression_text_coords[1]]

            if decompression_text == b"z":
                decompression_type = "zstd"
            else:
                decompression_type = "dictionary"
                #decompressor = self._get_decompression_dict(decompressor_file_path, column_index_name_dict)
                decompressor = f4.deserialize(decompression_text)

                for column_name in all_columns:
                    bigram_size_dict[column_name] = f4.get_bigram_size(len(decompressor[column_name]["map"]))

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
                #data_start_pos = fastnumbers.fast_int(self.__file_handles[".cc"][start_pos:next_start_pos].rstrip(b" "))
                data_start_pos = fastnumbers.fast_int(self._parse_data_value_from_file(0, 0, [start_pos, next_start_pos], data_prefix, file_key).rstrip(b" "))
                out_dict[index] = data_start_pos

            # See if we already have cached the end position.
            if (index + 1) in out_dict:
                data_end_pos = out_dict[index + 1]
            # If not, retrieve the end position from the cc file and then cache it.
            else:
                #data_end_pos = fastnumbers.fast_int(self.__file_handles[".cc"][next_start_pos:further_next_start_pos].rstrip(b" "))
                data_end_pos = fastnumbers.fast_int(self._parse_data_value_from_file(0, 0, [next_start_pos, further_next_start_pos], data_prefix, file_key).rstrip(b" "))
                out_dict[index + 1] = data_end_pos

            data_coords.append([data_start_pos, data_end_pos])

        return data_coords

    # def _parse_data_value_from_string(self, start_element, segment_length, coords, string):
    #     start_pos = start_element * segment_length
    #     return string[(start_pos + coords[0]):(start_pos + coords[1])]

    def _parse_data_value_from_file(self, start_element, segment_length, coords, data_prefix, file_key):
        start_pos = start_element * segment_length + self.__file_map_dict[data_prefix + file_key][0]
        return self.__file_handle[(start_pos + coords[0]):(start_pos + coords[1])]

    # def _parse_data_values_from_string(self, start_element, segment_length, data_coords, string):
    #     start_pos = start_element * segment_length
    #
    #     for coords in data_coords:
    #         yield string[(start_pos + coords[0]):(start_pos + coords[1])].rstrip(b" ")

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
        #return self._parse_data_value_from_file(row_index, line_length, column_coords, file_handle).rstrip(b" ")
        return self._parse_data_value_from_file(row_index, line_length, column_coords, data_prefix, "data").rstrip(b" ")

    def _parse_zstd_compressed_row_value(self, row_index, column_coords, line_length, data_prefix="", decompression_type=None, decompressor=None, bigram_size_dict=None, column_name=None):
        #line = self._parse_data_value_from_file(row_index, line_length, [0, line_length], file_handle)
        line = self._parse_data_value_from_file(row_index, line_length, [0, line_length], data_prefix, "data")
        line = decompressor.decompress(line)
        #return self._parse_data_value_from_string(0, 0, column_coords, line).rstrip(b" ")
        return line[column_coords[0]:column_coords[1]].rstrip(b" ")

    def _parse_dictionary_compressed_row_value(self, row_index, column_coords, line_length, data_prefix="", decompression_type=None, decompressor=None, bigram_size_dict=None, column_name=None):
        #value = self._parse_data_value_from_file(row_index, line_length, column_coords, file_handle).rstrip(b" ")
        value = self._parse_data_value_from_file(row_index, line_length, column_coords, data_prefix, "data").rstrip(b" ")
        return f4.decompress(value, decompressor[column_name], bigram_size_dict[column_name])

    def _get_parse_row_values_function(self, decompression_type):
        if not decompression_type:
            return self._parse_row_values
        elif decompression_type == "zstd":
            return self._parse_zstd_compressed_row_values
        else:
            return self._parse_dictionary_compressed_row_values

    def _parse_row_values(self, row_index, column_coords, data_prefix="", decompression_type=None, decompressor=None, bigram_size_dict=None, column_names=None):
        #return list(self._parse_data_values(row_index, self._get_stat("ll"), column_coords, self.__file_handles[".data"]))
        return list(self._parse_data_values_from_file(row_index, self._get_stat(data_prefix + "ll"), column_coords, data_prefix, "data"))

    def _parse_zstd_compressed_row_values(self, row_index, column_coords, data_prefix="", decompression_type=None, decompressor=None, bigram_size_dict=None, column_names=None):
        line_length = self._get_stat("ll")
        #line = self._parse_data_value_from_file(row_index, line_length, [0, line_length], self.__file_handles[".data"])
        line = self._parse_data_value_from_file(row_index, line_length, [0, line_length], data_prefix, "data")
        line = decompressor.decompress(line)

        return list(self._parse_data_values_from_string(column_coords, line))

    def _parse_dictionary_compressed_row_values(self, row_index, column_coords, data_prefix="", decompression_type=None, decompressor=None, bigram_size_dict=None, column_names=None):
            #values = list(self._parse_data_values(row_index, self._get_stat(".ll"), column_coords, self.__file_handles[".data"]))
            values = list(self._parse_data_values_from_file(row_index, self._get_stat("ll"), column_coords, data_prefix, "data"))

            return [f4.decompress(values.pop(0), decompressor[column_name], bigram_size_dict[column_name]) for column_name in column_names]

    # def _get_decompression_dict(self, file_path, column_index_name_dict):
    #     with open(file_path, "rb") as cmpr_file:
    #         return f4.deserialize(cmpr_file.read())

    #     compression_dict = {}
    #     with open(file_path, "rb") as cmpr_file:
    #         for line in cmpr_file:
    #             line_items = line.rstrip(b"\n").split(b"\t")
    #             column_index = fastnumbers.fast_int(line_items[0])

    #             if column_index in column_index_name_dict:
    #                 column_name = column_index_name_dict[column_index]
    #                 compression_dict[column_name] = f4.deserialize(line_items[1])

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
    #     #                 compressed_value = f4.convert_bytes_to_int(values[2])

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