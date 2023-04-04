import os.path

from .Utilities import *

#####################################################
# Filter classes
#####################################################

"""
This class is used to indicate that no filtering should be performed.
"""
class NoFilter:
    def _check_types(self, column_type_dict):
        pass

    def _get_column_name_set(self):
        return set()

    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, bigram_size_dict):
        return row_indices

    def _filter_indexed_column_values(self, file_data, end_index, num_parallel):
        return set(range(end_index))

class __SimpleBaseFilter(NoFilter):
    def __init__(self, column_name, value):
        self._check_argument(column_name, "column_name", str)
        self.column_name = column_name.encode()
        self.value = value

    def _check_argument(self, x, argument_name, expected_value_type):
        if x == None:
            raise Exception(f"A value of None was specified for the {argument_name} argument of the {type(self).__name__} class.")

        if type(x) != expected_value_type:
            raise Exception(f"A variable of {expected_value_type.__name__} type is required for the {argument_name} argument of the {type(self).__name__} class, but the type was {type(x).__name__}.")

    def _get_column_name_set(self):
        return set([self.column_name])

    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, bigram_size_dict):
        with initialize(data_file_path) as file_data:
            coords = column_coords_dict[self.column_name]

            parse_function = get_parse_row_value_function(file_data)

            passing_row_indices = set()
            for i in row_indices:
                if self._passes(parse_function(file_data, i, coords, bigram_size_dict=bigram_size_dict, column_name=self.column_name)):
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

    def _filter_indexed_column_values(self, file_data, end_index, num_parallel):
        index_file_path = get_index_file_path(file_data.data_file_path, self.column_name.decode())

        with initialize(index_file_path) as index_file_data:
            return filter_using_operator(index_file_data, self, end_index, num_parallel)

    def _check_column_types(self, column_index_dict, column_type_dict, expected_column_type, expected_column_type_description):
        if column_type_dict[column_index_dict[self.column_name]] != expected_column_type:
            raise Exception(f"A {type(self).__name__} may only be used with {expected_column_type_description} columns, and {self.column_name.decode()} is not a {expected_column_type_description}.")

    def _passes(self, value):
        return self.oper(self._get_conversion_function()(value), self.value)

class StringFilter(__OperatorFilter):
    def __init__(self, column_name, oper, value):
        self._check_argument(value, "value", str)
        super().__init__(column_name, oper, value.encode())

    def _check_types(self, column_type_dict):
        if column_type_dict[self.column_name] != "s":
            raise Exception(f"A StringFilter may only be used with string columns, and {self.column_name.decode()} is not a string ({column_type_dict[self.column_name]}).")

class FloatFilter(__OperatorFilter):
    def __init__(self, column_name, oper, value):
        self._check_argument(value, "value", float)
        super().__init__(column_name, oper, value)

    def _check_types(self, column_type_dict):
        if column_type_dict[self.column_name] != "f":
            raise Exception(f"A float filter may only be used with float columns, but {self.column_name.decode()} is not a float.")

    def _get_conversion_function(self):
        return fast_float

class IntFilter(__OperatorFilter):
    def __init__(self, column_name, oper, value):
        self._check_argument(value, "value", int)
        super().__init__(column_name, oper, value)

    def _check_types(self, column_type_dict):
        if column_type_dict[self.column_name] != "i":
            raise Exception(f"An integer filter may only be used with integer columns, but {self.column_name.decode()} is not an integer.")

    def _get_conversion_function(self):
        return fast_int

class StartsWithFilter(__SimpleBaseFilter):
    def __init__(self, column_name, value):
        self._check_argument(value, "value", str)
        super().__init__(column_name, value.encode())

    def _filter_indexed_column_values(self, file_data, end_index, num_parallel):
        index_file_path = get_index_file_path(file_data.data_file_path, self.column_name.decode())

        with initialize(index_file_path) as index_file_data:
            return get_passing_row_indices_with_filter(index_file_data, self, end_index, num_parallel)

    def _passes(self, value):
        return value.startswith(self.value)

class EndsWithFilter(StartsWithFilter):
    def _passes(self, value):
        return value.endswith(self.value)

    def _filter_indexed_column_values(self, file_data, end_index, num_parallel):
        custom_index_function = reverse_string
        custom_index_file_path = get_index_file_path(file_data.data_file_path, self.column_name.decode(), custom_index_function)

        if path.exists(custom_index_file_path):
            custom_fltr = StartsWithFilter(self.column_name.decode(), custom_index_function(self.value).decode())

            with initialize(custom_index_file_path) as index_file_data:
                return get_passing_row_indices_with_filter(index_file_data, custom_fltr, end_index, num_parallel)
        else:
            index_file_path = get_index_file_path(file_data.data_file_path, self.column_name.decode())

            with initialize(index_file_path) as index_file_data:
                coords = parse_data_coords(index_file_data, [0, 1])

                return get_passing_row_indices(index_file_data, self, coords[0], coords[1], 0, end_index)

class LikeFilter(__SimpleBaseFilter):
    def __init__(self, column_name, regular_expression):
        super().__init__(column_name, regular_expression)

        self._check_argument(regular_expression, "regular_expression", str)
        self.value = compile(self.value)

    def _filter_indexed_column_values(self, file_data, end_index, num_parallel):
        index_file_path = get_index_file_path(file_data.data_file_path, self.column_name.decode())

        with initialize(index_file_path) as index_file_data:
            coords = parse_data_coords(index_file_data, [0, 1])

            return get_passing_row_indices(index_file_data, self, coords[0], coords[1], 0, end_index)

    def _passes(self, value):
        return self.value.search(value.decode())

class NotLikeFilter(LikeFilter):
    def _passes(self, value):
        return not self.value.search(value.decode())

class HeadFilter(NoFilter):
    def __init__(self, n, select_columns):
        self.n = n
        self.select_columns_set = set([x.encode() for x in select_columns])

    def _get_column_name_set(self):
        return self.select_columns_set

    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, bigram_size_dict):
        return set(range(min(get_num_rows(data_file_path), self.n))) & row_indices

    def _filter_indexed_column_values(self, file_data, end_index, num_parallel):
        return set(range(min(get_num_rows(file_data.data_file_path), self.n)))

class TailFilter(HeadFilter):
    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, bigram_size_dict):
        num_rows = get_num_rows(data_file_path)
        return set(range(num_rows - self.n, num_rows)) & row_indices

    def _filter_indexed_column_values(self, file_data, end_index, num_parallel):
        num_rows = get_num_rows(file_data.data_file_path)
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

    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, bigram_size_dict):
        row_indices_1 = self.filter1._filter_column_values(data_file_path, row_indices, column_coords_dict, bigram_size_dict)
        return self.filter2._filter_column_values(data_file_path, row_indices_1, column_coords_dict, bigram_size_dict)

    def _filter_indexed_column_values(self, file_data, end_index, num_parallel):
        # Currently, only one combination (StringFilter + IntRangeFilter) of two-column filters is supported.
        # TODO: Add more combinations and generalize the code.
        if isinstance(self.filter1, StringFilter) and self.filter1.oper == eq and isinstance(self.filter2, IntRangeFilter):
            two_column_index_name = get_two_column_index_name(self.filter1, self.filter2.filter1)
            two_column_index_file_path = get_index_file_path(file_data.data_file_path, two_column_index_name)

            if path.exists(two_column_index_file_path):
                with initialize(two_column_index_file_path) as index_file_data:
                    coords = parse_data_coords(index_file_data, [0, 1, 2])

                    # Find range for string column
                    lower_position, upper_position = find_bounds_for_range(index_file_data, coords[0], self.filter1, self.filter1, end_index, num_parallel)

                    # Find range for int column
                    lower_position, upper_position = find_bounds_for_range(index_file_data, coords[1], self.filter2.filter1, self.filter2.filter2, upper_position, num_parallel, lower_position)

                    # Get row indices for the overlapping range
                    return retrieve_matching_row_indices(index_file_data, coords[2], (lower_position, upper_position), num_parallel)

        row_indices_1 = self.filter1._filter_indexed_column_values(file_data, end_index, num_parallel)
        row_indices_2 = self.filter2._filter_indexed_column_values(file_data, end_index, num_parallel)

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

    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, bigram_size_dict):
        row_indices_1 = self.filter1._filter_column_values(data_file_path, row_indices, column_coords_dict, bigram_size_dict)
        row_indices_2 = self.filter2._filter_column_values(data_file_path, row_indices - row_indices_1, column_coords_dict, bigram_size_dict)

        return row_indices_1 | row_indices_2

    def _filter_indexed_column_values(self, file_data, end_index, num_parallel):
        row_indices_1 = self.filter1._filter_indexed_column_values(file_data, end_index, num_parallel)
        row_indices_2 = self.filter2._filter_indexed_column_values(file_data, end_index, num_parallel)

        return row_indices_1 | row_indices_2

#    def filter_indexed_column_values_parallel(self, fltr_results_dict):
#        row_indices_1, row_indices_2 = self.get_sub_filter_row_indices(fltr_results_dict)
#        return row_indices_1 | row_indices_2

class __RangeFilter(__CompositeFilter):
    def __init__(self, filter1, filter2):
        super().__init__(filter1, filter2)

        if filter1.value > filter2.value:
            raise Exception("The lower_bound_value must be less than or equal to the upper_bound_value.")

    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, bigram_size_dict):
        return AndFilter(self.filter1, self.filter2)._filter_column_values(data_file_path, row_indices, column_coords_dict, bigram_size_dict)

    def _filter_indexed_column_values(self, file_data, end_index, num_parallel):
        index_file_path = get_index_file_path(file_data.data_file_path, self.filter1.column_name.decode())

        with initialize(index_file_path) as index_file_data:
            coords = parse_data_coords(index_file_data, [0, 1])

            return find_row_indices_for_range(index_file_data, coords[0], coords[1], self.filter1, self.filter2, end_index, num_parallel)

    def _get_conversion_function(self):
        return do_nothing

class FloatRangeFilter(__RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        filter1 = FloatFilter(column_name, ge, lower_bound_value)
        filter2 = FloatFilter(column_name, le, upper_bound_value)

        super().__init__(filter1, filter2)

    def _get_conversion_function(self):
        return fast_float

class IntRangeFilter(__RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        filter1 = IntFilter(column_name, ge, lower_bound_value)
        filter2 = IntFilter(column_name, le, upper_bound_value)

        super().__init__(filter1, filter2)

    def _get_conversion_function(self):
        return fast_int

class StringRangeFilter(__RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        filter1 = StringFilter(column_name, ge, lower_bound_value)
        filter2 = StringFilter(column_name, le, upper_bound_value)

        super().__init__(filter1, filter2)

class FileData:
    def __init__(self, data_file_path, file_handle, file_map_dict, stat_dict, decompression_type, decompressor):
        self.data_file_path = data_file_path
        self.file_handle = file_handle
        self.file_map_dict = file_map_dict
        self.stat_dict = stat_dict
        self.decompression_type = decompression_type
        self.decompressor = decompressor

#####################################################
# Public function(s)
#####################################################

def query(data_file_path, fltr=NoFilter(), select_columns=[], out_file_path=None, out_file_type="tsv", num_parallel=1, tmp_dir_path=None):
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

    if not isinstance(data_file_path, str):
        raise Exception("You must specify data_file_path as an str value.")

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

    if num_parallel > 1:
        global joblib
        joblib = __import__('joblib', globals(), locals())

    with initialize(data_file_path) as file_data:
        # Store column indices and types in dictionaries so we only have to retrieve
        # each once, even if we use the same column in multiple filters.
        select_columns, column_type_dict, column_coords_dict, bigram_size_dict = get_column_meta(file_data, fltr._get_column_name_set(), select_columns)

        fltr._check_types(column_type_dict)

        has_index = len(glob(data_file_path + ".idx_*")) > 0

        if has_index:
#TODO: Remove this stuff if we don't need it after testing on huge files.
#            sub_filters = fltr.get_sub_filters()

#            if num_parallel == 1 or len(sub_filters) == 1:
            keep_row_indices = sorted(fltr._filter_indexed_column_values(file_data, file_data.stat_dict["num_rows"], num_parallel))
#            else:
#                fltr_results_dict = {}

##                for f in sub_filters:
##                    fltr_results_dict[str(f)] = f.filter_indexed_column_values(self.data_file_path, self.compression_level, column_index_dict, column_type_dict, column_coords_dict, self.get_num_rows(), num_parallel)

                # This is a parallelization of the above code.
                # At least in some cases, it slows things down more than it speeds things up.
#                fltr_results = Parallel(n_jobs = num_parallel)(delayed(f.filter_indexed_column_values)(self.data_file_path, self.compression_level, column_index_dict, column_type_dict, column_coords_dict, self.get_num_rows(), num_parallel) for f in sub_filters)
#                for i in range(len(sub_filters)):
#                    fltr_results_dict[str(sub_filters[i])] = fltr_results[i]
#
#                keep_row_indices = sorted(fltr.filter_indexed_column_values_parallel(fltr_results_dict))
        else:
            if num_parallel == 1:
                row_indices = set(range(file_data.stat_dict["num_rows"]))
                keep_row_indices = sorted(fltr._filter_column_values(data_file_path, row_indices, column_coords_dict, bigram_size_dict))
            else:
                # Loop through the rows in parallel and find matching row indices.
                keep_row_indices = sorted(chain.from_iterable(joblib.Parallel(n_jobs = num_parallel)(joblib.delayed(fltr._filter_column_values)(data_file_path, row_indices, column_coords_dict, bigram_size_dict) for row_indices in generate_query_row_chunks(file_data.stat_dict["num_rows"], num_parallel))))

        select_column_coords = [column_coords_dict[name] for name in select_columns]
        parse_function = get_parse_row_values_function(file_data)

        if out_file_path:
            with open(out_file_path, 'wb') as out_file:
                out_file.write(b"\t".join(select_columns) + b"\n") # Header line

                if num_parallel == 1 or len(keep_row_indices) <= num_parallel:
                    # lines_per_chunk = 10
                    # out_lines = []

                    for row_index in keep_row_indices:
                        out_file.write(b"\t".join(parse_function(file_data, row_index, select_column_coords, bigram_size_dict=bigram_size_dict, column_names=select_columns)) + b"\n")
                    #     out_values = parse_function(file_data, row_index, select_column_coords, bigram_size_dict=bigram_size_dict, column_names=select_columns)
                    #     out_lines.append(b"\t".join(out_values))
                    #
                    #     if len(out_lines) % lines_per_chunk == 0:
                    #         out_file.write(b"\n".join(out_lines) + b"\n")
                    #         out_lines = []
                    #
                    # if len(out_lines) > 0:
                    #     out_file.write(b"\n".join(out_lines) + b"\n")
                else:
                    if tmp_dir_path:
                        makedirs(tmp_dir_path, exist_ok=True)
                    else:
                        tmp_dir_path = mkdtemp()

                    tmp_dir_path = fix_dir_path_ending(tmp_dir_path)
                    makedirs(tmp_dir_path, exist_ok=True)

                    row_index_chunks = split_integer_list_into_chunks(keep_row_indices, num_parallel)

                    joblib.Parallel(n_jobs=num_parallel)(
                        joblib.delayed(save_output_line_to_temp)(file_data.data_file_path, chunk_number, row_index_chunk, parse_function, select_column_coords, bigram_size_dict, select_columns, tmp_dir_path) for
                        chunk_number, row_index_chunk in enumerate(row_index_chunks))

                    read_length = 1000000
                    for chunk_number in range(num_parallel):
                        chunk_file_path = f"/{tmp_dir_path}/{chunk_number}"

                        if os.path.exists(chunk_file_path):
                            with open(chunk_file_path, "rb") as read_file:
                                with mmap(read_file.fileno(), 0, prot=PROT_READ) as mmap_handle:
                                    for start_pos in range(0, len(mmap_handle), read_length):
                                        out_file.write(mmap_handle[start_pos:(start_pos + read_length)])

                            remove(f"/{tmp_dir_path}/{chunk_number}")
        else:
            sys.stdout.buffer.write(b"\t".join(select_columns) + b"\n") # Header line

            for row_index in keep_row_indices:
                out_values = parse_function(file_data, row_index, select_column_coords, bigram_size_dict=bigram_size_dict, column_names=select_columns)
                sys.stdout.buffer.write(b"\t".join(out_values))

                if row_index != keep_row_indices[-1]:
                    sys.stdout.buffer.write(b"\n")

def head(data_file_path, n = 10, select_columns=None, out_file_path=None, out_file_type="tsv"):
    if not select_columns:
        select_columns = []

    query(data_file_path, HeadFilter(n, select_columns), select_columns, out_file_path=out_file_path, out_file_type=out_file_type)

def tail(data_file_path, n = 10, select_columns=None, out_file_path=None, out_file_type="tsv"):
    if not select_columns:
        select_columns = []

    query(data_file_path, TailFilter(n, select_columns), select_columns, out_file_path=out_file_path, out_file_type=out_file_type)

def get_num_rows(data_file_path):
    with initialize(data_file_path) as file_data:
        return file_data.stat_dict["num_rows"]

def get_num_cols(data_file_path):
    with initialize(data_file_path) as file_data:
        return file_data.stat_dict["num_cols"]

def get_column_type_from_name(data_file_path, column_name):
    try:
        with initialize(data_file_path) as file_data:
            column_index = get_column_index_from_name(file_data, column_name)
            return get_column_type_from_index(file_data, column_index)
    except:
        raise Exception(f"A column with the name {column_name} does not exist.")

##############################################
# Non-public functions
##############################################

@contextmanager
def initialize(data_file_path):
    with open(data_file_path, 'rb') as file_handle:
        with mmap(file_handle.fileno(), 0, prot=PROT_READ) as mmap_handle:
            file_map_length_string = mmap_handle.readline()
            file_map_length = fast_int(file_map_length_string.rstrip(b"\n"))
            file_map_dict = deserialize(mmap_handle[len(file_map_length_string):(len(file_map_length_string) + file_map_length)])

            stat_dict = {}
            for file_name, abbreviation in FILE_KEY_ABBREVIATIONS_STATS.items():
                if abbreviation in file_map_dict:
                    coordinates = file_map_dict[abbreviation]
                    stat_dict[file_name] = fast_int(mmap_handle[coordinates[0]:coordinates[1]])

            other_dict = {}
            for file_name, abbreviation in FILE_KEY_ABBREVIATIONS_OTHER.items():
                if abbreviation in file_map_dict:
                    coordinates = file_map_dict[abbreviation]
                    other_dict[file_name] = mmap_handle[coordinates[0]:coordinates[1]]

            # Invert the abbreviations used for file keys in the dictionary so they are more human readable.
            file_map_dict2 = {}
            for file_name, abbreviation in FILE_KEY_ABBREVIATIONS_NOCACHE.items():
                if abbreviation in file_map_dict:
                    file_map_dict2[file_name] = file_map_dict[abbreviation]

            decompression_type = None
            decompressor = None
            stat_dict["num_rows"] = -1

            if "cmpr" in other_dict:
                decompression_text = other_dict["cmpr"]

                if decompression_text == b"z":
                    decompression_type = "zstd"
                    # For now, this will be a dictionary with the length of each line
                    decompressor = ZstdDecompressor()
                    stat_dict["ll"] = deserialize(other_dict["ll"])
                    stat_dict["num_rows"] = len(stat_dict["ll"]) - 1
                else:
                    decompression_type = "dictionary"
                    decompressor = deserialize(decompression_text)

            if stat_dict["num_rows"] < 0:
                stat_dict["ll"] = fast_int(other_dict["ll"])

                data_size = file_map_dict2["data"][1] - file_map_dict2["data"][0]
                stat_dict["num_rows"] = fast_int(data_size / stat_dict["ll"])

            cc_size = file_map_dict2["cc"][1] - file_map_dict2["cc"][0]
            stat_dict["num_cols"] = fast_int(cc_size / stat_dict["mccl"]) - 1

            yield FileData(data_file_path, mmap_handle, file_map_dict2, stat_dict, decompression_type, decompressor)

            mmap_handle.close()
            file_handle.close()

def get_column_index_from_name(file_data, column_name):
    position = get_identifier_row_index(file_data, column_name.encode(), file_data.stat_dict["num_cols"], data_prefix="cn")

    if position < 0:
        raise Exception(f"Could not retrieve index because column named {column_name} was not found.")

    return position

def get_column_type_from_index(file_data, column_index):
    return next(parse_data_values_from_file(file_data, column_index, 1, [[0, 1]], "", "ct")).decode()

def get_column_meta(file_data, filter_column_set, select_columns):
    column_type_dict = {}
    column_coords_dict = {}
    column_index_name_dict = {}

    if len(select_columns) == 0:
        num_cols = file_data.stat_dict["num_cols"]
        coords = parse_data_coords(file_data, [0, 1], data_prefix="cn")

        for row_index in range(num_cols):
            values = parse_row_values(file_data, row_index, coords, data_prefix="cn")
            column_name = values[0]
            column_index = fast_int(values[1])

            column_index_name_dict[column_index] = column_name

            if column_name in filter_column_set:
                column_type_dict[column_name] = get_column_type_from_index(file_data, row_index)

        all_coords = parse_data_coords(file_data, range(num_cols))
        for row_index in range(num_cols):
            column_coords_dict[column_index_name_dict[row_index]] = all_coords[row_index]

        all_columns = [x[1] for x in sorted(column_index_name_dict.items())]
        select_columns = all_columns
    else:
        select_columns = [name.encode() for name in select_columns]
        all_columns = list(filter_column_set | set(select_columns))

        column_name_index_dict = {}
        for column_name in all_columns:
            column_index = get_column_index_from_name(file_data, column_name.decode())
            column_name_index_dict[column_name] = column_index
            column_index_name_dict[column_index] = column_name

        for column_name in filter_column_set:
            column_type_dict[column_name] = get_column_type_from_index(file_data, column_name_index_dict[column_name])

        all_column_indices = [column_name_index_dict[name] for name in all_columns]
        all_coords = parse_data_coords(file_data, all_column_indices)

        for i, column_name in enumerate(all_columns):
            column_coords_dict[column_name] = all_coords[i]

    bigram_size_dict = {}

    if file_data.decompression_type == "dictionary":
        for column_name in all_columns:
            bigram_size_dict[column_name] = get_bigram_size(len(file_data.decompressor[column_name]["map"]))

    return select_columns, column_type_dict, column_coords_dict, bigram_size_dict

def generate_query_row_chunks(num_rows, num_parallel):
    rows_per_chunk = ceil(num_rows / num_parallel)

    row_indices = set()

    for row_index in range(num_rows):
        row_indices.add(row_index)

        if len(row_indices) == rows_per_chunk:
            yield row_indices
            row_indices = set()

    if len(row_indices) > 0:
        yield row_indices

def parse_data_coords(file_data, indices, data_prefix=""):
    file_key = "cc"
    data_coords = []
    out_dict = {}
    mccl = file_data.stat_dict[data_prefix + "mccl"]

    for index in indices:
        start_pos = index * mccl
        next_start_pos = start_pos + mccl
        further_next_start_pos = next_start_pos + mccl

        # See if we already have cached the start position.
        if index in out_dict:
            data_start_pos = out_dict[index]
        # If not, retrieve the start position from the cc file and then cache it.
        else:
            data_start_pos = fast_int(parse_data_value_from_file(file_data, 0, 0, [start_pos, next_start_pos], data_prefix, file_key).rstrip(b" "))
            out_dict[index] = data_start_pos

        # See if we already have cached the end position.
        if (index + 1) in out_dict:
            data_end_pos = out_dict[index + 1]
        # If not, retrieve the end position from the cc file and then cache it.
        else:
            data_end_pos = fast_int(parse_data_value_from_file(file_data, 0, 0, [next_start_pos, further_next_start_pos], data_prefix, file_key).rstrip(b" "))
            out_dict[index + 1] = data_end_pos

        data_coords.append([data_start_pos, data_end_pos])

    return data_coords

def parse_data_value_from_file(file_data, start_element, segment_length, coords, data_prefix, file_key):
    start_pos = start_element * segment_length + file_data.file_map_dict[data_prefix + file_key][0]
    return file_data.file_handle[(start_pos + coords[0]):(start_pos + coords[1])]

def parse_data_values_from_string(data_coords, string):
    for coords in data_coords:
        yield string[(coords[0]):(coords[1])].rstrip(b" ")

def parse_data_values_from_file(file_data, start_element, segment_length, data_coords, data_prefix, file_key):
    start_pos = start_element * segment_length + file_data.file_map_dict[data_prefix + file_key][0]

    for coords in data_coords:
        yield file_data.file_handle[(start_pos + coords[0]):(start_pos + coords[1])].rstrip(b" ")

def get_parse_row_value_function(file_data):
    if not file_data.decompression_type:
        return parse_row_value
    elif file_data.decompression_type == "zstd":
        return parse_zstd_compressed_row_value
    else:
        return parse_dictionary_compressed_row_value

def parse_row_value(file_data, row_index, column_coords, data_prefix="", bigram_size_dict=None, column_name=None):
    return parse_data_value_from_file(file_data, row_index, file_data.stat_dict[data_prefix + "ll"], column_coords, data_prefix, "data").rstrip(b" ")

def parse_zstd_compressed_row_value(file_data, row_index, column_coords, data_prefix="", bigram_size_dict=None, column_name=None):
    line_lengths = file_data.stat_dict["ll"]
    line_start_position = line_lengths[row_index]
    line_length = line_lengths[row_index + 1] - line_start_position

    line = parse_data_value_from_file(file_data, line_start_position, 1, [0, line_length], data_prefix, "data")
    line = file_data.decompressor.decompress(line)

    return line[column_coords[0]:column_coords[1]].rstrip(b" ")

def parse_dictionary_compressed_row_value(file_data, row_index, column_coords, data_prefix="", bigram_size_dict=None, column_name=None):
    value = parse_data_value_from_file(file_data, row_index, file_data.stat_dict["ll"], column_coords, data_prefix, "data").rstrip(b" ")
    return decompress(value, file_data.decompressor[column_name], bigram_size_dict[column_name])

def get_parse_row_values_function(file_data):
    if not file_data.decompression_type:
        return parse_row_values
    elif file_data.decompression_type == "zstd":
        return parse_zstd_compressed_row_values
    else:
        return parse_dictionary_compressed_row_values

def parse_row_values(file_data, row_index, column_coords, data_prefix="", bigram_size_dict=None, column_names=None):
    return list(parse_data_values_from_file(file_data, row_index, file_data.stat_dict[data_prefix + "ll"], column_coords, data_prefix, "data"))

def parse_zstd_compressed_row_values(file_data, row_index, column_coords, data_prefix="", bigram_size_dict=None, column_names=None):
    line_start_position = file_data.stat_dict["ll"][row_index]
    line_length = file_data.stat_dict["ll"][row_index + 1] - line_start_position

    line = parse_data_value_from_file(file_data, line_start_position, 1, [0, line_length], data_prefix, "data")
    line = file_data.decompressor.decompress(line)

    return list(parse_data_values_from_string(column_coords, line))

def parse_dictionary_compressed_row_values(file_data, row_index, column_coords, data_prefix="", bigram_size_dict=None, column_names=None):
        values = list(parse_data_values_from_file(file_data, row_index, file_data.stat_dict["ll"], column_coords, data_prefix, "data"))

        return [decompress(values.pop(0), file_data.decompressor[column_name], bigram_size_dict[column_name]) for column_name in column_names]

def parse_values_in_column(file_data, column_name, column_coords, bigram_size_dict):
    num_rows = file_data.stat_dict["num_rows"]
    parse_function = get_parse_row_value_function(file_data)

    values = []
    for row_index in range(num_rows):
        values.append(parse_function(file_data, row_index, column_coords, bigram_size_dict=bigram_size_dict, column_name=column_name))

    return values

def save_output_line_to_temp(data_file_path, chunk_number, row_indices, parse_function, select_column_coords, bigram_size_dict, select_columns, tmp_dir_path):
    with initialize(data_file_path) as file_data:
        with open(f"/{tmp_dir_path}/{chunk_number}", "wb") as tmp_file:
            for row_index in row_indices:
                out_values = parse_function(file_data, row_index, select_column_coords, bigram_size_dict=bigram_size_dict, column_names=select_columns)
                tmp_file.write(b"\t".join(out_values) + b"\n")

# def _get_decompression_dict(self, file_path, column_index_name_dict):
#     with open(file_path, "rb") as cmpr_file:
#         return deserialize(cmpr_file.read())

#     compression_dict = {}
#     with open(file_path, "rb") as cmpr_file:
#         for line in cmpr_file:
#             line_items = line.rstrip(b"\n").split(b"\t")
#             column_index = fast_int(line_items[0])

#             if column_index in column_index_name_dict:
#                 column_name = column_index_name_dict[column_index]
#                 compression_dict[column_name] = deserialize(line_items[1])

#     #for column_index in column_index_name_dict.keys():
#     #     compression_dict[column_index_name_dict[column_index]] = {}
#     #     compression_dict[column_index_name_dict[column_index]]["map"] = {}

#     # with Parser(file_path, fixed_file_extensions=[".data", ".cc"], stats_file_extensions=[".ll", ".mccl"]) as parser:
#     #     coords = parser._parse_data_coords([0, 1, 2])
#     #     num_rows = fast_int((len(parser.get_file_handle(".data")) + 1) / parser.get_stat(".ll"))

#     #     # Use a set for performance reasons
#     #     column_indices_set = set(column_index_name_dict.keys())

#     #     with Parser(f"{self.data_file_path}.cmpr", fixed_file_extensions=[".data", ".cc"], stats_file_extensions=[".ll", ".mccl"]) as parser:
#     #         for row_index in range(num_rows):
#     #             values = parser.__parse_row_values(row_index, coords)
#     #             column_index = fast_int(values[0])

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

def get_identifier_row_index(file_data, query_value, end_index, data_prefix=""):
    if end_index == 0:
        return -1

    value_coords = parse_data_coords(file_data, [0], data_prefix=data_prefix)[0]
    position_coords = parse_data_coords(file_data, [1], data_prefix=data_prefix)[0]

    matching_position = binary_identifier_search(file_data, data_prefix, value_coords, query_value, 0, end_index)

    if matching_position == -1:
        return -1

    return fast_int(parse_row_value(file_data, matching_position, position_coords, data_prefix=data_prefix))

# Searches for a single matching value.
def binary_identifier_search(file_data, data_prefix, value_coords, value_to_find, l, r):
    if r == -1 or l > r:
        return -1

    mid = l + (r - l) // 2
    mid_value = parse_row_value(file_data, mid, value_coords, data_prefix=data_prefix)

    if mid_value == value_to_find:
        # If element is present at the middle itself
        return mid
    elif mid_value > value_to_find:
        return binary_identifier_search(file_data, data_prefix, value_coords, value_to_find, l, mid - 1)
    else:
        # Else the element can only be present in right subarray
        return binary_identifier_search(file_data, data_prefix, value_coords, value_to_find, mid + 1, r)

def filter_using_operator(file_data, fltr, end_index, num_parallel):
    if end_index == 0:
        return set()

    coords = parse_data_coords(file_data, [0, 1])

    if fltr.oper == eq:
        return find_row_indices_for_range(file_data, coords[0], coords[1], fltr, fltr, end_index, num_parallel)
    else:
        if fltr.oper == ne:
            lower_position, upper_position = find_bounds_for_range(file_data, coords[0], fltr, fltr, end_index, num_parallel)

            lower_positions = (0, lower_position)
            upper_positions = (upper_position, end_index)

            lower_row_indices = retrieve_matching_row_indices(file_data, coords[1], lower_positions, num_parallel)
            upper_row_indices = retrieve_matching_row_indices(file_data, coords[1], upper_positions, num_parallel)

            return lower_row_indices | upper_row_indices
        else:
            if fltr.oper == gt:
                positions = find_positions_g(file_data, coords[0], fltr, 0, end_index, le)
            elif fltr.oper == ge:
                positions = find_positions_g(file_data, coords[0], fltr, 0, end_index, lt)
            elif fltr.oper == lt:
                positions = find_positions_l(file_data, coords[0], fltr, 0, end_index, fltr.oper)
            elif fltr.oper == le:
                positions = find_positions_l(file_data, coords[0], fltr, 0, end_index, fltr.oper)

            return retrieve_matching_row_indices(file_data, coords[1], positions, num_parallel)

def find_positions_g(file_data, value_coords, fltr, start_index, end_index, all_false_operator):
    smallest_value = parse_row_value(file_data, start_index, value_coords)
    if smallest_value == b"":
        return start_index, end_index

    if not all_false_operator(fltr._get_conversion_function()(smallest_value), fltr.value):
        return start_index, end_index

    largest_value = parse_row_value(file_data, end_index - 1, value_coords)
    if largest_value == b"":
        return start_index, start_index

    matching_position = search(file_data, value_coords, fltr, 0, end_index, end_index, all_false_operator)

    return matching_position + 1, end_index

def find_positions_l(file_data, value_coords, fltr, start_index, end_index, all_true_operator):
    smallest_value = parse_row_value(file_data, start_index, value_coords)
    if smallest_value == b"":
        return start_index, start_index

    if not all_true_operator(fltr._get_conversion_function()(smallest_value), fltr.value):
        return start_index, start_index

    largest_value = parse_row_value(file_data, end_index - 1, value_coords)
    if largest_value == b"":
        return start_index, end_index

    if all_true_operator(fltr._get_conversion_function()(largest_value), fltr.value):
        return start_index, end_index

    matching_position = search(file_data, value_coords, fltr, 0, end_index, end_index, all_true_operator)

    return start_index, matching_position + 1

# TODO: It might make sense to combine this function with _search_with_filter
#      to avoid duplicating similar code.
def search(file_data, value_coords, fltr, left_index, right_index, overall_end_index, search_operator):
    mid_index = left_index + (right_index - left_index) // 2

    conversion_function = fltr._get_conversion_function()
    mid_value = conversion_function(parse_row_value(file_data, mid_index, value_coords))

    if search_operator(mid_value, fltr.value):
        next_index = mid_index + 1

        if next_index == overall_end_index:
            return mid_index

        next_value = parse_row_value(file_data, mid_index + 1, value_coords)

        # TODO: Does this work if we have a blank data value? Perhaps we can remove it?
        #       Modify to be like _search_with_filter?
        if next_value == b"":
           return mid_index
        elif not search_operator(conversion_function(next_value), fltr.value):
            return mid_index
        else:
            return search(file_data, value_coords, fltr, mid_index, right_index, overall_end_index, search_operator)
    else:
        return search(file_data, value_coords, fltr, left_index, mid_index, overall_end_index, search_operator)

def search_with_filter(file_data, value_coords, left_index, right_index, overall_end_index, fltr):
    mid_index = (left_index + right_index) // 2

    if mid_index == 0:
        return 0

    conversion_function = fltr._get_conversion_function()
    mid_value = conversion_function(parse_row_value(file_data, mid_index, value_coords))

    if fltr._passes(mid_value):
        if mid_index == right_index:
            return mid_index

        next_index = mid_index + 1

        if next_index == overall_end_index:
            return next_index

        next_value = conversion_function(parse_row_value(file_data, next_index, value_coords))

        if fltr._passes(next_value):
            return search_with_filter(file_data, value_coords, next_index, right_index, overall_end_index, fltr)
        else:
            return mid_index + 1
    else:
        if left_index == mid_index:
            return mid_index

        return search_with_filter(file_data, value_coords, left_index, mid_index, overall_end_index, fltr)

def find_matching_row_indices(file_data, position_coords, positions):
    matching_row_indices = set()

    for i in range(positions[0], positions[1]):
        matching_row_indices.add(fast_int(parse_row_value(file_data, i, position_coords)))

    return matching_row_indices

# This is the same as the function above it, but it needs to pass the file path
# to work with joblib.
def find_matching_row_indices_parallel(data_file_path, position_coords, positions):
    with initialize(data_file_path) as file_data:
        matching_row_indices = set()

        for i in range(positions[0], positions[1]):
            matching_row_indices.add(fast_int(parse_row_value(file_data, i, position_coords)))

        return matching_row_indices

def retrieve_matching_row_indices(file_data, position_coords, positions, num_parallel):
    # This is a rough threshold for determine whether it is worth the overhead to parallelize.
    num_indices = positions[1] - positions[0]

    if num_parallel == 1 or num_indices < 100:
        return find_matching_row_indices(file_data, position_coords, positions)
    else:
        chunk_size = ceil(num_indices / num_parallel)

        position_chunks = []
        for i in range(positions[0], positions[1], chunk_size):
            position_chunks.append((i, min(positions[1], i + chunk_size)))

        return set(chain.from_iterable(joblib.Parallel(n_jobs=num_parallel)(
            joblib.delayed(find_matching_row_indices_parallel)(file_data.data_file_path, position_coords, position_chunk)
            for position_chunk in position_chunks))
        )

def find_bounds_for_range(file_data, value_coords, filter1, filter2, end_index, num_parallel, start_index=0):
    lower_positions = find_positions_g(file_data, value_coords, filter1, start_index, end_index, lt)
    upper_positions = find_positions_l(file_data, value_coords, filter2, lower_positions[0], lower_positions[1], le)

    lower_position = max(lower_positions[0], upper_positions[0])
    upper_position = min(lower_positions[1], upper_positions[1])

    return lower_position, upper_position

def find_row_indices_for_range(file_data, value_coords, position_coords, filter1, filter2, end_index, num_parallel):
    lower_position, upper_position = find_bounds_for_range(file_data, value_coords, filter1, filter2, end_index, num_parallel)

    return retrieve_matching_row_indices(file_data, position_coords, (lower_position, upper_position), num_parallel)

def get_passing_row_indices(file_data, fltr, coords_value, coords_position, start_index, end_index):
    passing_row_indices = set()

    for i in range(start_index, end_index):
        if fltr._passes(parse_row_value(file_data, i, coords_value)):
            passing_row_indices.add(fast_int(parse_row_value(file_data, i, coords_position)))

    return passing_row_indices

def get_passing_row_indices_with_filter(file_data, fltr, end_index, num_parallel):
    coords = parse_data_coords(file_data, [0, 1])

    lower_range = find_positions_g(file_data, coords[0], fltr, 0, end_index, lt)

    if lower_range[0] == end_index:
        return set()

    upper_position = search_with_filter(file_data, coords[0], lower_range[0], lower_range[1], end_index, fltr)

    return retrieve_matching_row_indices(file_data, coords[1], (lower_range[0], upper_position), num_parallel)

def get_two_column_index_name(filter1, filter2):
    return "____".join([filter1.column_name.decode(), filter2.column_name.decode()])