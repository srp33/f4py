import os.path

from .Utilities import *

#####################################################
# Classes
#####################################################

class FileData:
    def __init__(self, data_file_path, file_handle, file_map_dict, cache_dict, version, decompression_type, decompressor):
        self.data_file_path = data_file_path
        self.file_handle = file_handle
        self.file_map_dict = file_map_dict
        self.cache_dict = cache_dict
        self.version = version
        self.decompression_type = decompression_type
        self.decompressor = decompressor

    def get_index_number(self, fltr):
        if "i" in self.cache_dict:
            dict_key = []

            for f in fltr.get_sub_filters():
                if hasattr(f, "column_name"):
                    dict_key.append((f.column_name.decode(), isinstance(f, EndsWithFilter)))

            return self.cache_dict["i"].get(tuple(dict_key), -1)

        return -1

"""
This class is used to indicate that no filtering should be performed.
"""
class __BaseFilter:
    def _check_types(self, file_data):
        pass

    def get_sub_filters(self):
        return [self]

    def get_matching_row_indices(self, data_file_path, row_indices, num_parallel):
        raise NotImplementedError

    def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, bigram_size_dict):
        raise NotImplementedError

    def _filter_indexed_column_values(self, file_data, index_number, end_row, num_parallel):
        raise NotImplementedError

class NoFilter(__BaseFilter):
    def get_matching_row_indices(self, data_file_path, row_indices, num_parallel):
        return row_indices

class __SimpleBaseFilter(__BaseFilter):
    def __init__(self, column_name, value):
        self._check_argument(column_name, "column_name", str)
        self.column_name = column_name.encode()
        self.value = value

    def _check_argument(self, x, argument_name, expected_value_type):
        if x == None:
            raise Exception(f"A value of None was specified for the {argument_name} argument of the {type(self).__name__} class.")

        if type(x) != expected_value_type:
            raise Exception(f"A variable of {expected_value_type.__name__} type is required for the {argument_name} argument of the {type(self).__name__} class, but the type was {type(x).__name__}.")

    def get_matching_row_indices(self, data_file_path, row_indices, num_parallel):
        with initialize(data_file_path) as file_data:
            index_number = file_data.get_index_number(self)

            if index_number < 0:
                column_index = get_column_index_from_name(file_data, self.column_name)
                coords = parse_data_coords(file_data, "", [column_index])[0]

                parse_function = get_parse_row_value_function(file_data)

                passing_row_indices = set()
                for i in row_indices:
                    if self._passes(parse_function(file_data, "", i, coords)):
                        passing_row_indices.add(i)

                return passing_row_indices
            else:
                return self.get_matching_row_indices_filter(file_data, index_number, num_parallel)
                # num_rows = file_data.cache_dict["num_rows"]
                # return filter_using_operator(file_data, f"i{index_number}", self, num_rows, num_parallel)

    def get_matching_row_indices_filter(self, file_data, index_number, num_parallel):
        raise NotImplementedError

    def _get_conversion_function(self):
        return do_nothing

#    def __str__(self):
#        return f"{type(self).__name__}____{self.column_name.decode()}____{self.value}"

class __OperatorFilter(__SimpleBaseFilter):
    def __init__(self, column_name, oper, value):
        super().__init__(column_name, value)

        self.oper = oper

    def _passes(self, value):
        return self.oper(self._get_conversion_function()(value), self.value)

    def get_matching_row_indices_filter(self, file_data, index_number, num_parallel):
        return filter_using_operator(file_data, f"i{index_number}", self, file_data.cache_dict["num_rows"], num_parallel)

class StringFilter(__OperatorFilter):
    def __init__(self, column_name, oper, value):
        self._check_argument(value, "value", str)
        super().__init__(column_name, oper, value.encode())

    def _check_types(self, file_data):
        column_index = get_column_index_from_name(file_data, self.column_name)
        column_type = get_column_type_from_index(file_data, column_index)

        if column_type != "s":
            raise Exception(f"A StringFilter may only be used with string columns, and {self.column_name.decode()} is not a string ({column_type}).")

class IntFilter(__OperatorFilter):
    def __init__(self, column_name, oper, value):
        self._check_argument(value, "value", int)
        super().__init__(column_name, oper, value)

    def _check_types(self, file_data):
        column_index = get_column_index_from_name(file_data, self.column_name)
        column_type = get_column_type_from_index(file_data, column_index)

        if column_type != "i":
            raise Exception(f"An IntegerFilter may only be used with integer columns, but {self.column_name.decode()} is not an integer ({column_type}).")

    def _get_conversion_function(self):
        return fast_int

class FloatFilter(__OperatorFilter):
    def __init__(self, column_name, oper, value):
        self._check_argument(value, "value", float)
        super().__init__(column_name, oper, value)

    def _check_types(self, file_data):
        column_index = get_column_index_from_name(file_data, self.column_name)
        column_type = get_column_type_from_index(file_data, column_index)

        if column_type != "f":
            raise Exception(f"A FloatFilter may only be used with float columns, but {self.column_name.decode()} is not a float ({column_type}).")

    def _get_conversion_function(self):
        return fast_float

class __RangeFilter(__SimpleBaseFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        self._check_argument(column_name, "column_name", str)

        if lower_bound_value > upper_bound_value:
            raise Exception("The lower_bound_value must be less than or equal to the upper_bound_value.")

        self.column_name = column_name.encode()
        self.lower_bound_value = lower_bound_value
        self.upper_bound_value = upper_bound_value

    def _passes(self, value):
        typed_value = self._get_conversion_function()(value)
        return self.lower_bound_value <= typed_value <= self.upper_bound_value

    def get_matching_row_indices_filter(self, file_data, index_number, num_parallel):

    # def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, bigram_size_dict):
    #     return AndFilter(self.filter1, self.filter2)._filter_column_values(data_file_path, row_indices, column_coords_dict, bigram_size_dict)
    #
    # def _filter_indexed_column_values(self, file_data, index_number, end_row, num_parallel):
    #     data_file_key = f"i{index_number}"
    #     coords = parse_data_coords(file_data, data_file_key, [0, 1])
    #
    #     return find_row_indices_for_range(file_data, data_file_key, coords[0], coords[1], self.filter1, self.filter2, end_row, num_parallel)

    def _get_conversion_function(self):
        raise NotImplementedError

class StringRangeFilter(__RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        # filter1 = StringFilter(column_name, ge, lower_bound_value)
        # filter2 = StringFilter(column_name, le, upper_bound_value)
        #
        # super().__init__(filter1, filter2)
        super().__init__(column_name, lower_bound_value, upper_bound_value)

    def _check_types(self, file_data):
        column_index = get_column_index_from_name(file_data, self.column_name)
        column_type = get_column_type_from_index(file_data, column_index)

        if column_type != "s":
            raise Exception(f"A StringFilter may only be used with string columns, and {self.column_name.decode()} is not a string ({column_type}).")

    def _get_conversion_function(self):
        return convert_bytes_to_str

class IntRangeFilter(__RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        # filter1 = IntFilter(column_name, ge, lower_bound_value)
        # filter2 = IntFilter(column_name, le, upper_bound_value)
        #
        # super().__init__(filter1, filter2)
        super().__init__(column_name, lower_bound_value, upper_bound_value)

    def _check_types(self, file_data):
        column_index = get_column_index_from_name(file_data, self.column_name)
        column_type = get_column_type_from_index(file_data, column_index)

        if column_type != "i":
            raise Exception(f"An IntRangeFilter may only be used with int columns, and {self.column_name.decode()} is not an integer ({column_type}).")

    def _get_conversion_function(self):
        return fast_int

class FloatRangeFilter(__RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        # filter1 = FloatFilter(column_name, ge, lower_bound_value)
        # filter2 = FloatFilter(column_name, le, upper_bound_value)

        # super().__init__(filter1, filter2)
        super().__init__(column_name, lower_bound_value, upper_bound_value)

    def _check_types(self, file_data):
        column_index = get_column_index_from_name(file_data, self.column_name)
        column_type = get_column_type_from_index(file_data, column_index)

        if column_type != "f":
            raise Exception(f"A FloatRangeFilter may only be used with float columns, and {self.column_name.decode()} is not a float ({column_type}).")

    def _get_conversion_function(self):
        return fast_float

class HeadFilter(__BaseFilter):
    def __init__(self, n):
        self.n = n

    def get_matching_row_indices(self, data_file_path, row_indices, num_parallel):
        return set(range(min(get_num_rows(data_file_path), self.n))) & row_indices

class TailFilter(HeadFilter):
    def get_matching_row_indices(self, data_file_path, row_indices, num_parallel):
        num_rows = get_num_rows(data_file_path)
        return set(range(num_rows - self.n, num_rows)) & row_indices

class StartsWithFilter(__SimpleBaseFilter):
    def __init__(self, column_name, value):
        self._check_argument(value, "value", str)
        super().__init__(column_name, value.encode())

    def _check_types(self, file_data):
        column_index = get_column_index_from_name(file_data, self.column_name)
        column_type = get_column_type_from_index(file_data, column_index)

        if column_type != "s":
            raise Exception(f"A StartsWithFilter or EndsWithFilter may only be used with string columns, and {self.column_name.decode()} is not a string ({column_type}).")

    def _passes(self, value):
        return value.startswith(self.value)

    # def _filter_indexed_column_values(self, file_data, index_number, end_row, num_parallel):
    #     # index_file_path = get_index_file_path(file_data.data_file_path, self.column_name.decode())
    #
    #     # with initialize(index_file_path) as index_file_data:
    #     #     return get_passing_row_indices_with_filter(index_file_data, self, end_row, num_parallel)
    #     return get_passing_row_indices_with_filter(file_data, f"i{index_number}", self, end_row, num_parallel)

class EndsWithFilter(StartsWithFilter):
    def _passes(self, value):
        return value.endswith(self.value)

    # def _filter_indexed_column_values(self, file_data, index_number, end_row, num_parallel):
    #     # index_file_path = get_index_file_path(file_data.data_file_path, self.column_name.decode(), custom_index_type="endswith")
    #     custom_fltr = StartsWithFilter(self.column_name.decode(), reverse_string(self.value).decode())
    #
    #     # with initialize(index_file_path) as index_file_data:
    #     #     return get_passing_row_indices_with_filter(index_file_data, custom_fltr, end_row, num_parallel)
    #     return get_passing_row_indices_with_filter(file_data, f"i{index_number}", custom_fltr, end_row, num_parallel)

# class RegularExpressionFilter(__SimpleBaseFilter):
#     def __init__(self, column_name, pattern):
#         super().__init__(column_name, pattern)
#
#         self._check_argument(pattern, "pattern", str)
#         self.value = compile(self.value)
#
#     def _check_types(self, file_data):
#         column_index = get_column_index_from_name(file_data, self.column_name)
#         column_type = get_column_type_from_index(file_data, column_index)
#
#         if column_type != "s":
#             raise Exception(f"A RegularExpressionFilter may only be used with string columns, and {self.column_name.decode()} is not a string ({column_type}).")
#
#     def _passes(self, value):
#         return self.value.search(value.decode())

# class LikeFilter(__SimpleBaseFilter):
#     def __init__(self, column_name, pattern):
#         super().__init__(column_name, pattern)
#
#         self._check_argument(pattern, "pattern", str)
#
#     def _filter_indexed_column_values(self, file_data, index_number, end_index, num_parallel):
#         index_file_path = get_index_file_path(file_data.data_file_path, self.column_name.decode())
#
#         with initialize(index_file_path) as index_file_data:
#             coords = parse_data_coords(index_file_data, [0, 1])
#
#             return get_passing_row_indices(index_file_data, self, coords[0], coords[1], 0, end_index)

# class NotLikeFilter(LikeFilter):
#     def _passes(self, value):
#         return not self.value.search(value.decode())

class __CompositeFilter(__BaseFilter):
    def __init__(self, filter1, filter2):
        self.filter1 = filter1
        self.filter2 = filter2

    def _check_types(self, file_data):
        self.filter1._check_types(file_data)
        self.filter2._check_types(file_data)

    def get_sub_filters(self):
        return self.filter1.get_sub_filters() + self.filter2.get_sub_filters()

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

    def get_matching_row_indices(self, data_file_path, row_indices, num_parallel):
        row_indices_1 = self.filter1.get_matching_row_indices(data_file_path, row_indices, num_parallel)
        return self.filter2.get_matching_row_indices(data_file_path, row_indices_1, num_parallel)

        # with initialize(data_file_path) as file_data:
        #     column_index1 = get_column_index_from_name(file_data, self.filter1.column_name.decode())
        #     column_index2 = get_column_index_from_name(file_data, self.filter1.column_name.decode())
        #
        #     coords1 = parse_data_coords(file_data, "", [column_index1])[0]
        #     coords2 = parse_data_coords(file_data, "", [column_index2])[0]
        #
        #     parse_function = get_parse_row_value_function(file_data)
        #
        #     passing_row_indices = set()
        #     for i in row_indices:
        #         if self.filter1._passes(parse_function(file_data, "", i, coords1)) and self.filter2._passes(parse_function(file_data, "", i, coords2)):
        #             passing_row_indices.add(i)
        #
        #     return passing_row_indices

    # def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, bigram_size_dict):
    #     row_indices_1 = self.filter1._filter_column_values(data_file_path, row_indices, column_coords_dict, bigram_size_dict)
    #     return self.filter2._filter_column_values(data_file_path, row_indices_1, column_coords_dict, bigram_size_dict)

    # def _filter_indexed_column_values(self, file_data, index_number, end_row, num_parallel):
    #     # TODO: Currently, only one combination (StringFilter + IntRangeFilter) of two-column filters is supported.
    #     #       Add more combinations and generalize the code.
    #
    #     if isinstance(self.filter1, StringFilter) and self.filter1.oper == eq and isinstance(self.filter2, IntRangeFilter):
    #         # index_file_path = f"{file_data.data_file_path}_{'_'.join([self.filter1.column_name.decode(), self.filter2.filter1.column_name.decode()])}.idx"
    #
    #         # if path.exists(index_file_path):
    #         #     with initialize(index_file_path) as index_file_data:
    #
    #         data_file_key = f"i{index_number}"
    #         coords = parse_data_coords(file_data, data_file_key, [0, 1, 2])
    #
    #         # Find range for string column
    #         lower_position, upper_position = find_bounds_for_range(file_data, data_file_key, coords[0], self.filter1, self.filter1, end_row, num_parallel)
    #
    #         # Find range for int column
    #         lower_position, upper_position = find_bounds_for_range(file_data, data_file_key, coords[1], self.filter2.filter1, self.filter2.filter2, upper_position, num_parallel, lower_position)
    #
    #         # Get row indices for the overlapping range
    #         return retrieve_matching_row_indices(file_data, data_file_key, coords[2], (lower_position, upper_position), num_parallel)
    #         # else:
    #         #     raise Exception(f"An index file was expected at {index_file_path}, but that file was not found.")
    #
    #     row_indices_1 = self.filter1._filter_indexed_column_values(file_data, index_number, end_row, num_parallel)
    #     row_indices_2 = self.filter2._filter_indexed_column_values(file_data, index_number, end_row, num_parallel)
    #
    #     return set(row_indices_1) & set(row_indices_2)

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

    def get_matching_row_indices(self, data_file_path, row_indices, num_parallel):
        row_indices_1 = self.filter1.get_matching_row_indices(data_file_path, row_indices, num_parallel)
        row_indices_2 = self.filter2.get_matching_row_indices(data_file_path, row_indices - row_indices_1, num_parallel)

        return row_indices_1 | row_indices_2

    # def _filter_column_values(self, data_file_path, row_indices, column_coords_dict, bigram_size_dict):
    #     row_indices_1 = self.filter1._filter_column_values(data_file_path, row_indices, column_coords_dict, bigram_size_dict)
    #     row_indices_2 = self.filter2._filter_column_values(data_file_path, row_indices - row_indices_1, column_coords_dict, bigram_size_dict)
    #
    #     return row_indices_1 | row_indices_2

    # def _filter_indexed_column_values(self, file_data, index_number, end_row, num_parallel):
    #     row_indices_1 = self.filter1._filter_indexed_column_values(file_data, index_number, end_row, num_parallel)
    #     row_indices_2 = self.filter2._filter_indexed_column_values(file_data, index_number, end_row, num_parallel)
    #
    #     return set(row_indices_1) | set(row_indices_2)

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

    if not isinstance(fltr, __BaseFilter):
        raise Exception("An object that inherits from __BaseFilter must be specified.")

    if out_file_type != "tsv":
        raise Exception("The only out_file_type currently supported is tsv.")

    if num_parallel > 1:
        global joblib
        joblib = __import__('joblib', globals(), locals())

    #TODO: We are parsing the column coords within
    #      the filter code to make things simpler.
    #      We need to calculate whether all filters are indexed,
    #      non are indexed, or there is a mix.
    #      If there is a mix, we should raise an exception (for now).

    write_obj = sys.stdout.buffer
    if out_file_path:
        write_obj = open(out_file_path, 'wb')

    with initialize(data_file_path) as file_data:
        # Make sure the filters match the column types.
        fltr._check_types(file_data)

        # Filter rows based on the data
        keep_row_indices = sorted(fltr.get_matching_row_indices(data_file_path, set(range(file_data.cache_dict["num_rows"])), num_parallel))

        # Parse information about columns to be selected.
        if select_columns:
            if not isinstance(select_columns, list):
                raise Exception("You must specify select_column as a list.")

            # Save header line
            select_columns = [c.encode() for c in select_columns]
            write_obj.write(b"\t".join(select_columns) + b"\n")

            # Get select column indices
            select_column_index_chunks = iterate_single_value([get_column_index_from_name(file_data, c) for c in select_columns])
            num_select_column_chunks = 1
        else:
            # Save header line
            chunk_size = 1000000
            cn_start = file_data.file_map_dict["cn"][0]
            cn_end = file_data.file_map_dict["cn"][1]

            for chunk_start in range(cn_start, cn_end, chunk_size):
                chunk_text = file_data.file_handle[chunk_start:min(chunk_start + chunk_size, cn_end)]
                write_obj.write(chunk_text.replace(b"\n", b"\t"))

            write_obj.write(b"\n")

            # Get select column indices
            max_columns_per_chunk = 100000
            num_cols = file_data.cache_dict["num_cols"]
            select_column_index_chunks = generate_range_chunks(num_cols, max_columns_per_chunk)
            num_select_column_chunks = ceil(num_cols / max_columns_per_chunk)

        # filter_indices = set()
        # for f in fltr.get_sub_filters():
        #     filter_indices.add(file_data.get_index_number(f))

        # select_columns, column_type_dict, column_coords_dict = get_column_meta(file_data, select_columns)

    if num_select_column_chunks == 1:
        save_output_rows(data_file_path, write_obj, keep_row_indices, next(select_column_index_chunks))
    else:
        if tmp_dir_path:
            makedirs(tmp_dir_path, exist_ok=True)
        else:
            tmp_dir_path = mkdtemp()
        tmp_dir_path = fix_dir_path_ending(tmp_dir_path)

        for chunk_number, select_column_indices in enumerate(select_column_index_chunks):
            with open(f"{tmp_dir_path}{chunk_number}", "wb") as chunk_file:
                save_output_rows(data_file_path, chunk_file, keep_row_indices, select_column_indices)

        chunk_file_dict = {}
        for chunk_number in range(num_select_column_chunks):
            chunk_file_dict[chunk_number] = open(f"{tmp_dir_path}{chunk_number}", "rb")

        for row_index in keep_row_indices:
            for chunk_number, chunk_file in chunk_file_dict.items():
                if chunk_number + 1 == num_select_column_chunks:
                    write_obj.write(chunk_file.readline())
                else:
                    write_obj.write(chunk_file.readline().rstrip(b"\n") + b"\t")

        for chunk_number, chunk_file in chunk_file_dict.items():
            chunk_file.close()
            remove(f"{tmp_dir_path}{chunk_number}")

    # TODO: Add logic for parallelizing across column chunks and row chunks.
    # TODO: look for opportunities to change parse_data_coords( to parse_data_coord(

            # filter_tasks = []
            # filter_args = []
            # filter_is_indexed = []
            #
            # for f in fltr.get_sub_filters():
            #     index_number = file_data.get_index_number(fltr)
            #
            #     if index_number > -1:
            #         filter_tasks.append(fltr._filter_indexed_column_values)
            #         filter_args.append([file_data, index_number, file_data.cache_dict["num_rows"]])
            #         filter_is_indexed.append(True)
            #     else:
            #         filter_tasks.append(fltr._filter_column_values)
            #         filter_args.append([data_file_path, column_coords_dict])
            #         filter_is_indexed.append(False)

                # if index_number > -1:
                #     keep_row_indices = sorted(fltr._filter_indexed_column_values(file_data, index_number, file_data.cache_dict["num_rows"], num_parallel))
                # else:
                #     if num_parallel == 1:
                #         row_indices = set(range(file_data.cache_dict["num_rows"]))
                #         keep_row_indices = sorted(fltr._filter_column_values(data_file_path, row_indices, column_coords_dict, bigram_size_dict))
                #     else:
                #         # Loop through the rows in parallel and find matching row indices.
                #         keep_row_indices = sorted(chain.from_iterable(joblib.Parallel(n_jobs = num_parallel)(joblib.delayed(fltr._filter_column_values)(data_file_path, row_indices, column_coords_dict, bigram_size_dict) for row_indices in generate_query_row_chunks(file_data.cache_dict["num_rows"], num_parallel))))

            # If you have no indexes, you should filter in succession so you don't need to parse as many values.
            #     However, each filter should use parallelization.
            # If all filter columns are indexed, you should filter in parallel.
            # If you have a mix of indexed and non-indexed columns, you should filter in parallel.
            #     The non-indexed columns will use parallelization during filtering.
            # However, don't do any parallelization if there are fewer than 1000[?] rows.
            # TODO: Can we actually allow double indexes?
            # TODO: Combine and sort the row indices at the end.
            # TODO: For memory limits, take into account the total number of rows.
            # TODO: Execute them in succession (might be faster)? Or in parallel
            # if len(filter_tasks) == 1 and num_parallel == 1:
                # No parallelization
            # else:
                # Parallelization
            # keep_row_indices = sorted(chain.from_iterable(joblib.Parallel(n_jobs=num_parallel)(joblib.delayed(fltr._filter_column_values)(data_file_path, row_indices, column_coords_dict, bigram_size_dict) for row_indices in generate_query_row_chunks(file_data.cache_dict["num_rows"], num_parallel))))

    #     joblib.Parallel(n_jobs=num_parallel)(
    #         joblib.delayed(save_output_rows)(file_data.data_file_path, f"{tmp_dir_path}/{chunk_number}", row_index_chunk, select_columns, select_column_coords) for
    #         chunk_number, row_index_chunk in enumerate(row_index_chunks))
    #
    #         if os.path.exists(chunk_file_path):
    #             with open(chunk_file_path, "rb") as read_file:
    #                 with mmap(read_file.fileno(), 0, prot=PROT_READ) as mmap_handle:
    #                     for start_pos in range(0, len(mmap_handle), read_length):
    #                         write_obj.write(mmap_handle[start_pos:(start_pos + read_length)])

    #TODO: Make this function a context manager? Or force the following code to happen
    #      when an exit signal is received.
    if out_file_path:
        write_obj.close()

def head(data_file_path, n = 10, select_columns=None, out_file_path=None, out_file_type="tsv"):
    if not select_columns:
        select_columns = []

    query(data_file_path, HeadFilter(n), select_columns, out_file_path=out_file_path, out_file_type=out_file_type)

def tail(data_file_path, n = 10, select_columns=None, out_file_path=None, out_file_type="tsv"):
    if not select_columns:
        select_columns = []

    query(data_file_path, TailFilter(n), select_columns, out_file_path=out_file_path, out_file_type=out_file_type)

def get_version(data_file_path):
    with initialize(data_file_path) as file_data:
        return file_data.version.decode()

def get_num_rows(data_file_path):
    with initialize(data_file_path) as file_data:
        return file_data.cache_dict["num_rows"]

def get_num_cols(data_file_path):
    with initialize(data_file_path) as file_data:
        return file_data.cache_dict["num_cols"]

def get_column_type_from_name(data_file_path, column_name):
    try:
        with initialize(data_file_path) as file_data:
            column_index = get_column_index_from_name(file_data, column_name.encode())

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

            cache_dict = {}

            cache_dict["ccml"] = fast_int(mmap_handle[file_map_dict["ccml"][0]:file_map_dict["ccml"][1]])
            last_cc = file_map_dict["cc"][1]
            cache_dict["ll"] = fast_int(mmap_handle[(last_cc - cache_dict["ccml"]):last_cc])
            cache_dict["num_rows"] = fast_int((file_map_dict[""][1] - file_map_dict[""][0]) / cache_dict["ll"])

            cache_dict["num_cols"] = fast_int((file_map_dict["cc"][1] - file_map_dict["cc"][0]) / cache_dict["ccml"]) - 1

            decompression_type = None
            decompressor = None

            # if "cmpr" in other_dict:
            #     decompression_text = other_dict["cmpr"]
            #
            #     if decompression_text == b"z":
            #         decompression_type = "zstd"
            #         decompressor = ZstdDecompressor()
            #         cache_dict["num_rows"] = fast_int((file_map_dict2["ll"][1] - file_map_dict2["ll"][0]) / cache_dict["mlll"] - 1)
            #     # else:
            #     #     decompression_type = "dictionary"
            #     #     decompressor = deserialize(decompression_text)
            # else:

            # Calculate line length based on last "cnicc" value.
            cache_dict["cniccml"] = fast_int(mmap_handle[file_map_dict["cniccml"][0]:file_map_dict["cniccml"][1]])
            cache_dict["cnill"] = fast_int(mmap_handle[(file_map_dict["cnicc"][1] - cache_dict["cniccml"]):file_map_dict["cnicc"][1]])

            if "i" in file_map_dict:
                cache_dict["i"] = deserialize(mmap_handle[file_map_dict["i"][0]:file_map_dict["i"][1]])

                for key in file_map_dict:
                    if key.endswith("ccml"):
                        cache_dict[key] = fast_int(mmap_handle[file_map_dict[key][0]:file_map_dict[key][1]])

                        last_cc = file_map_dict[key[:-2]][1]
                        cache_dict[key.replace("ccml", "ll")] = fast_int(mmap_handle[(last_cc - cache_dict[key]):last_cc])

            ver = mmap_handle[file_map_dict["ver"][0]:file_map_dict["ver"][1]]

            yield FileData(data_file_path, mmap_handle, file_map_dict, cache_dict, ver, decompression_type, decompressor)

def get_column_index_from_name(file_data, column_name):
    position = get_identifier_row_index(file_data, "cni", column_name, file_data.cache_dict["num_cols"])

    if position < 0:
        raise Exception(f"Could not retrieve index because column named {column_name.decode()} was not found.")

    return position

def get_column_name_from_index(file_data, column_index):
    position = get_identifier_row_index(file_data, "cin", column_index, file_data.cache_dict["num_cols"])

    if position < 0:
        raise Exception(f"Could not retrieve name because column index {column_index.decode()} was not found.")

    return position

def get_column_type_from_index(file_data, column_index):
    return next(parse_data_values_from_file(file_data, "ct", column_index, 1, [[0, 1]])).decode()

def get_value_from_single_column_file(file_handle, content_start_index, segment_length, row_index):
    start_position = content_start_index + row_index * segment_length
    return file_handle[start_position:(start_position + segment_length)].rstrip(b" ")

# def get_column_meta(file_data, filter_column_set, select_columns):
#     column_type_dict = {}
#     column_coords_dict = {}
#     column_index_name_dict = {}
#
#     if len(select_columns) == 0:
#         num_cols = file_data.cache_dict["num_cols"]
#         coords = parse_data_coords(file_data, "cn", [0, 1])
#
#         for row_index in range(num_cols):
#             values = parse_row_values(file_data, "cn", row_index, coords)
#             column_name = values[0]
#             column_index = fast_int(values[1])
#
#             column_index_name_dict[column_index] = column_name
#
#             if column_name in filter_column_set:
#                 column_type_dict[column_name] = get_column_type_from_index(file_data, row_index)
#
#         all_coords = parse_data_coords(file_data, "", range(num_cols))
#         for row_index in range(num_cols):
#             column_coords_dict[column_index_name_dict[row_index]] = all_coords[row_index]
#
#         all_columns = [x[1] for x in sorted(column_index_name_dict.items())]
#         select_columns = all_columns
#     else:
#         select_columns = [name.encode() for name in select_columns]
#         all_columns = list(filter_column_set | set(select_columns))
#
#         column_name_index_dict = {}
#         for column_name in all_columns:
#             column_index = get_column_index_from_name(file_data, column_name.decode())
#             column_name_index_dict[column_name] = column_index
#             column_index_name_dict[column_index] = column_name
#
#         for column_name in filter_column_set:
#             column_type_dict[column_name] = get_column_type_from_index(file_data, column_name_index_dict[column_name])
#
#         all_column_indices = [column_name_index_dict[name] for name in all_columns]
#         all_coords = parse_data_coords(file_data, "", all_column_indices)
#
#         for i, column_name in enumerate(all_columns):
#             column_coords_dict[column_name] = all_coords[i]
#
#     bigram_size_dict = {}
#
#     if file_data.decompression_type == "dictionary":
#         for column_name in all_columns:
#             bigram_size_dict[column_name] = get_bigram_size(len(file_data.decompressor[column_name]["map"]))
#
#     return select_columns, column_type_dict, column_coords_dict, bigram_size_dict

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

def parse_data_coord(file_data, data_file_key, index):
    ccml = file_data.cache_dict[data_file_key + "ccml"]

    start_pos = index * ccml
    next_start_pos = start_pos + ccml
    further_next_start_pos = next_start_pos + ccml

    # Retrieve the start position from the cc file and then cache it.
    data_start_pos = fast_int(parse_data_value_from_file(file_data, data_file_key + "cc", 0, 0, [start_pos, next_start_pos]).rstrip(b" "))

    # Retrieve the end position from the cc file and then cache it.
    data_end_pos = fast_int(parse_data_value_from_file(file_data, data_file_key + "cc", 0, 0, [next_start_pos, further_next_start_pos]).rstrip(b" "))

    return data_start_pos, data_end_pos

def parse_data_coords(file_data, data_file_key, indices):
    data_coords = []
    out_dict = {}

    ccml = file_data.cache_dict[data_file_key + "ccml"]

    for index in indices:
        start_pos = index * ccml
        next_start_pos = start_pos + ccml
        further_next_start_pos = next_start_pos + ccml

        # See if we already have cached the start position.
        if index in out_dict:
            data_start_pos = out_dict[index]
        # If not, retrieve the start position from the cc file and then cache it.
        else:
            data_start_pos = fast_int(parse_data_value_from_file(file_data, data_file_key + "cc", 0, 0, [start_pos, next_start_pos]).rstrip(b" "))
            out_dict[index] = data_start_pos

        # See if we already have cached the end position.
        if (index + 1) in out_dict:
            data_end_pos = out_dict[index + 1]
        # If not, retrieve the end position from the cc file and then cache it.
        else:
            data_end_pos = fast_int(parse_data_value_from_file(file_data, data_file_key + "cc", 0, 0, [next_start_pos, further_next_start_pos]).rstrip(b" "))
            out_dict[index + 1] = data_end_pos

        data_coords.append([data_start_pos, data_end_pos])

    return data_coords

def parse_data_value_from_file(file_data, data_file_key, start_element, segment_length, coords):
    start_pos = start_element * segment_length + file_data.file_map_dict[data_file_key][0]
    return file_data.file_handle[(start_pos + coords[0]):(start_pos + coords[1])]

def parse_data_values_from_string(data_coords, string):
    for coords in data_coords:
        yield string[(coords[0]):(coords[1])].rstrip(b" ")

def parse_data_values_from_file(file_data, data_file_key, start_element, segment_length, data_coords):
    # print(start_element)
    # print(data_file_key)
    # print(segment_length)
    # print(file_data.file_map_dict[data_file_key][0])
    # print(sorted(file_data.file_map_dict.keys()))
    start_pos = start_element * segment_length + file_data.file_map_dict[data_file_key][0]

    for coords in data_coords:
        yield file_data.file_handle[(start_pos + coords[0]):(start_pos + coords[1])].rstrip(b" ")

def get_parse_row_value_function(file_data):
    if not file_data.decompression_type:
        return parse_row_value
    # elif file_data.decompression_type == "zstd":
    else:
        return parse_zstd_compressed_row_value
    # else:
    #     return parse_dictionary_compressed_row_value

def parse_row_value(file_data, data_file_key, row_index, column_coords):
    # print(data_file_key)
    # print(row_index)
    # print(column_coords)
    # print(column_name)
    # print(data_file_key + "ll")
    # print(file_data.cache_dict[data_file_key + "ll"])
    return parse_data_value_from_file(file_data, data_file_key, row_index, file_data.cache_dict[data_file_key + "ll"], column_coords).rstrip(b" ")

def parse_zstd_compressed_row_value(file_data, data_file_key, row_index, column_coords):
    data_start_position = file_data.file_map_dict[""][0]
    line_start_position = data_start_position + fast_int(get_value_from_single_column_file(file_data.file_handle, file_data.file_map_dict["ll"][0], file_data.cache_dict["mlll"], row_index))
    next_line_start_position = data_start_position + fast_int(get_value_from_single_column_file(file_data.file_handle, file_data.file_map_dict["ll"][0], file_data.cache_dict["mlll"], row_index + 1))

    chunk_size = 100000
    all_text = b""
    # TODO: all_text could get really large. Find a way to use a generator.
    for i in range(line_start_position, next_line_start_position, chunk_size):
        line_end_position = min(i + chunk_size, next_line_start_position)
        text = file_data.decompressor.decompress(file_data.file_handle[i:line_end_position])
        all_text += text

    return all_text[column_coords[0]:column_coords[1]].rstrip(b" ")
    # import sys
    # sys.exit()
    # line_lengths = file_data.cache_dict["ll"]
    # line_start_position = line_lengths[row_index]
    # line_length = line_lengths[row_index + 1] - line_start_position
    #
    # line = parse_data_value_from_file(file_data, line_start_position, 1, [0, line_length], data_prefix, "data")
    # line = file_data.decompressor.decompress(line)
    #
    # return line[column_coords[0]:column_coords[1]].rstrip(b" ")

# def parse_dictionary_compressed_row_value(file_data, data_file_key, row_index, column_coords, bigram_size_dict=None, column_name=None):
#     value = parse_data_value_from_file(file_data, data_file_key, row_index, file_data.cache_dict["ll"], column_coords).rstrip(b" ")
#     return decompress(value, file_data.decompressor[column_name], bigram_size_dict[column_name])

def get_parse_row_values_function(file_data):
    if not file_data.decompression_type:
        return parse_row_values
    # elif file_data.decompression_type == "zstd":
    else:
        return parse_zstd_compressed_row_values
    # else:
    #     return parse_dictionary_compressed_row_values

def parse_row_values(file_data, data_file_key, row_index, column_coords):
    return list(parse_data_values_from_file(file_data, data_file_key, row_index, file_data.cache_dict[data_file_key + "ll"], column_coords))

def parse_zstd_compressed_row_values(file_data, data_file_key, row_index, column_coords):
    data_start_position = file_data.file_map_dict[""][0]
    line_start_position = data_start_position + fast_int(get_value_from_single_column_file(file_data.file_handle, file_data.file_map_dict["ll"][0], file_data.cache_dict["mlll"], row_index))
    next_line_start_position = data_start_position + fast_int(get_value_from_single_column_file(file_data.file_handle, file_data.file_map_dict["ll"][0], file_data.cache_dict["mlll"], row_index + 1))
    # line_length = next_line_start_position - line_start_position

    chunk_size = 100000
    all_text = b""
    #TODO: all_text could get really large. Find a way to use a generator.
    for i in range(line_start_position, next_line_start_position, chunk_size):
        line_end_position = min(i + chunk_size, next_line_start_position)
        text = file_data.decompressor.decompress(file_data.file_handle[i:line_end_position])
        all_text += text

    return list(parse_data_values_from_string(column_coords, all_text))

# def parse_dictionary_compressed_row_values(file_data, data_file_key, row_index, column_coords, bigram_size_dict=None, column_names=None):
#         values = list(parse_data_values_from_file(file_data, data_file_key, row_index, file_data.cache_dict["ll"], column_coords))
#
#         return [decompress(values.pop(0), file_data.decompressor[column_name], bigram_size_dict[column_name]) for column_name in column_names]

def parse_values_in_column(file_data, data_file_key, column_name, column_coords, bigram_size_dict):
    num_rows = file_data.cache_dict["num_rows"]
    parse_function = get_parse_row_value_function(file_data)

    values = []
    for row_index in range(num_rows):
        values.append(parse_function(file_data, data_file_key, row_index, column_coords))

    return values

def save_output_rows(in_file_path, out_file_path_or_obj, row_indices, column_indices):
    # if isinstance(out_file_path_or_obj, str):
    #     out_file_path_or_obj = open(out_file_path_or_obj, "wb")

    with initialize(in_file_path) as file_data:
        select_column_coords = parse_data_coords(file_data, "", column_indices)
        parse_row_values_function = get_parse_row_values_function(file_data)

        for row_index in row_indices:
            out_file_path_or_obj.write(b"\t".join(parse_row_values_function(file_data, "", row_index, select_column_coords)) + b"\n")

        # for row_index in row_indices:
        #     if select_columns:
        #         parse_row_values_function = get_parse_row_values_function(file_data)
        #         out_file_path_or_obj.write(b"\t".join(parse_row_values_function(file_data, "", row_index, select_column_coords)) + b"\n")
        #     else:
        #         parse_row_value_function = get_parse_row_value_function(file_data)
        #
        #         out_items = []
        #         for select_col_index in range(file_data.cache_dict["num_cols"] - 1):
        #             select_col_coords = parse_data_coord(file_data, "", select_col_index)
        #             out_items.append(parse_row_value_function(file_data, "", row_index, select_col_coords))
        #
        #             if len(out_items) == 1000000:
        #                 out_file_path_or_obj.write(b"\t".join(out_items) + b"\t")
        #                 out_items = []
        #
        #         if len(out_items) > 0:
        #             out_file_path_or_obj.write(b"\t".join(out_items) + b"\t")
        #
        #         last_col_coords = parse_data_coord(file_data, "", file_data.cache_dict['num_cols'] - 1)
        #         last_col_item = parse_row_value_function(file_data, '', row_index, last_col_coords)
        #         out_file_path_or_obj.write(last_col_item + b"\n")

    # if hasattr(out_file_path_or_obj, "close"):
    #     out_file_path_or_obj.close()

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

#     # with Parser(file_path, fixed_file_extensions=[".data", ".cc"], stats_file_extensions=[".ll", ".ccml"]) as parser:
#     #     coords = parser._parse_data_coords([0, 1, 2])
#     #     num_rows = fast_int((len(parser.get_file_handle(".data")) + 1) / parser.get_stat(".ll"))

#     #     # Use a set for performance reasons
#     #     column_indices_set = set(column_index_name_dict.keys())

#     #     with Parser(f"{self.data_file_path}.cmpr", fixed_file_extensions=[".data", ".cc"], stats_file_extensions=[".ll", ".ccml"]) as parser:
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

def get_identifier_row_index(file_data, data_file_key, query_value, end_index):
    if end_index == 0:
        return -1

    value_coords = parse_data_coords(file_data, data_file_key, [0])[0]
    position_coords = parse_data_coords(file_data, data_file_key, [1])[0]

    matching_position = binary_identifier_search(file_data, data_file_key, value_coords, query_value, 0, end_index)

    if matching_position == -1:
        return -1

    return fast_int(parse_row_value(file_data, data_file_key, matching_position, position_coords))

# Searches for a single matching value.
def binary_identifier_search(file_data, data_file_key, value_coords, value_to_find, l, r):
    if r == -1 or l > r:
        return -1

    mid = l + (r - l) // 2
    mid_value = parse_row_value(file_data, data_file_key, mid, value_coords)

    if mid_value == value_to_find:
        # If element is present at the middle itself
        return mid
    elif mid_value > value_to_find:
        return binary_identifier_search(file_data, data_file_key, value_coords, value_to_find, l, mid - 1)
    else:
        # Else the element can only be present in right subarray
        return binary_identifier_search(file_data, data_file_key, value_coords, value_to_find, mid + 1, r)

def filter_using_operator(file_data, data_file_key, fltr, end_row, num_parallel):
    if end_row == 0:
        return set()

    coords = parse_data_coords(file_data, data_file_key, [0, 1])

    if fltr.oper == eq:
        return find_row_indices_for_range(file_data, data_file_key, coords[0], coords[1], fltr, fltr, end_row, num_parallel)
    else:
        if fltr.oper == ne:
            lower_position, upper_position = find_bounds_for_range(file_data, data_file_key, coords[0], fltr, fltr, end_row, num_parallel)

            lower_positions = (0, lower_position)
            upper_positions = (upper_position, end_row)

            lower_row_indices = retrieve_matching_row_indices(file_data, data_file_key, coords[1], lower_positions, num_parallel)
            upper_row_indices = retrieve_matching_row_indices(file_data, data_file_key, coords[1], upper_positions, num_parallel)

            return lower_row_indices | upper_row_indices
        else:
            if fltr.oper == gt:
                positions = find_positions_g(file_data, data_file_key, coords[0], fltr, 0, end_row, le)
            elif fltr.oper == ge:
                positions = find_positions_g(file_data, data_file_key, coords[0], fltr, 0, end_row, lt)
            elif fltr.oper == lt:
                positions = find_positions_l(file_data, data_file_key, coords[0], fltr, 0, end_row, fltr.oper)
            elif fltr.oper == le:
                positions = find_positions_l(file_data, data_file_key, coords[0], fltr, 0, end_row, fltr.oper)

            return retrieve_matching_row_indices(file_data, data_file_key, coords[1], positions, num_parallel)

def find_positions_g(file_data, data_file_key, value_coords, fltr, start_index, end_index, all_false_operator):
    smallest_value = parse_row_value(file_data, data_file_key, start_index, value_coords)
    if smallest_value == b"":
        return start_index, end_index

    if not all_false_operator(fltr._get_conversion_function()(smallest_value), fltr.value):
        return start_index, end_index

    largest_value = parse_row_value(file_data, data_file_key, end_index - 1, value_coords)
    if largest_value == b"":
        return start_index, start_index

    matching_position = search(file_data, data_file_key, value_coords, fltr, 0, end_index, end_index, all_false_operator)

    return matching_position + 1, end_index

def find_positions_l(file_data, data_file_key, value_coords, fltr, start_index, end_index, all_true_operator):
    smallest_value = parse_row_value(file_data, data_file_key, start_index, value_coords)
    if smallest_value == b"":
        return start_index, start_index

    if not all_true_operator(fltr._get_conversion_function()(smallest_value), fltr.value):
        return start_index, start_index

    largest_value = parse_row_value(file_data, data_file_key, end_index - 1, value_coords)
    if largest_value == b"":
        return start_index, end_index

    if all_true_operator(fltr._get_conversion_function()(largest_value), fltr.value):
        return start_index, end_index

    matching_position = search(file_data, data_file_key, value_coords, fltr, 0, end_index, end_index, all_true_operator)

    return start_index, matching_position + 1

# TODO: It might make sense to combine this function with _search_with_filter
#      to avoid duplicating similar code.
def search(file_data, data_file_key, value_coords, fltr, left_index, right_index, overall_end_index, search_operator):
    mid_index = left_index + (right_index - left_index) // 2

    conversion_function = fltr._get_conversion_function()
    mid_value = conversion_function(parse_row_value(file_data, data_file_key, mid_index, value_coords))

    if search_operator(mid_value, fltr.value):
        next_index = mid_index + 1

        if next_index == overall_end_index:
            return mid_index

        next_value = parse_row_value(file_data, data_file_key, mid_index + 1, value_coords)

        # TODO: Does this work if we have a blank data value? Perhaps we can remove it?
        #       Modify to be like _search_with_filter?
        if next_value == b"":
           return mid_index
        elif not search_operator(conversion_function(next_value), fltr.value):
            return mid_index
        else:
            return search(file_data, data_file_key, value_coords, fltr, mid_index, right_index, overall_end_index, search_operator)
    else:
        return search(file_data, data_file_key, value_coords, fltr, left_index, mid_index, overall_end_index, search_operator)

def search_with_filter(file_data, data_file_key, value_coords, left_index, right_index, overall_end_index, fltr):
    mid_index = (left_index + right_index) // 2

    if mid_index == 0:
        return 0

    conversion_function = fltr._get_conversion_function()
    mid_value = conversion_function(parse_row_value(file_data, data_file_key, mid_index, value_coords))

    if fltr._passes(mid_value):
        if mid_index == right_index:
            return mid_index

        next_index = mid_index + 1

        if next_index == overall_end_index:
            return next_index

        next_value = conversion_function(parse_row_value(file_data, data_file_key, next_index, value_coords))

        if fltr._passes(next_value):
            return search_with_filter(file_data, data_file_key, value_coords, next_index, right_index, overall_end_index, fltr)
        else:
            return mid_index + 1
    else:
        if left_index == mid_index:
            return mid_index

        return search_with_filter(file_data, data_file_key, value_coords, left_index, mid_index, overall_end_index, fltr)

def find_matching_row_indices(file_data, data_file_key, position_coords, positions):
    matching_row_indices = set()

    for i in range(positions[0], positions[1]):
        matching_row_indices.add(fast_int(parse_row_value(file_data, data_file_key, i, position_coords)))

    return matching_row_indices

# This is the same as the function above it, but it needs to pass the file path
# to work with joblib.
def find_matching_row_indices_parallel(data_file_path, data_file_key, position_coords, positions):
    with initialize(data_file_path) as file_data:
        matching_row_indices = set()

        for i in range(positions[0], positions[1]):
            matching_row_indices.add(fast_int(parse_row_value(file_data, data_file_key, i, position_coords)))

        return matching_row_indices

def retrieve_matching_row_indices(file_data, data_file_key, position_coords, positions, num_parallel):
    # This is a rough threshold for determine whether it is worth the overhead to parallelize.
    num_indices = positions[1] - positions[0]

    if num_parallel == 1 or num_indices < 100:
        return find_matching_row_indices(file_data, data_file_key, position_coords, positions)
    else:
        chunk_size = ceil(num_indices / num_parallel)

        position_chunks = []
        for i in range(positions[0], positions[1], chunk_size):
            position_chunks.append((i, min(positions[1], i + chunk_size)))

        return set(chain.from_iterable(joblib.Parallel(n_jobs=num_parallel)(
            joblib.delayed(find_matching_row_indices_parallel)(file_data.data_file_path, data_file_key, position_coords, position_chunk)
            for position_chunk in position_chunks))
        )

def find_bounds_for_range(file_data, data_file_key, value_coords, filter1, filter2, end_index, num_parallel, start_index=0):
    lower_positions = find_positions_g(file_data, data_file_key, value_coords, filter1, start_index, end_index, lt)
    upper_positions = find_positions_l(file_data, data_file_key, value_coords, filter2, lower_positions[0], lower_positions[1], le)

    lower_position = max(lower_positions[0], upper_positions[0])
    upper_position = min(lower_positions[1], upper_positions[1])

    return lower_position, upper_position

def find_row_indices_for_range(file_data, data_file_key, value_coords, position_coords, filter1, filter2, end_index, num_parallel):
    lower_position, upper_position = find_bounds_for_range(file_data, data_file_key, value_coords, filter1, filter2, end_index, num_parallel)

    return retrieve_matching_row_indices(file_data, data_file_key, position_coords, (lower_position, upper_position), num_parallel)

def get_passing_row_indices(file_data, data_file_key, fltr, coords_value, coords_position, start_index, end_index):
    passing_row_indices = set()

    for i in range(start_index, end_index):
        if fltr._passes(parse_row_value(file_data, data_file_key, i, coords_value)):
            passing_row_indices.add(fast_int(parse_row_value(file_data, data_file_key, i, coords_position)))

    return passing_row_indices

def get_passing_row_indices_with_filter(file_data, data_file_key, fltr, end_index, num_parallel):
    coords = parse_data_coords(file_data, data_file_key, [0, 1])

    lower_range = find_positions_g(file_data, data_file_key, coords[0], fltr, 0, end_index, lt)

    if lower_range[0] == end_index:
        return set()

    if lower_range[1] == end_index:
        upper_position = end_index
    else:
        upper_position = search_with_filter(file_data, data_file_key, coords[0], lower_range[0], lower_range[1], end_index, fltr)

    return retrieve_matching_row_indices(file_data, data_file_key, coords[1], (lower_range[0], upper_position), num_parallel)

def get_two_column_index_name(filter1, filter2):
    return "____".join([filter1.column_name.decode(), filter2.column_name.decode()])