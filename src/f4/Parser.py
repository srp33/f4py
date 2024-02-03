import operator

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

"""
This class is used to indicate that no filtering should be performed.
"""
class _BaseFilter:
    def _check_types(self, file_data):
        pass

    def get_matching_row_indices(self, file_data, row_indices, num_parallel):
        raise NotImplementedError

    def get_matching_row_indices_indexed(self, file_data, index_number, cc_value_column_index, cc_position_column_index, start_search_position, end_search_position, retrieve_row_indices, num_parallel):
        raise NotImplementedError

class NoFilter(_BaseFilter):
    def get_matching_row_indices(self, file_data, row_indices, num_parallel):
        return row_indices

class _SimpleBaseFilter(_BaseFilter):
    def __init__(self, column_name, value):
        self._check_argument(column_name, "column_name", str)
        self.column_name = column_name.encode()
        self.value = value

    def _check_argument(self, x, argument_name, expected_value_type):
        if x == None:
            raise Exception(f"A value of None was specified for the {argument_name} argument of the {type(self).__name__} class.")

        if type(x) != expected_value_type:
            raise Exception(f"A variable of {expected_value_type.__name__} type is required for the {argument_name} argument of the {type(self).__name__} class, but the type was {type(x).__name__}.")

    def get_matching_row_indices(self, file_data, row_indices, num_parallel):
        index_number = self._get_index_number(file_data)

        if index_number < 0:
            column_index = get_column_index_from_name(file_data, self.column_name)
            coords = parse_data_coord(file_data, "", column_index)
            parse_function = get_parse_row_value_function(file_data)

            if row_indices is None:
                num_rows = file_data.cache_dict["num_rows"]

                if num_rows <= 100 or num_parallel == 1:
                    return self._do_row_indices_pass(file_data, coords, parse_function, range(num_rows))
                else:
                    return set(chain.from_iterable(joblib.Parallel(n_jobs=num_parallel)(
                        joblib.delayed(self._do_row_indices_pass_parallel)(file_data.data_file_path, "", coords, parse_function, chunk_row_indices)
                        for chunk_row_indices in generate_range_chunks(num_rows, 1000001)))
                    )
            else:
                if len(row_indices) <= 100 or num_parallel == 1:
                    return self._do_row_indices_pass(file_data, coords, parse_function, row_indices)
                else:
                    return set(chain.from_iterable(joblib.Parallel(n_jobs=num_parallel)(
                        joblib.delayed(self._do_row_indices_pass_parallel)(file_data.data_file_path, "", coords, parse_function, chunk_row_indices)
                        for chunk_row_indices in split_list_into_chunks(list(row_indices), 1000001)))
                    )
        else:
            matching_row_indices = self.get_matching_row_indices_indexed(file_data, index_number, 0, 1, 0, file_data.cache_dict["num_rows"], True, num_parallel)

            if row_indices is None:
                return matching_row_indices

            return matching_row_indices & row_indices

    def _get_index_number(self, file_data):
        if "i" in file_data.cache_dict:
            if hasattr(self, "column_name"):
                dict_key = ((self.column_name.decode(), isinstance(self, EndsWithFilter)), )

                return file_data.cache_dict["i"].get(dict_key, -1)

        return -1

    def _do_row_indices_pass(self, file_data, coords, parse_function, row_indices_to_check):
        passing_row_indices = set()

        for i in row_indices_to_check:
            if self._passes(parse_function(file_data, "", i, coords)):
                passing_row_indices.add(i)

        return passing_row_indices

    def _do_row_indices_pass_parallel(self, data_file_path, data_file_key, coords, parse_function, row_indices_to_check):
        passing_row_indices = set()

        with initialize(data_file_path) as file_data:
            for i in row_indices_to_check:
                if self._passes(parse_function(file_data, data_file_key, i, coords)):
                    passing_row_indices.add(i)

        return passing_row_indices

    def _get_conversion_function(self):
        return do_nothing

#    def __str__(self):
#        return f"{type(self).__name__}____{self.column_name.decode()}____{self.value}"

class _OperatorFilter(_SimpleBaseFilter):
    def __init__(self, column_name, oper, value):
        super().__init__(column_name, value)

        self.oper = oper

    def _passes(self, value):
        return self.oper(self._get_conversion_function()(value), self.value)

    def get_matching_row_indices_indexed(self, file_data, index_number, cc_value_column_index, cc_position_column_index, start_search_position, end_search_position, retrieve_row_indices, num_parallel):
        return filter_using_operator(file_data, f"i{index_number}", self, cc_value_column_index, cc_position_column_index, start_search_position, end_search_position, retrieve_row_indices, num_parallel)

class StringFilter(_OperatorFilter):
    def __init__(self, column_name, oper, value):
        self._check_argument(value, "value", str)
        super().__init__(column_name, oper, value.encode())

    def _check_types(self, file_data):
        column_index = get_column_index_from_name(file_data, self.column_name)
        column_type = get_column_type_from_index(file_data, column_index)

        if column_type != "s":
            raise Exception(f"A StringFilter may only be used with string columns, and {self.column_name.decode()} is not a string ({column_type}).")

class IntFilter(_OperatorFilter):
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

class FloatFilter(_OperatorFilter):
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

class _RangeFilter(_SimpleBaseFilter):
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

    def get_matching_row_indices_indexed(self, file_data, index_number, cc_value_column_index, cc_position_column_index, start_search_position, end_search_position, retrieve_row_indices, num_parallel):
        data_file_key = f"i{index_number}"
        coords = parse_data_coords(file_data, data_file_key, [cc_value_column_index, cc_position_column_index])

        return find_row_indices_for_range(file_data, data_file_key, coords[0], coords[1], self._get_index_filter_1(), self._get_index_filter_2(), start_search_position, end_search_position, retrieve_row_indices, num_parallel)

    def _get_conversion_function(self):
        raise NotImplementedError

class StringRangeFilter(_RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        super().__init__(column_name, lower_bound_value, upper_bound_value)

    def _check_types(self, file_data):
        column_index = get_column_index_from_name(file_data, self.column_name)
        column_type = get_column_type_from_index(file_data, column_index)

        if column_type != "s":
            raise Exception(f"A StringFilter may only be used with string columns, and {self.column_name.decode()} is not a string ({column_type}).")

    def _get_conversion_function(self):
        return convert_bytes_to_str

    def _get_index_filter_1(self):
        return StringFilter(self.column_name.decode(), ge, self.lower_bound_value)

    def _get_index_filter_2(self):
        return StringFilter(self.column_name.decode(), le, self.upper_bound_value)

class IntRangeFilter(_RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        super().__init__(column_name, lower_bound_value, upper_bound_value)

    def _check_types(self, file_data):
        column_index = get_column_index_from_name(file_data, self.column_name)
        column_type = get_column_type_from_index(file_data, column_index)

        if column_type != "i":
            raise Exception(f"An IntRangeFilter may only be used with int columns, and {self.column_name.decode()} is not an integer ({column_type}).")

    def _get_conversion_function(self):
        return fast_int

    def _get_index_filter_1(self):
        return IntFilter(self.column_name.decode(), ge, self.lower_bound_value)

    def _get_index_filter_2(self):
        return IntFilter(self.column_name.decode(), le, self.upper_bound_value)

class FloatRangeFilter(_RangeFilter):
    def __init__(self, column_name, lower_bound_value, upper_bound_value):
        super().__init__(column_name, lower_bound_value, upper_bound_value)

    def _check_types(self, file_data):
        column_index = get_column_index_from_name(file_data, self.column_name)
        column_type = get_column_type_from_index(file_data, column_index)

        if column_type != "f":
            raise Exception(f"A FloatRangeFilter may only be used with float columns, and {self.column_name.decode()} is not a float ({column_type}).")

    def _get_conversion_function(self):
        return fast_float

    def _get_index_filter_1(self):
        return FloatFilter(self.column_name.decode(), ge, self.lower_bound_value)

    def _get_index_filter_2(self):
        return FloatFilter(self.column_name.decode(), le, self.upper_bound_value)

class HeadFilter(_BaseFilter):
    def __init__(self, n):
        self.n = n

    def get_matching_row_indices(self, file_data, row_indices, num_parallel):
        if row_indices is None:
            return set(range(min(file_data.cache_dict["num_rows"], self.n)))
        else:
            return set(range(min(file_data.cache_dict["num_rows"], self.n))) & row_indices

class TailFilter(HeadFilter):
    def get_matching_row_indices(self, file_data, row_indices, num_parallel):
        num_rows = file_data.cache_dict["num_rows"]
        matching_row_indices = set(range(num_rows - self.n, num_rows))

        if row_indices is None:
            return matching_row_indices
        else:
            return matching_row_indices & row_indices

class StartsWithFilter(_SimpleBaseFilter):
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

    def get_matching_row_indices_indexed(self, file_data, index_number, cc_value_column_index, cc_position_column_index, start_search_position, end_search_position, retrieve_row_indices, num_parallel):
        return get_passing_row_indices_with_filter(file_data, f"i{index_number}", self, cc_value_column_index, cc_position_column_index, start_search_position, end_search_position, retrieve_row_indices, num_parallel)

class EndsWithFilter(StartsWithFilter):
    def _passes(self, value):
        return value.endswith(self.value)

class _CompositeFilter(_BaseFilter):
    def __init__(self, filters):
        for f in filters:
            if not isinstance(f, _BaseFilter):
                raise Exception("The inputs to this filter must be filters.")

        self.filters = filters

    def _check_types(self, file_data):
        for f in self.filters:
            f._check_types(file_data)

class AndFilter(_CompositeFilter):
    """
    This class is used to construct a filter with multiple sub-filters that must all evaluate to True.
    Order does matter; filter1 is applied first. Any rows that remain after filter1 has been applied
    will be sent to filter2.

    Args:
        filter1: The first filter to be evaluated.
        filter2: The second filter to be evaluated.
    """
    def __init__(self, *filters):
        super().__init__(filters)

    def get_matching_row_indices(self, file_data, row_indices, num_parallel):
        index_number = self._get_index_number(file_data)

        if index_number < 0:
            for f in self.filters:
                row_indices = f.get_matching_row_indices(file_data, row_indices, num_parallel)

            return row_indices
        else:
            num_filters = len(self.filters)
            rows_start_end = (0, file_data.cache_dict["num_rows"])

            for i, f in enumerate(self.filters[:-1]):
                rows_start_end = f.get_matching_row_indices_indexed(file_data, index_number, i, num_filters, rows_start_end[0], rows_start_end[1], False, num_parallel)

            return self.filters[-1].get_matching_row_indices_indexed(file_data, index_number, num_filters - 1, num_filters, rows_start_end[0], rows_start_end[1], True, num_parallel)

    def _get_index_number(self, file_data):
        if "i" in file_data.cache_dict:
            # Multi-column indices are only supported in certain situations.
            # Composite sub-filters are not allowed.
            for f in self.filters:
                if not isinstance(f, _SimpleBaseFilter):
                    return -1

            # All but the last filter must use oper.eq.
            for f in self.filters[:-1]:
                if not isinstance(f, _OperatorFilter) or f.oper != operator.eq:
                    return -1

            dict_key = []

            # Find the longest match that there is. It can return shorter than the full match.
            for i, f in enumerate(self.filters):
                dict_key.append((f.column_name.decode(), isinstance(f, EndsWithFilter)))

            return file_data.cache_dict["i"].get(tuple(dict_key), -1)

        return -1

class OrFilter(_CompositeFilter):
    """
    This class is used to construct a filter with multiple sub-filters. At least one must evaluate to True.
    Order does matter; filter1 is applied first. Any rows that did not pass after filter1 has been
    applied will be sent to filter2.

    Args:
        *args (list): A variable number of filters that should be evaluated. At least two filters must be specified.
    """
    def __init__(self, *filters):
        super().__init__(filters)

    def get_matching_row_indices(self, file_data, row_indices, num_parallel):
        if row_indices is None:
            row_indices = set(range(file_data.cache_dict["num_rows"]))

        are_all_operator_filters = True
        for f in self.filters:
            if f is not _OperatorFilter:
                are_all_operator_filters = False
                break

        row_indices_out = set()

        if are_all_operator_filters:
            column_indices = [get_column_index_from_name(file_data, f.column_name) for f in self.filters]
            coords = [parse_data_coord(file_data, "", column_index) for column_index in column_indices]
            parse_function = get_parse_row_value_function(file_data)

            # This is a special case where we can make it more efficient.
            # We always check the first filter. If that equates to False, we continue checking subsequent filters.
            for row_index in row_indices:
                for i, f in enumerate(self.filters):
                    if self._passes(parse_function(file_data, "", row_index, coords[i])):
                        row_indices_out.add(row_index)
                        break
        else:
            for f in self.filters:
                row_indices_f = f.get_matching_row_indices(file_data, row_indices, num_parallel)
                row_indices_out = row_indices_out | row_indices_f

        return row_indices_out

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

    if not isinstance(fltr, _BaseFilter):
        raise Exception("An object that inherits from __BaseFilter must be specified.")

    if out_file_type != "tsv":
        raise Exception("The only out_file_type currently supported is tsv.")

    if num_parallel > 1:
        global joblib
        joblib = __import__('joblib', globals(), locals())

    with initialize(data_file_path) as file_data:
        # Make sure the filters match the column types.
        fltr._check_types(file_data)

        # Filter rows based on the data
        keep_row_indices = fltr.get_matching_row_indices(file_data, None, num_parallel)

        if keep_row_indices is None:
            keep_row_indices = range(file_data.cache_dict["num_rows"])
        else:
            keep_row_indices = sorted(keep_row_indices)

        # Parse information about columns to be selected.
        if select_columns:
            if not isinstance(select_columns, list):
                raise Exception("You must specify select_column as a list.")

            # Save header line
            select_columns = [c.encode() for c in select_columns]
            with get_write_object(out_file_path) as write_obj:
                write_obj.write(b"\t".join(select_columns) + b"\n")

            # Get select column indices
            if len(select_columns) <= 100:
                select_column_index_chunks = [[get_column_index_from_name(file_data, c) for c in select_columns]]
                num_select_column_chunks = 1
            else:
                max_columns_per_chunk = 1000001

                select_column_name_chunks = split_list_into_chunks(select_columns, max_columns_per_chunk)
                select_column_index_chunks = list(get_column_index_chunks_from_names(file_data, select_column_name_chunks))

                num_select_column_chunks = ceil(len(select_columns) / max_columns_per_chunk)
        else:
            # Save header line
            chunk_size = 1000001
            cn_start = file_data.file_map_dict["cn"][0]
            cn_end = file_data.file_map_dict["cn"][1]

            with get_write_object(out_file_path) as write_obj:
                for chunk_start in range(cn_start, cn_end, chunk_size):
                    chunk_text = file_data.file_handle[chunk_start:min(chunk_start + chunk_size, cn_end)]
                    write_obj.write(chunk_text.replace(b"\n", b"\t"))

                write_obj.write(b"\n")

            # Get select column indices
            max_columns_per_chunk = 1000001
            num_cols = file_data.cache_dict["num_cols"]
            select_column_index_chunks = list(generate_range_chunks(num_cols, max_columns_per_chunk))
            num_select_column_chunks = ceil(num_cols / max_columns_per_chunk)

        if len(keep_row_indices) == 0:
            return

        num_keep_rows = len(keep_row_indices)
        max_rows_per_chunk = ceil(num_keep_rows / num_parallel)
        num_row_index_chunks = ceil(num_keep_rows / max_rows_per_chunk)

        if num_row_index_chunks == 1:
            keep_row_indices = [keep_row_indices]
        else:
            keep_row_indices = list(split_list_into_chunks(keep_row_indices, max_rows_per_chunk))

        if num_select_column_chunks == 1 and num_row_index_chunks == 1:
            save_output_rows(data_file_path, out_file_path, keep_row_indices[0], select_column_index_chunks[0])
        else:
            if tmp_dir_path:
                makedirs(tmp_dir_path, exist_ok=True)
            else:
                tmp_dir_path = mkdtemp()

            tmp_dir_path = fix_dir_path_ending(tmp_dir_path)

            if num_parallel == 1:
                for row_chunk_number, row_chunk_indices in enumerate(keep_row_indices):
                    for column_chunk_number, column_chunk_indices in enumerate(select_column_index_chunks):
                        save_output_rows(data_file_path, f"{tmp_dir_path}{row_chunk_number}_{column_chunk_number}", row_chunk_indices, column_chunk_indices)
            else:
                joblib.Parallel(n_jobs=num_parallel)(
                    joblib.delayed(save_output_rows)(data_file_path, f"{tmp_dir_path}{row_chunk_number}_{column_chunk_number}", row_chunk_indices, column_chunk_indices)
                        for row_chunk_number, row_chunk_indices in enumerate(keep_row_indices)
                            for column_chunk_number, column_chunk_indices in enumerate(select_column_index_chunks)
                )

            with get_write_object(out_file_path, "ab") as write_obj:
                for row_chunk_number, row_indices in enumerate(keep_row_indices):
                    chunk_file_dict = {}
                    for column_chunk_number in range(num_select_column_chunks):
                        chunk_file_dict[column_chunk_number] = open(f"{tmp_dir_path}{row_chunk_number}_{column_chunk_number}", "rb")

                    for row_index in range(len(row_indices)):
                        for column_chunk_number in range(num_select_column_chunks):
                            chunk_file = chunk_file_dict[column_chunk_number]

                            if column_chunk_number + 1 == num_select_column_chunks:
                                write_obj.write(chunk_file.readline())
                            else:
                                write_obj.write(chunk_file.readline().rstrip(b"\n") + b"\t")

                    for chunk_file in chunk_file_dict.values():
                        chunk_file.close()

            for row_chunk_number in range(num_row_index_chunks):
                for column_chunk_number in range(num_select_column_chunks):
                    remove(f"{tmp_dir_path}{row_chunk_number}_{column_chunk_number}")

    # TODO: Use the RangeSet or intervaltree packages to store discrete
    #         indices more compactly (and quickly)?

def head(data_file_path, n=10, select_columns=None, out_file_path=None, out_file_type="tsv"):
    if not select_columns:
        select_columns = []

    query(data_file_path, HeadFilter(n), select_columns, out_file_path=out_file_path, out_file_type=out_file_type)

def tail(data_file_path, n=10, select_columns=None, out_file_path=None, out_file_type="tsv"):
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
            cache_dict["num_cols"] = fast_int((file_map_dict["cc"][1] - file_map_dict["cc"][0]) / cache_dict["ccml"]) - 1

            decompression_type = None
            decompressor = None

            if "cmpr" in file_map_dict:
                decompression_text = mmap_handle[file_map_dict["cmpr"][0]:file_map_dict["cmpr"][1]]

                if decompression_text == b"z":
                    decompression_type = "zstd"
                    decompressor = ZstdDecompressor()

                    # TODO: For super tall files, this gets too large to fit in memory.
                    #       If we continue to support zstd compression, you may need to incorporate
                    #       the idea of row chunks when building the file and then retrieve
                    #       the row_starts just for those.
                    #       However, the custom compression approach would avoid this problem.
                    row_lengths = deserialize(mmap_handle[file_map_dict["rl"][0]:file_map_dict["rl"][1]])
                    cache_dict["row_starts"] = [file_map_dict[""][0]]
                    for i, row_length in enumerate(row_lengths):
                        cache_dict["row_starts"].append(cache_dict["row_starts"][-1] + row_length)

                    cache_dict["ll"] = fast_int(mmap_handle[file_map_dict["ll"][0]:file_map_dict["ll"][1]])
                    cache_dict["num_rows"] = fast_int(mmap_handle[file_map_dict["nrow"][0]:file_map_dict["nrow"][1]])
                # else:
                #     decompression_type = "dictionary"
                #     decompressor = deserialize(decompression_text)
            else:
                last_cc = file_map_dict["cc"][1]
                cache_dict["ll"] = fast_int(mmap_handle[(last_cc - cache_dict["ccml"]):last_cc])
                cache_dict["num_rows"] = fast_int((file_map_dict[""][1] - file_map_dict[""][0]) / cache_dict["ll"])

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

def get_column_index_chunks_from_names(file_data, column_names_chunks):
    name_index_coords = parse_data_coords(file_data, "cni", [0, 1])
    num_column_chunks = 0

    for column_names_chunk in column_names_chunks:
        num_column_chunks += 1
        yield _get_column_index_chunk_from_names(file_data, name_index_coords, set(column_names_chunk))

def _get_column_index_chunk_from_names(file_data, name_index_coords, column_names_set):
    column_indices = []

    for column_index in range(file_data.cache_dict["num_cols"]):
        column_name = parse_row_value(file_data, "cni", column_index, name_index_coords[0])

        if column_name in column_names_set:
            column_indices.append(column_index)

    return column_indices

# def get_column_name_from_index(file_data, column_index):
#     position = get_identifier_row_index(file_data, "cin", column_index, file_data.cache_dict["num_cols"])
#
#     if position < 0:
#         raise Exception(f"Could not retrieve name because column index {column_index.decode()} was not found.")
#
#     return position

def get_column_type_from_index(file_data, column_index):
    return next(parse_data_values_from_file(file_data, "ct", column_index, 1, [[0, 1]])).decode()

def get_value_from_single_column_file(file_handle, content_start_index, segment_length, row_index):
    start_search_position = content_start_index + row_index * segment_length
    return file_handle[start_search_position:(start_search_position + segment_length)].rstrip(b" ")

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

def parse_data_value_from_string(coords, string):
    return string[(coords[0]):(coords[1])].rstrip(b" ")

def parse_data_values_from_string(data_coords, string):
    for coords in data_coords:
        yield string[(coords[0]):(coords[1])].rstrip(b" ")

def parse_data_values_from_file(file_data, data_file_key, start_element, segment_length, data_coords):
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
    return parse_data_value_from_file(file_data, data_file_key, row_index, file_data.cache_dict[data_file_key + "ll"], column_coords).rstrip(b" ")

def parse_zstd_compressed_row_value(file_data, data_file_key, row_index, column_coords):
    line = get_zstd_compressed_row(file_data, row_index)

    return parse_data_value_from_string(column_coords, line)

    # TODO: This will not work well when files are super wide. If we continue
    #       to support zstd compression, you will need to compress column chunks.
    #       However, the custom compression scheme would avoid this problem.

    # data_start_search_position = file_data.file_map_dict[""][0]
    # line_start_search_position = data_start_search_position + fast_int(get_value_from_single_column_file(file_data.file_handle, file_data.file_map_dict["ll"][0], file_data.cache_dict["mlll"], row_index))
    # next_line_start_search_position = data_start_search_position + fast_int(get_value_from_single_column_file(file_data.file_handle, file_data.file_map_dict["ll"][0], file_data.cache_dict["mlll"], row_index + 1))
    #
    # chunk_size = 1000001
    # all_text = b""
    # for i in range(line_start_search_position, next_line_start_search_position, chunk_size):
    #     line_end_search_position = min(i + chunk_size, next_line_start_search_position)
    #     text = file_data.decompressor.decompress(file_data.file_handle[i:line_end_search_position])
    #     all_text += text
    #
    # return all_text[column_coords[0]:column_coords[1]].rstrip(b" ")

def get_zstd_compressed_row(file_data, row_index):
    row_start = file_data.cache_dict["row_starts"][row_index]
    row_end = file_data.cache_dict["row_starts"][row_index + 1]

    return file_data.decompressor.decompress(file_data.file_handle[row_start:row_end])

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
    line = get_zstd_compressed_row(file_data, row_index)

    return list(parse_data_values_from_string(column_coords, line))

    # data_start_search_position = file_data.file_map_dict[""][0]
    # line_start_search_position = data_start_search_position + fast_int(get_value_from_single_column_file(file_data.file_handle, file_data.file_map_dict["ll"][0], file_data.cache_dict["mlll"], row_index))
    # next_line_start_search_position = data_start_search_position + fast_int(get_value_from_single_column_file(file_data.file_handle, file_data.file_map_dict["ll"][0], file_data.cache_dict["mlll"], row_index + 1))
    #
    # chunk_size = 1000001
    # all_text_list = []
    #
    # for i in range(line_start_search_position, next_line_start_search_position, chunk_size):
    #     line_end_search_position = min(i + chunk_size, next_line_start_search_position)
    #     text = file_data.decompressor.decompress(file_data.file_handle[i:line_end_search_position])
    #     all_text_list.append(text)
    #
    # return list(parse_data_values_from_string(column_coords, b"".join(all_text_list)))

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

@contextmanager
def get_write_object(out_file_path, mode="wb"):
    if out_file_path:
        write_obj = open(out_file_path, mode)

        try:
            yield write_obj
        finally:
            write_obj.close()
    else:
        yield sys.stdout.buffer

def save_output_rows(in_file_path, out_file_path, row_indices, column_indices):
    with initialize(in_file_path) as file_data:
        with get_write_object(out_file_path, "ab") as write_obj:
            select_column_coords = parse_data_coords(file_data, "", column_indices)
            parse_row_values_function = get_parse_row_values_function(file_data)

            for row_index in row_indices:
                # print(row_index)
                write_obj.write(b"\t".join(parse_row_values_function(file_data, "", row_index, select_column_coords)) + b"\n")

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

def get_identifier_row_index(file_data, data_file_key, query_value, end_search_position):
    if end_search_position == 0:
        return -1

    value_coords = parse_data_coord(file_data, data_file_key, 0)
    position_coords = parse_data_coord(file_data, data_file_key, 1)

    matching_position = binary_identifier_search(file_data, data_file_key, value_coords, query_value, 0, end_search_position)

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

def filter_using_operator(file_data, data_file_key, fltr, cc_value_column_index, cc_position_column_index, start_search_position, end_search_position, retrieve_row_indices, num_parallel):
    if end_search_position == 0:
        return set()

    coords = parse_data_coords(file_data, data_file_key, [cc_value_column_index, cc_position_column_index])

    if fltr.oper == eq:
        return find_row_indices_for_range(file_data, data_file_key, coords[0], coords[1], fltr, fltr, start_search_position, end_search_position, retrieve_row_indices, num_parallel)
    else:
        if fltr.oper == ne:
            # FYI: retrieve_row_indices is not supported in this case.
            lower_position, upper_position = find_bounds_for_range(file_data, data_file_key, coords[0], fltr, fltr, 0, end_search_position)

            lower_positions = (0, lower_position)
            upper_positions = (upper_position, end_search_position)

            lower_row_indices = retrieve_matching_row_indices(file_data, data_file_key, coords[1], lower_positions, num_parallel)
            upper_row_indices = retrieve_matching_row_indices(file_data, data_file_key, coords[1], upper_positions, num_parallel)

            return lower_row_indices | upper_row_indices
        else:
            if fltr.oper == gt:
                positions = find_positions_g(file_data, data_file_key, coords[0], fltr, start_search_position, end_search_position, le)
            elif fltr.oper == ge:
                positions = find_positions_g(file_data, data_file_key, coords[0], fltr, start_search_position, end_search_position, lt)
            elif fltr.oper == lt:
                positions = find_positions_l(file_data, data_file_key, coords[0], fltr, start_search_position, end_search_position, fltr.oper)
            elif fltr.oper == le:
                positions = find_positions_l(file_data, data_file_key, coords[0], fltr, start_search_position, end_search_position, fltr.oper)

            if retrieve_row_indices:
                return retrieve_matching_row_indices(file_data, data_file_key, coords[1], positions, num_parallel)

            return positions

def find_positions_g(file_data, data_file_key, value_coords, fltr, start_search_position, end_search_position, all_false_operator):
    smallest_value = parse_row_value(file_data, data_file_key, start_search_position, value_coords)
    if smallest_value == b"":
        return start_search_position, end_search_position

    if not all_false_operator(fltr._get_conversion_function()(smallest_value), fltr.value):
        return start_search_position, end_search_position

    largest_value = parse_row_value(file_data, data_file_key, end_search_position - 1, value_coords)
    if largest_value == b"":
        return start_search_position, start_search_position

    matching_position = search(file_data, data_file_key, value_coords, fltr, 0, end_search_position, end_search_position, all_false_operator)

    return matching_position + 1, end_search_position

def find_positions_l(file_data, data_file_key, value_coords, fltr, start_search_position, end_search_position, all_true_operator):
    smallest_value = parse_row_value(file_data, data_file_key, start_search_position, value_coords)
    if smallest_value == b"":
        return start_search_position, start_search_position

    if not all_true_operator(fltr._get_conversion_function()(smallest_value), fltr.value):
        return start_search_position, start_search_position

    largest_value = parse_row_value(file_data, data_file_key, end_search_position - 1, value_coords)
    if largest_value == b"":
        return start_search_position, end_search_position

    if all_true_operator(fltr._get_conversion_function()(largest_value), fltr.value):
        return start_search_position, end_search_position

    matching_position = search(file_data, data_file_key, value_coords, fltr, 0, end_search_position, end_search_position, all_true_operator)

    return start_search_position, matching_position + 1

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

def find_bounds_for_range(file_data, data_file_key, value_coords, filter1, filter2, start_search_position, end_search_position):
    lower_positions = find_positions_g(file_data, data_file_key, value_coords, filter1, start_search_position, end_search_position, lt)
    upper_positions = find_positions_l(file_data, data_file_key, value_coords, filter2, lower_positions[0], lower_positions[1], le)

    lower_position = max(lower_positions[0], upper_positions[0])
    upper_position = min(lower_positions[1], upper_positions[1])

    return lower_position, upper_position

def find_row_indices_for_range(file_data, data_file_key, value_coords, position_coords, filter1, filter2, start_search_position, end_search_position, retrieve_row_indices, num_parallel):
    lower_position, upper_position = find_bounds_for_range(file_data, data_file_key, value_coords, filter1, filter2, start_search_position, end_search_position)

    if retrieve_row_indices:
        return retrieve_matching_row_indices(file_data, data_file_key, position_coords, (lower_position, upper_position), num_parallel)
    else:
        return (lower_position, upper_position)

def get_passing_row_indices_with_filter(file_data, data_file_key, fltr, cc_value_column_index, cc_position_column_index, start_search_position, end_search_position, retrieve_row_indices, num_parallel):
    coords = parse_data_coords(file_data, data_file_key, [cc_value_column_index, cc_position_column_index])

    lower_range = find_positions_g(file_data, data_file_key, coords[0], fltr, start_search_position, end_search_position, lt)

    if lower_range[0] == end_search_position:
        return set()

    if lower_range[1] == end_search_position:
        upper_position = end_search_position
    else:
        upper_position = search_with_filter(file_data, data_file_key, coords[0], lower_range[0], lower_range[1], end_search_position, fltr)

    if retrieve_row_indices:
        return retrieve_matching_row_indices(file_data, data_file_key, coords[1], (lower_range[0], upper_position), num_parallel)

    return (lower_range[0], upper_position)