import sys
sys.path.append('/')

import f4
import glob
import gzip
from io import TextIOWrapper, BytesIO
import operator
import os
import random
import shutil
import time

def get_delimited_file_handle(file_path):
    if file_path.endswith(".gz"):
        return gzip.open(file_path)
    else:
        return open(file_path, 'rb')

def read_file_into_lists(file_path, delimiter=b"\t"):
    out_items = []

    the_file = get_delimited_file_handle(file_path)

    for line in the_file:
        if not line.startswith(b"#"):
            out_items.append(line.rstrip(b"\n").split(delimiter))

    the_file.close()

    return out_items

def read_string_into_lists(s, delimiter="\t"):
    out_items = []

    for line in s.split("\n"):
        items = [x.encode() for x in line.split(delimiter) if len(x) > 0]

        if len(items) > 0:
            out_items.append(items)

    return out_items

def check_results(description, actual_lists, expected_lists):
    #print(actual_lists)
    #print(expected_lists)
    check_result(description, "Number of rows", len(actual_lists), len(expected_lists), False)

    for i in range(len(actual_lists)):
        actual_row = actual_lists[i]
        expected_row = expected_lists[i]

        check_result(description, f"Number of columns in row {i}", len(actual_row), len(expected_row), False)

        for j in range(len(actual_row)):
            check_result(description, f"row {i}, column {j}", actual_row[j], expected_row[j], False)

    pass_test(description)

def check_result(description, test, actual, expected, show_confirmation=True):
    if actual != expected:
        fail_test(f"A test failed for '{description}' and '{test}'.\n  Actual: {actual}\n  Expected: {expected}")
        sys.exit(1)
    elif show_confirmation:
        pass_test(f"'{description}' and '{test}'")

def pass_test(message):
    print(f"PASS: {message}")

def fail_test(message):
    print(f"FAIL: {message}")
    sys.exit(1)

def run_small_tests(in_file_path, f4_file_path, out_file_path, num_parallel=1, compression_type=None, index_columns=[], use_memory_mapping=True):
    print("-------------------------------------------------------")
    print(f"Input file path: {in_file_path}")
    print(f"Output file path: {f4_file_path}")
    print(f"num_parallel: {num_parallel}")
    print(f"compression_type: {compression_type}")
    print(f"index_columns: {index_columns}")
    print(f"use_memory_mapping: {use_memory_mapping}")
    print("-------------------------------------------------------")

    # Clean up data files if they already exist
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

    f4.convert_delimited_file(in_file_path, f4_file_path, compression_type=compression_type, num_parallel=num_parallel, index_columns=index_columns, tmp_dir_path="/tmp/f4_small_tests")

    try:
        f4.query("bogus_file_path")
        fail_test("Invalid file path.")
    except:
        pass_test("Invalid file path.")

    print("version:")
    print(f4.get_version(f4_file_path))
    check_result("Version", "Version number - major", f4.get_version(f4_file_path), "1")
    check_result("Dimensions", "Number of rows", f4.get_num_rows(f4_file_path, use_memory_mapping), 5)
    check_result("Dimensions", "Number of columns", f4.get_num_cols(f4_file_path, use_memory_mapping), 9)
    check_result("Indexes", "Index details", f4.get_indexes(f4_file_path, use_memory_mapping), index_columns)

    check_result("Column types", "ID column", f4.get_column_type_from_name(f4_file_path, "ID", use_memory_mapping), "s")
    check_result("Column types", "FloatA column", f4.get_column_type_from_name(f4_file_path, "FloatA", use_memory_mapping), "f")
    check_result("Column types", "FloatB column", f4.get_column_type_from_name(f4_file_path, "FloatB", use_memory_mapping), "f")
    check_result("Column types", "OrdinalA column", f4.get_column_type_from_name(f4_file_path, "OrdinalA", use_memory_mapping), "s")
    check_result("Column types", "OrdinalB column", f4.get_column_type_from_name(f4_file_path, "OrdinalB", use_memory_mapping), "s")
    check_result("Column types", "IntA column", f4.get_column_type_from_name(f4_file_path, "IntA", use_memory_mapping), "i")
    check_result("Column types", "IntB column", f4.get_column_type_from_name(f4_file_path, "IntB", use_memory_mapping), "i")
    check_result("Column types", "CategoricalA column", f4.get_column_type_from_name(f4_file_path, "CategoricalA", use_memory_mapping), "s")
    check_result("Column types", "CategoricalB column", f4.get_column_type_from_name(f4_file_path, "CategoricalB", use_memory_mapping), "s")

    f4.query(f4_file_path, f4.NoFilter(), [], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("No filters, select all columns", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.NoFilter(), ["ID","FloatA","FloatB","OrdinalA","OrdinalB","IntA","IntB","CategoricalA","CategoricalB"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("No filters, select all columns explicitly", read_file_into_lists(out_file_path), read_file_into_lists(in_file_path))
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.NoFilter(), ["ID"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("No filters, select first column", read_file_into_lists(out_file_path), [[b"ID"],[b"E"],[b"A"],[b"B"],[b"C"],[b"D"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.NoFilter(), ["CategoricalB"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("No filters, select last column", read_file_into_lists(out_file_path), [[b"CategoricalB"],[b"Brown"],[b"Yellow"],[b"Yellow"],[b"Brown"],[b"Orange"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.NoFilter(), ["FloatA", "CategoricalB"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("No filters, select two columns", read_file_into_lists(out_file_path), [[b"FloatA", b"CategoricalB"],[b"9.9", b"Brown"],[b"1.1", b"Yellow"],[b"2.2", b"Yellow"],[b"2.2", b"Brown"],[b"4.4", b"Orange"]])
    os.unlink(out_file_path)

    try:
        f4.query(f4_file_path, f4.NoFilter(), ["ID", "InvalidColumn"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
        fail_test("Invalid column name in select.")
    except:
        pass_test("Invalid column name in select.")

    f4.query(f4_file_path, f4.StringFilter("ID", operator.eq, "A"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter by ID using equals filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("ID", operator.ne, "A"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter by ID using not equals filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"2.2"],[b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("ID", operator.ge, "A"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter by ID using string >= filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("ID", operator.gt, "A"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter by ID using string > filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("ID", operator.le, "A"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter by ID using string <= filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("ID", operator.lt, "A"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter by ID using string < filter", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.IntRangeFilter("IntA", -100, 100), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("IntA within -100 and 100", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.IntFilter("IntA", operator.eq, 7), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Int equals filter", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.IntFilter("IntA", operator.eq, 5), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Int equals filter - one match", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.IntFilter("IntA", operator.ne, 5), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Int not equals filter - two matches", read_file_into_lists(out_file_path), [[b"FloatA"],[b"9.9"],[b"2.2"],[b"2.2"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.eq, 1.1), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Float equals filter - one match", read_file_into_lists(out_file_path), [[b"FloatA"],[b"1.1"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.eq, 2.2), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Float equals filter - two matches", read_file_into_lists(out_file_path), [[b"FloatA"],[b"2.2"],[b"2.2"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.ne, 1.1), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Float not equals filter", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.AndFilter(f4.IntFilter("IntA", operator.eq, 7), f4.FloatFilter("FloatA", operator.ne, 1.1)), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Two filters (And)", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.AndFilter(f4.StringFilter("OrdinalA", operator.eq, "Med"), f4.IntFilter("IntA", operator.gt, 0), f4.FloatFilter("FloatA", operator.lt, 3.0)), ["ID"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Three filters (And)", read_file_into_lists(out_file_path), [[b"ID"], [b"C"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.OrFilter(f4.IntFilter("IntA", operator.eq, 7), f4.FloatFilter("FloatA", operator.eq, 1.1)), ["ID"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Two filters (Or)", read_file_into_lists(out_file_path), [[b"ID"], [b"A"], [b"C"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.OrFilter(f4.StringFilter("OrdinalA", operator.eq, "Med"), f4.IntFilter("IntA", operator.gt, 5), f4.FloatFilter("FloatA", operator.gt, 1.1)), ["ID"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Three filters (Or)", read_file_into_lists(out_file_path), [[b"ID"], [b"E"], [b"B"], [b"C"], [b"D"]])
    os.unlink(out_file_path)

    or_1 = f4.OrFilter(
       f4.StringFilter("OrdinalA", operator.eq, "Med"),
       f4.StringFilter("OrdinalA", operator.eq, "High")
     )
    or_2 = f4.OrFilter(
               f4.OrFilter(
                 f4.IntFilter("IntB", operator.eq, 44),
                 f4.IntFilter("IntB", operator.eq, 99)
               ),
               f4.IntFilter("IntB", operator.eq, 77)
             )
    fltr = f4.AndFilter(or_1, or_2)
    f4.query(f4_file_path, fltr, ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Nested Or filters", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    or_3 = f4.OrFilter(
             f4.IntFilter("IntB", operator.eq, 44),
             f4.IntFilter("IntB", operator.eq, 99),
             f4.IntFilter("IntB", operator.eq, 77)
           )
    fltr = f4.AndFilter(or_1, or_3)
    f4.query(f4_file_path, fltr, ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Nested Or filters (simplified)", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    fltr = f4.AndFilter(
             f4.OrFilter(
               f4.StringFilter("OrdinalA", operator.eq, "Low"),
               f4.StringFilter("OrdinalA", operator.eq, "Med"),
               f4.StringFilter("OrdinalA", operator.eq, "High")
             ),
             f4.FloatFilter("FloatB", operator.le, 44.4)
           )
    f4.query(f4_file_path, fltr, ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Numeric filters and string filters", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StartsWithFilter("CategoricalB", "Ye"), ["ID"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("StartsWithFilter on categorical column B", read_file_into_lists(out_file_path), [[b"ID"], [b"A"], [b"B"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.EndsWithFilter("CategoricalB", "ow"), ["ID"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("EndsWithFilter on categorical column B", read_file_into_lists(out_file_path), [[b"ID"], [b"A"], [b"B"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatRangeFilter("FloatA", -9.9, 4.4), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("FloatA within -9.9 and 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatRangeFilter("FloatA", 2.2, 4.4), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("FloatA within 2.2 and 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatRangeFilter("FloatA", 4.4, 9.9), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("FloatA within 4.4 and 9.9", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatRangeFilter("FloatA", 1.1, 1.1), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("FloatA within 1.1 and 1.1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatRangeFilter("FloatA", 2.2, 2.2), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("FloatA within 2.2 and 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"], [b"2.2"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatRangeFilter("FloatA", 100.0, 1000.0), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("FloatA within 100 and 1000", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.IntRangeFilter("IntA", -100, 100), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("IntA within -100 and 100", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.IntRangeFilter("IntA", 5, 8), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("IntA within 5 and 8", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.IntRangeFilter("IntA", -8, -5), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("IntA within -8 and -5", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.IntRangeFilter("IntA", 5, 7), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("IntA within 5 and 7", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"4.4"]])

    f4.query(f4_file_path, f4.IntRangeFilter("IntA", 6, 8), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("IntA within 6 and 8", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"], [b"2.2"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.IntRangeFilter("IntA", 5, 5), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("IntA within 5 and 5", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.IntRangeFilter("IntA", 6, 6), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("IntA within 6 and 6", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringRangeFilter("OrdinalA", "High", "Medium"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("OrdinalA within High and Medium", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringRangeFilter("OrdinalA", "High", "Low"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("OrdinalA within High and Low", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringRangeFilter("OrdinalA", "Low", "Medium"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("OrdinalA within Low and Medium", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringRangeFilter("OrdinalA", "A", "Z"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("OrdinalA within High and Medium", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringRangeFilter("OrdinalA", "A", "B"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("OrdinalA within High and Medium", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    fltr = f4.AndFilter(f4.StringRangeFilter("OrdinalA", "High", "Low"), f4.IntRangeFilter("IntA", 5, 6))
    f4.query(f4_file_path, fltr, ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("StringRangeFilter and IntRangeFilter", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"]])
    os.unlink(out_file_path)

    fltr = f4.AndFilter(f4.StringRangeFilter("OrdinalA", "High", "Low"), f4.FloatRangeFilter("FloatA", 0.0, 5.0))
    f4.query(f4_file_path, fltr, ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("StringRangeFilter and IntRangeFilter", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"]])
    os.unlink(out_file_path)

    f4.head(f4_file_path, 3, ["FloatA"], out_file_path)
    check_results("Head filter", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"]])
    os.unlink(out_file_path)

    f4.tail(f4_file_path, 3, ["FloatA"], out_file_path)
    check_results("Tail filter", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    try:
        f4.query(f4_file_path, FloatFilter("InvalidColumn", operator.eq, 1), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
        fail_test("Invalid column name in float filter.")
    except:
        pass_test("Invalid column name in float filter.")

    try:
        f4.query(f4_file_path, FloatFilter(2, operator.eq, 1), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
        fail_test("Non-string column name in float filter.")
    except:
        pass_test("Non-string column name in float filter.")

    try:
        f4.query(f4_file_path, StringFilter("CategoricalA", operator.eq, None), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
        fail_test("None value to equals filter.")
    except:
        pass_test("None value to equals filter.")

    try:
        f4.query(f4_file_path, StringFilter("CategoricalA", operator.eq, 1), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
        fail_test("Non-string value to equals filter.")
    except:
        pass_test("Non-string value to equals filter.")

    try:
        f4.query(f4_file_path, FloatFilter("FloatA", operator.eq, "2"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
        fail_test("Non-number specified in float filter.")
    except:
        pass_test("Non-number specified in float filter.")

    try:
        f4.query(f4_file_path, FloatFilter("OrdinalA", operator.eq, 2), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
        fail_test("Non-float column specified for float filter.")
    except:
        pass_test("Non-float column specified for float filter.")

    try:
        f4.query(f4_file_path, "abc", ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
        fail_test("Non-filter is passed as a filter.")
    except:
        pass_test("Non-filter is passed as a filter.")

    f4.query(f4_file_path, f4.StringFilter("ID", operator.eq, "A"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter ID = A", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("ID", operator.eq, "B"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter ID = B", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("ID", operator.eq, "D"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter ID = D", read_file_into_lists(out_file_path), [[b"FloatA"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("ID", operator.eq, "E"), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter ID = E", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.gt, 0.0), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA > 0", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.gt, 1.1), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA > 1.1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.gt, 2.2), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA > 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.gt, 4.4), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA > 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.gt, 9.9), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA > 9.9", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.gt, 100.0), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA > 100", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.ge, 0.0), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA >= 0", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.ge, 1.1), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA >= 1.1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.ge, 2.2), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA >= 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.ge, 4.4), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA >= 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.ge, 9.9), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA >= 9.9", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.ge, 100.0), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA >= 100", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.lt, 0.0), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA < 0", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.lt, 1.1), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA < 1.1", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.lt, 2.2), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA < 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.lt, 4.4), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA < 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.lt, 9.9), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA < 9.9", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.lt, 100.0), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA < 100", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.le, 0.0), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA <= 0", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.le, 1.1), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA <= 1.1", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.le, 2.2), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA <= 2.2", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.le, 4.4), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA <= 4.4", read_file_into_lists(out_file_path), [[b"FloatA"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.le, 9.9), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA <= 9.9", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.FloatFilter("FloatA", operator.le, 100.0), ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter FloatA <= 100", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"1.1"], [b"2.2"], [b"2.2"], [b"4.4"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("OrdinalA", operator.eq, "Low"), ["ID"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Categorical filter OrdinalA = Low", read_file_into_lists(out_file_path), [[b"ID"], [b"E"], [b"A"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("OrdinalA", operator.eq, "Med"), ["ID"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Categorical filter OrdinalA = Med", read_file_into_lists(out_file_path), [[b"ID"], [b"C"], [b"D"]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("OrdinalA", operator.eq, "High"), ["ID"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Categorical filter OrdinalA = High", read_file_into_lists(out_file_path), [[b"ID"], [b"B"]])
    os.unlink(out_file_path)

    fltr = f4.AndFilter(
             f4.OrFilter(
               f4.StringFilter("ID", operator.eq, "A"),
               f4.StringFilter("ID", operator.eq, "B"),
               f4.StringFilter("ID", operator.eq, "C")
             ),
             f4.FloatFilter("FloatA", operator.ge, 2.0)
           )
    f4.query(f4_file_path, fltr, ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter using two, single-index columns", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"], [b"2.2"]])
    os.unlink(out_file_path)

    fltr = f4.AndFilter(f4.StringFilter("CategoricalB", operator.eq, "Yellow"), f4.IntRangeFilter("IntB", 0, 50))
    f4.query(f4_file_path, fltr, ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter using string/int-range two-column index - A", read_file_into_lists(out_file_path), [[b"FloatA"], [b"2.2"]])
    os.unlink(out_file_path)

    fltr = f4.AndFilter(f4.StringFilter("CategoricalB", operator.eq, "Yellow"), f4.IntRangeFilter("IntB", 0, 25))
    f4.query(f4_file_path, fltr, ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter using string/int-range two-column index - B", read_file_into_lists(out_file_path), [[b"FloatA"]])
    os.unlink(out_file_path)

    fltr = f4.AndFilter(f4.StringFilter("CategoricalB", operator.eq, "Brown"), f4.IntRangeFilter("IntB", 50, 100))
    f4.query(f4_file_path, fltr, ["FloatA"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter using string/int-range two-column index - C", read_file_into_lists(out_file_path), [[b"FloatA"], [b"9.9"], [b"2.2"]])
    os.unlink(out_file_path)

    fltr = f4.AndFilter(f4.FloatFilter("FloatA", operator.eq, 2.2), f4.StringFilter("OrdinalA", operator.eq, "Med"), f4.IntFilter("IntA", operator.gt, 6))
    f4.query(f4_file_path, fltr, ["ID"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter using 3-column index - All 3", read_file_into_lists(out_file_path), [[b"ID"], [b"C"]])
    os.unlink(out_file_path)

    fltr = f4.AndFilter(f4.FloatFilter("FloatA", operator.eq, 2.2), f4.StringFilter("OrdinalA", operator.eq, "Med"))
    f4.query(f4_file_path, fltr, ["ID"], out_file_path, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("3-column index - Only partial match", read_file_into_lists(out_file_path), [[b"ID"], [b"C"]])
    os.unlink(out_file_path)

    # Clean up data files
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

def test_transpose(tsv_file_path, f4_file_path, out_file_path, num_parallel, compression_type, use_memory_mapping):
    f4.convert_delimited_file(tsv_file_path, f4_file_path, num_parallel=num_parallel, compression_type=compression_type)
    f4.transpose(f4_file_path, out_file_path, src_column_for_names="ID", num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)

    check_result("Transpose - Dimensions", "Number of rows", f4.get_num_rows(out_file_path), 8)
    check_result("Transpose - Dimensions", "Number of columns", f4.get_num_cols(out_file_path), 6)

    check_result("Transpose - Column types", "ID column", f4.get_column_type_from_name(out_file_path, "ID"), "s")
    check_result("Transpose - Column types", "E column", f4.get_column_type_from_name(out_file_path, "E"), "s")
    check_result("Transpose - Column types", "A column", f4.get_column_type_from_name(out_file_path, "A"), "s")
    check_result("Transpose - Column types", "B column", f4.get_column_type_from_name(out_file_path, "B"), "s")
    check_result("Transpose - Column types", "C column", f4.get_column_type_from_name(out_file_path, "C"), "s")
    check_result("Transpose - Column types", "D column", f4.get_column_type_from_name(out_file_path, "D"), "s")

    out_file_path_2 = out_file_path + "2"
    f4.query(out_file_path, f4.StringFilter("ID", operator.eq, "OrdinalA"), ["A"], out_file_path_2, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Transpose - Filter by ID", read_file_into_lists(out_file_path_2), [[b"A"], [b"Low"]])
    os.unlink(out_file_path_2)

    f4.query(out_file_path, f4.StringFilter("B", operator.eq, "High"), ["D"], out_file_path_2, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Transpose - Filter by B", read_file_into_lists(out_file_path_2), [[b"D"], [b"Med"]])
    os.unlink(out_file_path_2)

def test_inner_join(num_parallel, compression_type, use_memory_mapping):
    data1 = read_file_into_lists("data/small.tsv")
    data2 = read_file_into_lists("data/small.tsv")

    # Manage header rows.
    header_row = data1.pop(0)
    data2.pop(0)

    # Create a modified version of data1 that has a duplicate ID.
    new_row = list(data1[0])
    for i in range(1, len(new_row)):
        new_row[i] += b"_left_Z"
    data1.append(new_row)

    # Modify data2 to have different values (except ID column) as data1.
    for row_i in range(len(data2)):
        for col_i in range(1, len(data2[row_i])):
            mod_value = data2[row_i][col_i] + b"_right"
            data2[row_i][col_i] = mod_value

    # Shuffle the row order for row2 and add a row with a duplicate ID (but different values).
    random.seed(0)
    random.shuffle(data2)
    data2.pop(0)
    new_row = list(data2[0])
    for i in range(1, len(new_row)):
        new_row[i] += b"_Z"
    data2.append(new_row)

    with open("/tmp/small1.tsv", "wb") as file2:
        file2.write(b"\t".join(header_row[0:1] + [x + b"_left" for x in header_row[1:]]) + b"\n")

        for row in data1:
            file2.write(b"\t".join(row) + b"\n")

    with open("/tmp/small2.tsv", "wb") as file2:
        file2.write(b"\t".join(header_row[0:1] + [x + b"_right" for x in header_row[1:]]) + b"\n")

        for row in data2:
            file2.write(b"\t".join(row) + b"\n")

    f4.convert_delimited_file("/tmp/small1.tsv", "/tmp/small1.f4", num_parallel=num_parallel, compression_type=compression_type)
    f4.convert_delimited_file("/tmp/small2.tsv", "/tmp/small2.f4", num_parallel=num_parallel, compression_type=compression_type)

    out_file_path = "/tmp/small_joined.f4"
    f4.inner_join("/tmp/small1.f4", "/tmp/small2.f4", "ID", "/tmp/small_joined.f4", num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)

    check_result("Dimensions", "Number of rows", f4.get_num_rows(out_file_path), 6)
    check_result("Dimensions", "Number of columns", f4.get_num_cols(out_file_path), 17)

    check_result("Column types", "ID column", f4.get_column_type_from_name(out_file_path, "ID"), "s")
    check_result("Column types", "FloatA_left column", f4.get_column_type_from_name(out_file_path, "FloatA_left"), "s")
    check_result("Column types", "FloatB_right", f4.get_column_type_from_name(out_file_path, "FloatB_right"), "s")

    out_file_path_2 = out_file_path + "2"
    f4.query(out_file_path, f4.StringFilter("ID", operator.eq, "E"), ["IntA_left"], out_file_path_2, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter by ID - left", read_file_into_lists(out_file_path_2), [[b"IntA_left"], [b"6"], [b"6_left_Z"]])
    os.unlink(out_file_path_2)

    f4.query(out_file_path, f4.StringFilter("ID", operator.eq, "A"), ["IntA_right"], out_file_path_2, num_parallel=num_parallel, use_memory_mapping=use_memory_mapping)
    check_results("Filter by ID - right", read_file_into_lists(out_file_path_2), [[b"IntA_right"], [b"5_right"], [b"5_right_Z"]])
    os.unlink(out_file_path_2)

def run_larger_tests(num_parallel, size, extension, discrete1_index, numeric1_index, build_outputs, compression_type, check_outputs, verbose, tmp_dir_path, use_memory_mapping, do_test_with_indexing):
    in_file_path = f"data/{size}.tsv{extension}"
    f4_file_path = f"data/{size}.f4"
    out_file_path = "/tmp/f4_out.tsv"

    larger_ID = []
    larger_Categorical1 = []
    larger_Discrete1 = []
    larger_Numeric1 = []

    if check_outputs:
        print("------------------------------------------------------------------")
        print(f"Parsing {in_file_path} - cmpr: {compression_type} - for master outputs")
        print("------------------------------------------------------------------")

        with get_delimited_file_handle(in_file_path) as larger_file:
            for line in larger_file:
                line_items = line.rstrip(b"\n").split(b"\t")
                larger_ID.append([line_items[0]])
                larger_Categorical1.append([line_items[1]])
                larger_Discrete1.append([line_items[discrete1_index]])

                number = line_items[numeric1_index]
                if number != b"Numeric1":
                    number = float(number)
                larger_Numeric1.append([number])

    print("-------------------------------------------------------------------")
    print(f"Running tests for {in_file_path} - no indexing (cmpr: {compression_type})")
    print("-------------------------------------------------------------------")

    if build_outputs or not os.path.exists(f4_file_path):
        print("-------------------------------------------------------------------")
        print(f"Converting {in_file_path} to {f4_file_path} (cmpr: {compression_type})")
        print("-------------------------------------------------------------------")

        # Clean up data files if they already exist
        for file_path in glob.glob(f"{f4_file_path}*"):
            os.unlink(file_path)

        f4.convert_delimited_file(in_file_path, f4_file_path, compression_type=compression_type, num_parallel=num_parallel, verbose=verbose, tmp_dir_path=tmp_dir_path)

    run_larger_tests2(f4_file_path, out_file_path, larger_ID, larger_Categorical1, larger_Discrete1, larger_Numeric1, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)

    if do_test_with_indexing:
        print("---------------------------------------------------------------------")
        print(f"Running tests for {in_file_path} - with indexing (cmpr: {compression_type})")
        print("---------------------------------------------------------------------")

        if build_outputs:
            # Clean up data files
            for file_path in glob.glob(f"{f4_file_path}*"):
                os.unlink(file_path)

            f4.convert_delimited_file(in_file_path, f4_file_path, index_columns=["ID", "Categorical1", "Discrete1", "Numeric1"], compression_type=compression_type, num_parallel=num_parallel, verbose=verbose, tmp_dir_path=tmp_dir_path)

        run_larger_tests2(f4_file_path, out_file_path, larger_ID, larger_Categorical1, larger_Discrete1, larger_Numeric1, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)

def run_larger_tests2(f4_file_path, out_file_path, larger_ID, larger_Categorical1, larger_Discrete1, larger_Numeric1, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping):
    f4.query(f4_file_path, f4.StringFilter("ID", operator.eq, "Row1"), ["Discrete1"], out_file_path, num_parallel=num_parallel, tmp_dir_path=tmp_dir_path, use_memory_mapping=use_memory_mapping)
    if check_outputs:
        check_results("Filter ID = Row1", read_file_into_lists(out_file_path), [[b"Discrete1"], larger_Discrete1[1]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("ID", operator.eq, "Row33"), ["Discrete1"], out_file_path, num_parallel=num_parallel, tmp_dir_path=tmp_dir_path, use_memory_mapping=use_memory_mapping)
    if check_outputs:
        check_results("Filter ID = Row33", read_file_into_lists(out_file_path), [[b"Discrete1"], larger_Discrete1[33]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("ID", operator.eq, "Row91"), ["Discrete1"], out_file_path, num_parallel=num_parallel, tmp_dir_path=tmp_dir_path, use_memory_mapping=use_memory_mapping)
    if check_outputs:
        check_results("Filter ID = Row91", read_file_into_lists(out_file_path), [[b"Discrete1"], larger_Discrete1[91]])
    os.unlink(out_file_path)

    f4.query(f4_file_path, f4.StringFilter("ID", operator.eq, "Row100"), ["Discrete1"], out_file_path, num_parallel=num_parallel, tmp_dir_path=tmp_dir_path, use_memory_mapping=use_memory_mapping)
    if check_outputs:
        check_results("Filter ID = Row100", read_file_into_lists(out_file_path), [[b"Discrete1"], larger_Discrete1[100]])
    os.unlink(out_file_path)

    run_string_test("Categorical1", "A", "A", f4_file_path, larger_ID, larger_Categorical1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_string_test("Categorical1", "D", "D", f4_file_path, larger_ID, larger_Categorical1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_string_test("Categorical1", "A", "D", f4_file_path, larger_ID, larger_Categorical1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_string_test("Categorical1", "B", "B", f4_file_path, larger_ID, larger_Categorical1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_string_test("Categorical1", "B", "C", f4_file_path, larger_ID, larger_Categorical1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_string_test("Categorical1", "A", "C", f4_file_path, larger_ID, larger_Categorical1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_string_test("Categorical1", "B", "D", f4_file_path, larger_ID, larger_Categorical1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_string_test("Categorical1", "B", "Z", f4_file_path, larger_ID, larger_Categorical1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)

    run_string_test("Discrete1", "AA", "AA", f4_file_path, larger_ID, larger_Discrete1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_string_test("Discrete1", "PM", "PM", f4_file_path, larger_ID, larger_Discrete1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_string_test("Discrete1", "AA", "ZZ", f4_file_path, larger_ID, larger_Discrete1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_string_test("Discrete1", "FA", "SZ", f4_file_path, larger_ID, larger_Discrete1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)

    run_float_test(0.0, 1.0, f4_file_path, larger_ID, larger_Numeric1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_float_test(0.85, 0.9, f4_file_path, larger_ID, larger_Numeric1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_float_test(-0.9, -0.85, f4_file_path, larger_ID, larger_Numeric1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_float_test(-0.5, 0.0, f4_file_path, larger_ID, larger_Numeric1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_float_test(-0.5, 0.5, f4_file_path, larger_ID, larger_Numeric1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_float_test(-1000.0, 1000.0, f4_file_path, larger_ID, larger_Numeric1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)
    run_float_test(0.5, 0.5, f4_file_path, larger_ID, larger_Numeric1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping)

def run_string_test(column_name, lower_bound, upper_bound, f4_file_path, larger_ID, filter_values, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping):
    f4.query(f4_file_path, f4.StringRangeFilter(column_name, lower_bound, upper_bound), ["ID"], out_file_path, num_parallel=num_parallel, tmp_dir_path=tmp_dir_path, use_memory_mapping=use_memory_mapping)

    if check_outputs:
        indices = [i for i in range(len(filter_values)) if filter_values[i][0] == column_name.encode() or (filter_values[i][0] >= lower_bound.encode() and filter_values[i][0] <= upper_bound.encode())]
        matches = [larger_ID[i] for i in indices]
        actual = read_file_into_lists(out_file_path)
        check_results(f"Filter {column_name} = {lower_bound} <> {upper_bound} = {len(matches) - 1} matches", read_file_into_lists(out_file_path), matches)

    os.unlink(out_file_path)

def run_float_test(lower_bound, upper_bound, f4_file_path, larger_ID, larger_Numeric1, out_file_path, num_parallel, check_outputs, tmp_dir_path, use_memory_mapping):
    column_name = "Numeric1"
    f4.query(f4_file_path, f4.FloatRangeFilter(column_name, lower_bound, upper_bound), ["ID"], out_file_path, num_parallel=num_parallel, tmp_dir_path=tmp_dir_path, use_memory_mapping=use_memory_mapping)

    if check_outputs:
        indices = [i for i in range(len(larger_Numeric1)) if isinstance(larger_Numeric1[i][0], bytes) or (larger_Numeric1[i][0] >= lower_bound and larger_Numeric1[i][0] <= upper_bound)]
        matches = [larger_ID[i] for i in indices]
        check_results(f"Filter FloatWithin = {lower_bound} <> {upper_bound} = {len(matches) - 1} matches", read_file_into_lists(out_file_path), matches)

    os.unlink(out_file_path)

def run_all_small_tests():
    # Basic small tests
    f4_file_path = "data/small.f4"
    out_file_path = "/tmp/small_out.tsv"
    run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_parallel = 1)
    run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_parallel = 2)

    # Basic small tests (with gzipped files)
    run_small_tests("data/small.tsv.gz", f4_file_path, out_file_path, num_parallel = 1)
    run_small_tests("data/small.tsv.gz", f4_file_path, out_file_path, num_parallel = 2)

    # Make sure we print to standard out properly (this code does not work inside a function).
    f4.convert_delimited_file("data/small.tsv", f4_file_path)
    old_stdout = sys.stdout
    sys.stdout = TextIOWrapper(BytesIO(), sys.stdout.encoding)
    f4.query(f4_file_path, f4.NoFilter(), [], out_file_path=None, num_parallel=1)
    sys.stdout.seek(0)
    out = sys.stdout.read()
    sys.stdout.close()
    sys.stdout = old_stdout
    check_results("No filters, select all columns - std out", read_string_into_lists(out), read_file_into_lists("data/small.tsv"))

    index_columns = ["ID", "CategoricalB", "CategoricalB_endswith", "FloatA", "FloatB", "IntA", "IntB", "OrdinalA", ["CategoricalB", "IntB"], ["FloatA", "OrdinalA", "IntA"]]

    # Small tests with indexing
    run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_parallel = 1, index_columns = index_columns)
    run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_parallel = 2, index_columns = index_columns)

    # Small tests with z-standard compression
    run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_parallel = 1, compression_type = "zstd")
    run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_parallel = 2, compression_type = "zstd")

    # Small tests with z-standard compression (and indexing)
    run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_parallel = 1, compression_type = "zstd", index_columns = index_columns)
    run_small_tests("data/small.tsv", f4_file_path, out_file_path, num_parallel = 2, compression_type = "zstd", index_columns = index_columns)

    # Transpose without compression
    f4_transposed_file_path = "/tmp/small_transposed.f4"
    test_transpose("data/small.tsv", f4_file_path, f4_transposed_file_path, num_parallel = 1, compression_type = None, use_memory_mapping=True)
    test_transpose("data/small.tsv", f4_file_path, f4_transposed_file_path, num_parallel = 2, compression_type = None, use_memory_mapping=True)

    # Transpose with zstd compression
    f4_transposed_file_path = "/tmp/small_transposed_zstd.f4"
    test_transpose("data/small.tsv", f4_file_path, f4_transposed_file_path, num_parallel = 1, compression_type = "zstd", use_memory_mapping=True)
    test_transpose("data/small.tsv", f4_file_path, f4_transposed_file_path, num_parallel = 2, compression_type = "zstd", use_memory_mapping=True)

    # Inner join without compression
    test_inner_join(num_parallel = 1, compression_type = None, use_memory_mapping=True)

    # Inner join with compression
    test_inner_join(num_parallel = 1, compression_type = "zstd", use_memory_mapping=True)

    # No memory mapping
    for num_parallel in [1, 2]:
        run_small_tests("data/small.tsv", f4_file_path, "/tmp/small_out.tsv", num_parallel = num_parallel, use_memory_mapping=False)
        test_transpose("data/small.tsv", f4_file_path, "/tmp/small_transposed.f4", num_parallel = num_parallel, compression_type = None, use_memory_mapping=False)
        test_inner_join(num_parallel = num_parallel, compression_type = None, use_memory_mapping=False)

    # Clean up data files
    for file_path in glob.glob(f"{f4_file_path}*"):
        os.unlink(file_path)

def run_super_tests(num_parallel, size, extension, compression_type, verbose, tmp_dir_path=None):
    in_file_path = f"data/{size}.tsv{extension}"
    f4_file_path = f"data/{size}.f4.zstd" if compression_type else f"data/{size}.f4"
    out_file_path = "/tmp/f4_out.tsv"

    print("-------------------------------------------------------------------")
    print(f"Converting {in_file_path} to {f4_file_path} (cmpr: {compression_type})")
    print("-------------------------------------------------------------------")

    # Clean up files if they already exist
    if os.path.exists(f4_file_path):
        os.unlink(f4_file_path)
    if os.path.exists(out_file_path):
        os.unlink(out_file_path)

    f4.convert_delimited_file(in_file_path, f4_file_path, compression_type=compression_type, num_parallel=num_parallel, index_columns=["X1", "X2", ["X1", "X2"]], verbose=verbose, tmp_dir_path=tmp_dir_path)

    run_super_test("[Ignore] Just priming the timer...", f4.HeadFilter(n = 1), ["X1"], num_parallel, tmp_dir_path, f4_file_path, out_file_path)
    run_super_test("Querying all rows, one column", f4.NoFilter(), ["X2"], num_parallel, tmp_dir_path, f4_file_path, out_file_path)

    run_super_test("Querying many rows, one column - String filter, no index", f4.StringFilter("X3", operator.eq, "A"), ["X4"], num_parallel, tmp_dir_path, f4_file_path, out_file_path)
    run_super_test("Querying many rows, one column - String filter, index", f4.StringFilter("X1", operator.eq, "A"), ["X2"], num_parallel, tmp_dir_path, f4_file_path, out_file_path)

    select_columns = ["X1", "X2", "X3", "X4"]

    fltr = f4.AndFilter(f4.StringFilter("X3", operator.eq, "A"), f4.StringFilter("X4", operator.eq, "A"))
    run_super_test("Querying fewer rows (AndFilter), four columns, no index", fltr, select_columns, num_parallel, tmp_dir_path, f4_file_path, out_file_path)

    fltr = f4.AndFilter(f4.StringFilter("X1", operator.eq, "A"), f4.StringFilter("X2", operator.eq, "A"))
    run_super_test("Querying fewer rows (AndFilter), four columns, multi-column index", fltr, select_columns, num_parallel, tmp_dir_path, f4_file_path, out_file_path)

    run_super_test("Querying all columns, one row", f4.HeadFilter(n = 1), [], num_parallel, tmp_dir_path, f4_file_path, out_file_path)

    select_columns = [f"X{i}" for i in range(1, min(f4.get_num_cols(f4_file_path) + 1, 1000001))]
    run_super_test("Querying many columns, one row", f4.HeadFilter(n = 1), select_columns, num_parallel, tmp_dir_path, f4_file_path, out_file_path)

    run_super_test("Querying a single value", f4.HeadFilter(n = 1), ["X1"], num_parallel, tmp_dir_path, f4_file_path, out_file_path)

    os.unlink(out_file_path)

def run_super_test(description, fltr, select_columns, num_parallel, tmp_dir_path, f4_file_path, out_file_path, use_memory_mapping):
    print(f"{description}:")
    start_time = time.time()

    f4.query(f4_file_path, fltr, select_columns, out_file_path, num_parallel=num_parallel, tmp_dir_path=tmp_dir_path, use_memory_mapping=use_memory_mapping)

    end_time = time.time()
    elapsed_time = end_time - start_time

    num_lines = 0
    with open(out_file_path, "r", newline="\n") as out_file:
        for line in out_file:
            num_lines += 1

    print(f"  {elapsed_time:.2f} seconds, {num_lines} lines in output file")

#run_all_small_tests()
#sys.exit()

for compression_type in [None]:
#for compression_type in ["zstd"]:
#for compression_type in [None, "zstd"]:
#    num_parallel = 1
#    num_parallel = 4
    num_parallel = 16
    build_outputs = True
    #build_outputs = False
    #verbose = True
    verbose = False
    check_outputs = True
    #check_outputs = False
    use_memory_mapping=True
    #use_memory_mapping=False
    #do_test_with_indexing = True
    do_test_with_indexing = False

    # Medium tests
#    run_larger_tests(num_parallel=num_parallel, size="medium", extension="", discrete1_index=11, numeric1_index=21, build_outputs=build_outputs, compression_type=compression_type, check_outputs=check_outputs, verbose=verbose, tmp_dir_path="/tmp/medium", use_memory_mapping=use_memory_mapping, do_test_with_indexing=do_test_with_indexing)

    # Large tests
    run_larger_tests(num_parallel=num_parallel, size="large_tall", extension="", discrete1_index=251, numeric1_index=501, build_outputs=build_outputs, compression_type=compression_type, check_outputs=check_outputs, verbose=verbose, tmp_dir_path="/tmp/large_tall", use_memory_mapping=use_memory_mapping, do_test_with_indexing=do_test_with_indexing)
#    run_larger_tests(num_parallel=num_parallel, size="large_wide", extension="", discrete1_index=250001, numeric1_index=500001, build_outputs=build_outputs, compression_type=compression_type, check_outputs=check_outputs, verbose=verbose, tmp_dir_path="/tmp/large_wide", use_memory_mapping=use_memory_mapping, do_test_with_indexing=do_test_with_indexing)
#
#    run_super_tests(num_parallel=num_parallel, size="test_tall", extension=".gz", compression_type=compression_type, verbose=verbose, tmp_dir_path="/tmp/test_tall", use_memory_mapping=use_memory_mapping)
#    run_super_tests(num_parallel=num_parallel, size="test_wide", extension=".gz", compression_type=compression_type, verbose=verbose, tmp_dir_path="/tmp/test_wide", use_memory_mapping=use_memory_mapping)

#    run_super_tests(num_parallel=num_parallel, size="kinda_tall", extension=".gz", compression_type=compression_type, verbose=verbose, tmp_dir_path="/tmp/kinda_tall", use_memory_mapping=use_memory_mapping)
#    run_super_tests(num_parallel=num_parallel, size="kinda_wide", extension=".gz", compression_type=compression_type, verbose=verbose, tmp_dir_path="/tmp/kinda_wide", use_memory_mapping=use_memory_mapping)

# FYI: The following tests do not yet work when zstd is used.
#      Implementing custom compression will fix this approach.

#    run_super_tests(num_parallel=num_parallel, size="super_tall", extension=".gz", compression_type=compression_type, verbose=verbose, tmp_dir_path="/tmp/super_tall", use_memory_mapping=use_memory_mapping)
#    run_super_tests(num_parallel=num_parallel, size="super_wide", extension=".gz", compression_type=compression_type, verbose=verbose, tmp_dir_path="/tmp/super_wide", use_memory_mapping=use_memory_mapping)

#    run_super_tests(num_parallel=num_parallel, size="hyper_tall", extension=".gz", compression_type=compression_type, verbose=verbose, tmp_dir_path="/tmp/hyper_tall", use_memory_mapping=use_memory_mapping)
#    run_super_tests(num_parallel=num_parallel, size="hyper_wide", extension=".gz", compression_type=compression_type, verbose=verbose, tmp_dir_path="/tmp/hyper_wide", use_memory_mapping=use_memory_mapping)

    #Attempt this? https://community.hpe.com/t5/servers-systems-the-right/cray-graph-engine-takes-on-a-trillion-triples/ba-p/7096770

#    f4.transpose("data/medium.f4", "/tmp/medium_transposed.f4", src_column_for_names="ID", num_parallel=num_parallel, verbose=verbose, use_memory_mapping=use_memory_mapping)
#    f4.transpose("data/large_tall.f4", "/tmp/large_tall_transposed.f4", src_column_for_names="ID", num_parallel=num_parallel, verbose=verbose, use_memory_mapping=use_memory_mapping)

#    f4.inner_join("data/medium.f4", "data/medium.f4", "ID", "/tmp/medium_joined.f4", num_parallel=num_parallel, verbose=verbose, use_memory_mapping=use_memory_mapping)
#    f4.inner_join("data/large_tall.f4", "data/large_wide.f4", "ID", "/tmp/large_joined.f4", num_parallel=num_parallel, verbose=verbose, use_memory_mapping=use_memory_mapping)

print("All tests passed!!")
