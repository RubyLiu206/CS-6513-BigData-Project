import sys
import subprocess
import numpy as np
import json
import re
from pyspark import SparkContext
from pyspark.sql import SQLContext

# identify the data type
# this needs to be tested


def get_type(data):
    if re.match("^\d+?\.\d+?$", data) is not None:
        return "REAL"
    elif re.match("^\s*-?[0-9]{1,10}\s*$", data) is not None:
        return "INTEGER (LONG)"
    elif re.match('^(([0-1]?[0-9])|([2][0-3])):([0-5]?[0-9]):([0-5]?[0-9])$', data) is not None or re.match('[0-9]{2}/[0-9]{2}/[0-9]{4}', data) is not None:
        return "DATE/TIME"
    else:
        return "TEXT"


def data_with_type(data):
    type_ = get_type(data)
    return data, type_


def get_file_path():
    cmd = "hdfs dfs -ls /user/hm7/NYCOpenData"
    files = subprocess.Popen('hdfs dfs -ls /user/hm74/NYCOpenData',
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    count = 0
    final_file_name = []

    for line in files.stdout.readlines():
        line = str(line).strip().split()
        final_file_name.append(line[-1].strip())
        count += 1


def profile_single_file(sc, file):
    lines = sc.textFile(file, 1)

    # get the number of rows this dataset has
    # print("There are ", lines.count(), " lines in the file")
    # split all lines with \t
    lines = lines.map(lambda x: x.split('\t'))
    # get the header which is the column name
    header = lines.first()
    # print("the column name are: ", header)

    # modify the dataset without the header row
    lines_without_header = lines.filter(lambda line: line != header)

    num_unique_value = []
    columns_information = []
    # go through every column
    for i in range(len(header)):
        #print("\nCurrent Column: ", header[i])
        # Part one: question 2 --- count the empty column:
        number_empty = lines_without_header.map(lambda x: x[i]).filter(
            lambda x: x is None or x == 'No Data').count()
        # Part one: question 1 --- count the non empty column:
        number_non_empty = lines_without_header.map(
            lambda x: x[i]).count() - number_empty
        #print("Number of non-empty cells: ", number_non_empty)
        #print("Number of empty-cells: ", number_empty)

        # Part one: question 3 --- number of distinct number
        number_distinct = lines_without_header.map(
            lambda x: (x[i], 1)).reduceByKey(lambda x, y: x+y).count()
        #print("Number of distinct values: ", number_distinct)
        num_unique_value.append(number_distinct)

        # Part one: question 4 --- the most 5 frequency items in each column
        top_five_freq = []
        number_frequency = lines_without_header.map(lambda x: (x[i], 1)).reduceByKey(
            lambda x, y: x+y).sortBy(lambda x: x[1], False).take(5)
        for j in range(len(number_frequency)):
            top_five_freq.append(number_frequency[j][0].strip())
        #print("Top-5 most frequent values: ", top_five_freq)

        # Part one: question 5 --- get the data type
        data_type = []
        column_data_types = lines_without_header.map(
            lambda x: (data_with_type(x[i]), 1))

        column_data_if_int = column_data_types.filter(
            lambda x: x[1] == 'INTEGER (LONG)')
        column_data_if_real = column_data_types.filter(
            lambda x: x[1] == 'REAL')
        column_data_if_datetime = column_data_types.filter(
            lambda x: x[1] == 'DATE/TIME')
        column_data_if_text = column_data_types.filter(
            lambda x: x[1] == 'TEXT')

        if column_data_if_int.count() != 0:
            column_data = column_data_if_int.map(lambda x: x[0])
            # max and min
            max_value = column_data.sortBy(lambda x: x, False).take(1)
            min_value = column_data.sortBy(lambda x: x, True).take(1)
            # average and std
            column_data = np.array(column_data.collect()).astype('float')
            mean_value = np.mean(column_data)
            std = np.std(column_data)
            data_type.append({"type": "INTEGER (LONG)", "count": len(column_data), "max_value": max_value,
                              "min_value": min_value, "mean": mean_value, "stddev": std})

        # for pair in find_data_type:
        #     data_type.append({"type": pair[0], "count": pair[1]})

        columns_information.append({"column_name": header[i], "number_non_empty_cells": number_non_empty, "number_empty_cells":
                                    number_empty, "number_distinct_values": number_distinct, "frequent_values": top_five_freq, "data_type": data_type})

    # extra credit
    # should be distinct value == number of lines
    key_column_candidates = []
    for i in range(len(header)):
        if num_unique_value[i] == lines_without_header.count():
            key_column_candidates.append(header[i])

    basic_information = {"dataset_name": file, "columns": columns_information,
                         "key_column_candidates": key_column_candidates}
    with open('result.json', 'w') as fp:
        json.dump(basic_information, fp)

    # # Part one question with data type ---- 2 ---- get the maximum value and minumum value in DATE type
    # # TODO:  test this one!!
    # DATE_result = []
    # for i in range(len(header)):

    #     column_data_for_datetime = lines_without_header.map(
    #         lambda x: data_with_type(x[i])).filter(lambda x: x[1] == 'DATETIME')

    #     if test_empty_RDD(column_data_for_datetime):
    #         max_date_time = column_data_for_datetime.map(lambda x: x.strip(
    #             '/')).sortBy(lambda x: x[2], False).sortBy(lambda x: x[1], False).sortBy(lambda x: x[0], False).take(1)
    #         min_date_time = column_data_for_datetime.map(lambda x: x.strip(
    #             '/')).sortBy(lambda x: x[2], False).sortBy(lambda x: x[1], False).sortBy(lambda x: x[1], False).take(1)
    #         print("the max datetime in " + '\t' + str(i) + '\t' +
    #               " column is:" + '\t' + str(max_date_time))
    #         print("the min datetime in:" + '\t' + str(i) +
    #               '\t' + "column is:" + '\t' + str(min_date_time))
    #         DATE_result.append([i, max_date_time, min_date_time])
    # # print(DATE_result)
    # # Part one question with data type ---- 3 ---- get the top 5 shortest value and top 5 longest value and average value length in TEXT type

    # TEXT_result = []
    # for i in range(len(header)):

    #     column_data_for_text = lines_without_header.map(lambda x: (data_with_type(
    #         x[i]), 1)).filter(lambda x: x[0][1] == 'TEXT' and x[0][0] != "No Data")

    #     if test_empty_RDD(column_data_for_text):
    #         text_data_with_length = column_data_for_text.map(
    #             lambda x: (x[0][0], len(x[0][0].strip())))
    #         # print(text_data_with_length)
    #         # fot the top 5 value, we need to distinct to get the distinct value
    #         # otherwise you will get 10 ELEMENT for many times
    #         # TODO: question is there are some space in data, but also count in --- done
    #         top_longest_length = text_data_with_length.sortBy(
    #             lambda x: x[1], False).distinct().take(5)
    #         top_shortest_length = text_data_with_length.sortBy(
    #             lambda x: x[1], True).distinct().take(5)
    #         print("the top shortest length data in " + '\t' + str(i) +
    #               "column is: " + '\t' + str(top_shortest_length))
    #         print("the top longest length data in: " + '\t' + str(i) +
    #               "column is: " + '\t' + str(top_longest_length))
    #         # average value length
    #         # or the same method with int data type, not sure which one is better
    #         length = text_data_with_length.map(lambda x: (x[1], 1)).reduceByKey(
    #             lambda x, y: x+y).map(lambda x: (x[0]*x[1], x[1])).collect()
    #         length = np.array(length)
    #         sum_ = 0
    #         count = 0
    #         for i in length:
    #             sum_ += i[0]
    #             count += i[1]
    #         if count != 0:
    #             print("the average length of text data in:" +
    #                   '\t' + str(i) + "column is:" + str(sum_/count))
    #             TEXT_result.append(
    #                 [i, top_shortest_length, top_longest_length, sum_/count])
    # # TODO: write into json file

    # # print(TEXT_result)


# some initialization
sc = SparkContext()
sqlContext = SQLContext(sc)
profile_single_file(sc, "/user/hm74/NYCOpenData/vxxs-iyt2.tsv.gz")
