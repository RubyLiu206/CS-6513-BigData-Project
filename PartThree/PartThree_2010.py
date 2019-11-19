
import numpy as np
import json
import re
from pyspark import SparkContext
import csv
from csv import reader
import re
from pyspark.sql import SQLContext
import json



def get_type(data):
    if re.match("^\d+?\.\d+?$", data) is not None:
        return "REAL"
    elif re.match("^\s*-?[0-9]{1,10}\s*$", data) is not None:
        return "INTEGER (LONG)"
    elif re.match('^(([0-1]?[0-9])|([2][0-3])):([0-5]?[0-9])::([0-5]?[0-9])$', data) is not None or re.match('[0-9]{2}/[0-9]{2}/[0-9]{4}', data) is not None:
        return "DATE/TIME"
    else:
        return "TEXT"


def data_with_type(data):
    type_ = get_type(data)
    return data, type_

def sortDate(date):
    # rewrite the string to yyyymmdd then let spark sort
    date = date.split(" ")
    date = str(date[0]).split('/')
    return date[2]

def profile_single_file(sc, file):
    lines = sc.textFile(file, 1).mapPartitions(lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
    # split all lines with \t
    #lines = lines.map(lambda x: x.split('\t'))
    # get the header which is the column name
    header = lines.first()
    print("the column name are: ", header)
    header_list = list(header)
    # modify the dataset without the header row
    lines_without_header = lines.filter(lambda line: line != header)

    name = ["Complaint Type", "Borough", "Created Date"]

    date_index = header_list.index(name[2])

    type_index = header_list.index(name[0])

    bor_index = header_list.index(name[1])
    print(type_index)
    print(bor_index)
    type_bor = lines_without_header.map(lambda x: (data_with_type(x[date_index]), x[type_index], x[bor_index]))
    sort_type_bor = type_bor.map(lambda x: (sortDate(x[0]),x[1], x[2]).sortBy(lambda x: sortDate(x[0]))
    data_2010 = sort_type_bor.filter
    print(sort_type_bor.take(5))
    return sort_type_bor


def bor_analysis(type_bor):
    type_manhattan = type_bor.filter(lambda x: x[0][0] == 'MANHATTAN').sortBy(lambda x: -x[1])
    
    print(type_man.take(5))


sc = SparkContext()
# file_names = get_file_names(sc, "/user/hm74/NYCOpenData/")
# for file_name in file_names:
#     profile_single_file(sc, file_name)
# profile_single_file(sc, "/user/hm74/NYCOpenData/uvks-tn5n.tsv.gz")

# data from 2010
type_bor = profile_single_file(sc, "/user/hm74/NYCOpenData/erm2-nwe9.tsv.gz")
#bor_analysis(type_bor)



