import sys
import numpy as np
import json
import re
import csv
from pyspark import SparkContext

# change get_semantic_type function to add more semantic types


def get_semantic_type(line):
    if re.match("http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+", line) is not None:
        return "WebSites"
    elif re.match("(?:\+?(\d{1})?-?\(?(\d{3})\)?[\s-\.]?)?(\d{3})[\s-\.]?(\d{4})[\s-\.]?", line) is not None:
        return "Phone Number"
    elif re.match("\(\d+, \d+\)", line) is not None:
        return "LAT/LON coordinates"
    elif re.match("r\d[-(0-9a-z)]+", line) is not None:
        return "Building Classification"
    return "Others"


def semanticCheck(sc, file_name):
    file_name = file_name.strip()[1:-1]
    file_path = "/user/hm74/NYCColumns/" + str(file_name)

    data = sc.textFile(file_path, 1).mapPartitions(
        lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
    # key is semantic type, value is count
    semantic_information = {}
    semantic_type = data.map(lambda x: (get_semantic_type(x[0].lower()), int(
        x[1]))).reduceByKey(lambda x, y: x+y).collect()

    for row in semantic_type:
        semantic_information["semantic_type"] = row[0]
        semantic_information["count"] = row[1]

    with open(file_name+'_semantic_result.json', 'w') as fp:
        json.dump({"semantic_types": semantic_information}, fp)


sc = SparkContext()

file_list = open('cluster3.txt').readline().strip().split(",")

# for item in file_list:
#     semanticCheck(sc, item)

semanticCheck(sc, file_list[0])
