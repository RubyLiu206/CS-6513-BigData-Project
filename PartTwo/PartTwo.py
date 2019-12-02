import sys
import numpy as np
import json
import re
import csv
from pyspark import SparkContext


def get_type(string):
    if re.match("http://[\w\.]+\.(com|cn|org|edu)", string.lower()) is not None:
        return "WebSites"
    return "Others"


def semanticCheck(sc, file_name):
    file_name = file_name[1:-1]
    file_path = "/user/hm74/NYCColumns/" + str(file_name).strip()
    print(file_path)

    data = sc.textFile(file_path, 1).mapPartitions(
        lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
    # key is semantic type, value is count
    semantic_information = {}
    semantic_type = data.map(lambda x: (get_type(x[0]), int(
        x[1]))).reduceByKey(lambda x, y: x+y).collect()

    for row in semantic_type:
        semantic_information["semantic_type"] = row[0]
        semantic_information["count"] = row[1]

    with open(file_name+'_semantic_result.json', 'w') as fp:
        json.dump({"semantic_types": semantic_information}, fp)


sc = SparkContext()

file_list = open('cluster3.txt').readline().strip().split(",")
semanticCheck(sc, file_list[0])

# for item in file_list:
#     semanticCheck(sc, item)
#
