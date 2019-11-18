import sys
import numpy as np
import pandas as pd
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkConf, SparkContext
from csv import reader
import re
from pyspark.sql import SQLContext
import json



def get_all_column():
	file_ = open('cluster3.txt')
	line = file_.readline()
	file_with_column = line.split(",")
	file_list = []
	column_list = []
	for file_name in file_with_column:
		file_name = eval(file_name)
		file_list.append(file_name[0:10]+'tsv.gz')
		column_list.append(file_name[10:-7])
	#print(file_list)
	#print(column_list)
	return file_list,column_list

		



def semantic_profiling_file(sc, file_path, column_name):
	lines = sc.textFile(file_path,1)
	print("ther are:" + "\t" + str(lines.count()) + "lines in the file")
	
	lines = lines.map(lambda x: x.split('\t'))
	header = lines.first()
	header_string = list(header)

	specific_column_index = header_string.index(column_name)
	print("the column names are :" + str(header))
	print("the specific column:" + str(specific_column_index))

	lines_without_header = lines.filter(lambda line: line!=header)
	
	specific_column = lines_without_header.map(lambda x: x[specific_column_index])
	print(specific_column.take(5))

	
	

def read_into_RDD(file_name, column_name):
	sc = SparkContext()
	sqlContext = SQLContext(sc)
	#5694-9szk.Business_Website_or_Other_URL.txt.gz for test
	column_name = str(column_name)
	column_name = column_name.replace("_", " ")
	semantic_profiling_file(sc, "/user/hm74/NYCOpenData/"+str(file_name), column_name)


file_list, column_list = get_all_column()
read_into_RDD(file_list[0], column_list[0])
