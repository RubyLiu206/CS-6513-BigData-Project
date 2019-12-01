import sys
import numpy as np
import pandas as pd
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkConf, SparkContext
import csv
from csv import reader
import re
from pyspark.sql import SQLContext
import json

def get_file_column_name():
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


def read_into_RDD(sc, file_name, column_name):
	#sqlContext = SQLContext(sc)
	#5694-9szk.Business_Website_or_Other_URL.txt.gz for test
	column_name = str(column_name)
	column_name = column_name.replace("_", " ")
	file_path = "/user/hm74/NYCOpenData/"+str(file_name)
	lines = sc.textFile(file_path, 1).mapPartitions(lambda x: csv.reader(x, delimiter='\t', quotechar='"'))
	# get the header which is the column name
	header = lines.first()
	# print("the column name are: ", header)
	# modify the dataset without the header row
	print("ther are:" + "\t" + str(lines.count()) + "lines in the file")
	header_string = list(header)
	if column_name in header_string:
		specific_column_index = header_string.index(column_name)
		print("the column names are :" + str(header))
		print("the specific column:" + str(specific_column_index))
		lines_without_header = lines.filter(lambda line: line!=header)
		specific_column = lines_without_header.map(lambda x: x[specific_column_index])
		return specific_column
	else:
		return None


def dealing_with_column(column):	
	data_collect = column.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).sortByKey(lambda x: x[1])
	return data_collect.take(5)

def write_into_df(file_list,column_list, item):
	dataset = pd.DataFrame({'file': file_list, 'column_name': column_list, 'freq_item':item})
	#print(dataset)
	dataset.to_csv('true_type.csv', encoding='gbk')




file_list, column_list = get_file_column_name()
item = []
sc = SparkContext()

for i in range(274):
	column = read_into_RDD(sc, file_list[i], column_list[i])
	if column != None:
		each_item = dealing_with_column(column)
		item.append(each_item)
	else:
		item.append([0])


write_into_df(file_list,column_list, item)

