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


#find empty data
def count_empty(data):
#input is column
	count = 0
	for i in range(len(data)):
		if data[i] is None or data[i] =='No Data':
			count += 1
	return count

#find the number of distinct data
def find_distinct(data):
	distinct_data = set(data)
	return len(distinct_data)

def data_type(data):
	if re.match("^\d+?\.\d+?$", data) is not None:
	    return "FLOAT"
	elif re.match("^\s*-?[0-9]{1,10}\s*$", data) is not None: 
	    return "INT"
	elif re.match('^(([0-1]?[0-9])|([2][0-3])):([0-5]?[0-9]):([0-5]?[0-9])$', data) is not None or re.match('[0-9]{2}/[0-9]{2}/[0-9]{4}', data) is not None:
	    return "DateTime"
	else:
	    return "TEXT"
	   


sc = SparkContext()
sqlContext = SQLContext(sc)

file_path = sys.argv[1]    
lines = sc.textFile(file_path,1)
lines = lines.map(lambda x: x.split('\t'))
header = lines.first()

#print("length of column"+str(len(header)))
#print(header)


lines_without_header = lines.filter(lambda line: line != header)
#print(lines_without_header.take(5))



for i in range(len(header)):
	#each column data
	column_data = lines_without_header.map(lambda x:x[i]).collect()
	#print(line_data.take(5))
	number_empty = count_empty(column_data)
	number_non_empty = len(column_data)-number_empty
	print("number of empyt data"+ '\t'+str(number_empty))
	print("number of non empty data" + '\t' + str(number_non_empty))
	number_distinct =lines_without_header.map(lambda x : (x[i], 1)).reduceByKey(lambda x,y : x+y).collect()
	number_frequency = lines_without_header.map(lambda x:(x[i],1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False).take(5)
	#another way to find distinct
	#result = find_distinct(column_data)
	#print("number of distinct" + '\t'+str(result))
	print("using spark, number of distinct" + '\t' + str(len(number_distinct)))
	five_freq = []
	for i in range(len(number_frequency)):
		five_freq.append(number_frequency[i][0])
	print("the top five frequency" + '\t' + str(five_freq)) 



for i in range(len(header)):

	find_data_type = lines_without_header.map(lambda x:(data_type(x[i]),1)).reduceByKey(lambda x,y:	x+y).collect()
	
	print(find_data_type)
	print("the data type in this column is :" + '\t' + str(find_data_type)) 
#line_data_5 = line_data.toDF()
#print(line_data_5[0])
#print(column)
#q_1 = spark.sql("SELECT COUNT(column NOT NULL) FROM column")

#for i in range(len(header)):
#	column = line_data.map(lambda x:x[i])
#	column.createOrReplaceTempView(column)
#	q_1 = spark.sql("SELECT COUNT(column NOT NULL) as result_1 FROM column")
#	result.select(format_string("%s", result_1).write.save("try.out",format = 'text)
#line_data = line_data.sortBy(lambda x:x)

