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

#identify the data type
def data_type(data):
	if re.match("^\d+?\.\d+?$", data) is not None:
	    return "FLOAT"
	# not sure if the REAL is same with int
	elif re.match("^\s*-?[0-9]{1,10}\s*$", data) is not None: 
	    return "REAL"
	elif re.match('^(([0-1]?[0-9])|([2][0-3])):([0-5]?[0-9]):([0-5]?[0-9])$', data) is not None or re.match('[0-9]{2}/[0-9]{2}/[0-9]{4}', data) is not None:
	    return "DateTime"
	else:
	    return "TEXT"
	 


def data_with_type(data):
	
	type_ = data_type(data)
	return data,type_


  

def main():
	# some initialization 
	sc = SparkContext()
	sqlContext = SQLContext(sc)
	
	# get the input gz file
	# need change, after testing pass
	file_path = sys.argv[1]    
	lines = sc.textFile(file_path,1)
	
	#get the number of rows this ds has 
	print("there are " + '\t' + str(lines.count()) + "lines in the file")
	#split all lines with \t
	#need test, because we don't know whether all the gz file have the same format with the gz we testing 
	lines = lines.map(lambda x: x.split('\t'))
	#get the header which is the column name
	header = lines.first()

	print("the column name are: " + '\t' + str(header))

	# modify the dataset without the header row
	lines_without_header = lines.filter(lambda line: line != header)


	for i in range(len(header)):
		#each column data
		column_data = lines_without_header.map(lambda x:x[i]).collect()
		#if you want to test the result of this line
		#print(line_data.take(5))
		
		#Part one: question 2 --- count the empty column, 
		number_empty = count_empty(column_data)
		#Part one: question 1 --- count the non empty column,
		#the method we use is the total row minus the empty row, which will be more efficiency
		number_non_empty = len(column_data)-number_empty
		print("number of empyt data"+ '\t'+str(number_empty))
		print("number of non empty data" + '\t' + str(number_non_empty))
	
		#Part one: question 3 --- number of distinct number
		number_distinct =lines_without_header.map(lambda x : (x[i], 1)).reduceByKey(lambda x,y : x+y).collect()
		#Part one: question 4 --- the most 5 frequency items in each column
		#the method we use is the same with the homework
		number_frequency = lines_without_header.map(lambda x:(x[i],1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False).take(5)
		#another way to find distinct
		#result = find_distinct(column_data)
		#print("number of distinct" + '\t'+str(result))
		print("using spark, number of distinct" + '\t' + str(len(number_distinct)))
		
		five_freq = []	
		#TODO: try to find a more efficiency way to get the five freq,
		#in this method, we need to initial a list to store
		for i in range(len(number_frequency)):
			five_freq.append(number_frequency[i][0])
		print("the top five frequency" + '\t' + str(five_freq)) 


	#Part one: question 5 --- get the data type
	#TODO: we need to count the different data type(actually done with this part, but need test)
	for i in range(len(header)):
		find_data_type = lines_without_header.map(lambda x:(data_type(x[i]),1)).reduceByKey(lambda x,y:	x+y).collect()
		print("the data type in this column is :" + '\t' + str(find_data_type)) 


	#Part oneL question with data type ---- 1 ---- get the max and min from datatype long and real
	for i in range(len(header)):
		column_data_for_int = lines_without_header.map(lambda x: (data_with_type(x[i]),1)).filter(lambda x: x[0][1] == 'INT' or x[0][1] == 'FLOAT')
		max_data_int_long = column_data_for_int.sortBy(lambda x: x[0][0], False).take(1)
		min_data_int_long = column_data_for_int.sortBy(lambda x: x[0][0], True).take(1)
		print("the max data in this column with data type int and long" + '\t' + str(max_data_int_long))
		print("the min data in this column with data type int and long" + '\t' + str(min_data_int_long))
		
		#to compute the mean and std
		#the method is trans to the numpy for each column
		#but not efficiency
		#TODO;find spark way to do that, or using the sql
		mean_data_int_long = column_data_for_int.map(lambda x: x[0][0]).collect()
		mean_data_int_long = np.array(mean_data_int_long).astype('float')
		mean_value = np.mean(mean_data_int_long)
		print("the mean of this column data is :" +'\t' + str(mean_value))
		std = np.std(mean_data_int_long)
		print("the std of this column data is :" +'\t' + str(std))
		#if we want to be more efficiency, not dealing with the column not in REAL and LONG type
		#TODO: find a way to not go those column

	#Part one question with data type ---- 2 ---- get the maximum value and minumum value in DATE type


	#Part one question with data type ---- 3 ---- get the top 5 shortest value and top 5 longest value and average value length in TEXT type

	for i in range(len(header)):
		column_data_for_text = lines_without_header.map(lambda x: (data_with_type(x[i]),1)).filter(lambda x: x[0][1] == 'TEXT' and x[0][0] != "No Data")
		text_data_with_length = column_data_for_text.map(lambda x:(x[0][0],len(x[0][0].strip())))
		#print(text_data_with_length)
		# fot the top 5 value, we need to distinct to get the distinct value
		# otherwise you will get 10 ELEMENT for many times
		#TODO: question is there are some space in data, but also count in
		top_longest_length = text_data_with_length.sortBy(lambda x: x[1], False).distinct().take(5)
		top_shortest_length = text_data_with_length.sortBy(lambda x: x[1], True).distinct().take(5)
		print("the top shortest length data is: " + '\t' + str(top_shortest_length))
		print("the top longest length data is: " + '\t' + str(top_longest_length))

		


main()
