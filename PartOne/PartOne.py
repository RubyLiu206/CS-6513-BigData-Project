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

#identify the data type
def data_type(data):
	if re.match("^\d+?\.\d+?$", data) is not None:
	    return "REAL"
	# not sure if the REAL is same with int
	elif re.match("^\s*-?[0-9]{1,10}\s*$", data) is not None: 
	    return "INTEGER"
	elif re.match('^(([0-1]?[0-9])|([2][0-3])):([0-5]?[0-9]):([0-5]?[0-9])$', data) is not None or re.match('[0-9]{2}/[0-9]{2}/[0-9]{4}', data) is not None:
	    return "DateTime"
	else:
	    return "TEXT"
	 


def data_with_type(data):
	
	type_ = data_type(data)
	return data,type_

#method to test whether this RDD is empty
#we can also use the rdd.partitions.isEmpty()
#but that method may not work for some situation, like the initial block rdd is already divided
def test_empty_RDD(RDD):
	return RDD.count() != 0


def get_file_path():

	cmd = "hdfs dfs -ls /user/hm7/NYCOpenData"
	files = subprocess.Popen('hdfs dfs -ls /user/hm74/NYCOpenData' , stdout = subprocess.PIPE, stderr = subprocess.STDOUT, shell = True)
	count = 0
	final_file_name = []

	for line in files.stdout.readlines():
		line = str(line).strip().split()
		final_file_name.append(line[-1].strip())
		count += 1
	#test
	print(final_file_name[1])

# use vxxs-iyt2.tsv.gz as our test
def main():
	# some initialization 
	sc = SparkContext()
	sqlContext = SQLContext(sc)
	
	# get the input gz file
	# need change, after testing pass
	file_path = sys.argv[1]    # file path is single file name
	lines = sc.textFile(file_path,1)
	#TODO: according to the instor, we need to automatic go through all gz file in the path
	#file PatrOne get all file path already get from get_file_path function
	#need to using in here


	#get the number of rows this ds has 
	print("There are ", lines.count(), " lines in the file")
	#split all lines with \t
	#need test, because we don't know whether all the gz file have the same format with the gz we testing 
	lines = lines.map(lambda x: x.split('\t'))
	#get the header which is the column name
	header = lines.first()

	print("the column name are: " + '\t' + str(header))

	# modify the dataset without the header row
	lines_without_header = lines.filter(lambda line: line != header)

	# go through every column
	for i in range(len(header)):
		
		#Part one: question 2 --- count the empty column:
		number_empty = lines_without_header.map(lambda x : x[i]).filter(lambda x : x is None or x =='No Data').count()
		#Part one: question 1 --- count the non empty column:
		number_non_empty = lines_without_header.map(lambda x : x[i]).count() - number_empty

		print("Number of non-empty cells: ", number_non_empty)
		print("Number of empyt-cells: ", number_empty)
				
		#Part one: question 3 --- number of distinct number
		number_distinct = lines_without_header.map(lambda x : (x[i], 1)).reduceByKey(lambda x,y : x+y).count()
		print("Number of distinct values: ", number_distinct)

		#Part one: question 4 --- the most 5 frequency items in each column
		top_five_freq = []
		number_frequency = lines_without_header.map(lambda x : (x[i], 1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[1], False).take(5)
		for i in range(len(number_frequency)):
			top_five_freq.append(number_frequency[i][0])
		print("Top-5 most frequent values: ", top_five_freq) 

	#Part one: question 5 --- get the data type
	#TODO: we need to count the different data type(actually done with this part, but need test)
	for i in range(len(header)):
		find_data_type = lines_without_header.map(lambda x:(data_type(x[i]),1)).reduceByKey(lambda x,y:	x+y).collect()
		print("the data type in this column is :" + '\t' + str(find_data_type)) 

	#Part oneL question with data type ---- 1 ---- get the max and min from datatype long and real
	for i in range(len(header)):	
		
		column_data_for_int = lines_without_header.map(lambda x: (data_with_type(x[i]),1)).filter(lambda x: x[0][1] == 'INT' or x[0][1] == 'FLOAT')
		
		if test_empty_RDD(column_data_for_int):
			max_data_int_long = column_data_for_int.sortBy(lambda x: x[0][0], False).take(1)
			min_data_int_long = column_data_for_int.sortBy(lambda x: x[0][0], True).take(1)
			print("the max data in this column with data type int and long" + '\t' + str(max_data_int_long))
			print("the min data in this column with data type int and long" + '\t' + str(min_data_int_long))
		
			#to compute the mean and std
			#the method is trans to the numpy for the column which contains INT FLOAT type data
			#TODO;find spark way to do that, or using the sql
			mean_data_int_long = column_data_for_int.map(lambda x: x[0][0]).collect()
			mean_data_int_long = np.array(mean_data_int_long).astype('float')
			mean_value = np.mean(mean_data_int_long)
			print("the mean of this column data is :" +'\t' + str(mean_value))
			std = np.std(mean_data_int_long)
			print("the std of this column data is :" +'\t' + str(std))
			#if we want to be more efficiency, not dealing with the column not in REAL and LONG type
			#TODO: find a way to not go those column --- done

	#Part one question with data type ---- 2 ---- get the maximum value and minumum value in DATE type
	#TODO:  test this one!! 
	for i in range(len(header)):
		
		column_data_for_datetime = lines_without_header.map(lambda x: data_with_type(x[i])).filter(lambda x:x[1] == 'DATETIME')
		
		if test_empty_RDD(column_data_for_datetime):	
			max_date_time = column_data_for_datetime.map(lambda x: x.strip('/')).sortBy(lambda x: x[2],False).sortBy(lambda x: x[1], False).sortBy(lambda x: x[0], False).take(1)
			min_date_time = column_data_for_datetime.map(lambda x: x.strip('/')).sortBy(lambda x: x[2], False).sortBy(lambda x: x[1], False).sortBy(lambda x: x[1], False).take(1)
			print("the max datetime is:" + '\t' + str(max_date_time))
			print("the min datetime is:" + '\t' + str(min_date_time))


	#Part one question with data type ---- 3 ---- get the top 5 shortest value and top 5 longest value and average value length in TEXT type

	for i in range(len(header)):
		
		column_data_for_text = lines_without_header.map(lambda x: (data_with_type(x[i]),1)).filter(lambda x: x[0][1] == 'TEXT' and x[0][0] != "No Data")
		
		if test_empty_RDD(column_data_for_text):
			text_data_with_length = column_data_for_text.map(lambda x:(x[0][0],len(x[0][0].strip())))
			#print(text_data_with_length)
			# fot the top 5 value, we need to distinct to get the distinct value
			# otherwise you will get 10 ELEMENT for many times
			#TODO: question is there are some space in data, but also count in --- done
			top_longest_length = text_data_with_length.sortBy(lambda x: x[1], False).distinct().take(5)
			top_shortest_length = text_data_with_length.sortBy(lambda x: x[1], True).distinct().take(5)
			print("the top shortest length data is: " + '\t' + str(top_shortest_length))
			print("the top longest length data is: " + '\t' + str(top_longest_length))
			#average value length
			#or the same method with int data type, not sure which one is better
			length = text_data_with_length.map(lambda x: (x[1],1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0]*x[1],x[1])).collect()
			length = np.array(length)
			sum_ = 0
			count = 0
			for i in length:
				sum_ += i[0]
				count += i[1]
			if count != 0:	
				print("the average length of text data:" + '\t' + str(sum_/count))

	#TODO: write into json file

		


main()
