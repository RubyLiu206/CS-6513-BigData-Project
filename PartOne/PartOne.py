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
#import matplotlib.pyplot as plt

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
	    return "REAL"
	# not sure if the REAL is same with int
	elif re.match("^\s*-?[0-9]{1,10}\s*$", data) is not None: 
	    return "INTEGER"
	elif re.match('^(([0-1]?[0-9])|([2][0-3])):([0-5]?[0-9]):([0-5]?[0-9])$', data) is not None or re.match('[0-9]{2}/[0-9]{2}/[0-9]{4}', data) is not None:
	    return "DATETIME"
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
def plot_non_empty_empty(empty, non_empty):
	name = ['empty data', 'non empty data']
	number = [empty, non_empty]
	plt.bar(name,number, align = 'center', aplha = 0.5)


def write_into_json(list_, json_file_name, json_file_save_path):
	
	with open(json_file_name, 'w') as f:
		#json.dumps(list_,indent=4, separators=(',', ': '))
		json.dump(list_, f,indent=4, separators=(',', ': '))
		
def write_into_data(final_data, data, column_number):
	final_data['column'][column_number]['data_type'] = data
	return final_data
	
def read_from_json(json_file_name):

	with open(json_file_name, 'r') as f:
		data_infor = json.load(f) 
	print(data_infor['column'][0])
	return data_infor

def profile_single_file(sc, file_path):


	lines = sc.textFile(file_path,1)

	# get the number of rows this dataset has 
	print("there are " + '\t' + str(lines.count()) + "lines in the file")

	#split all lines with \t
	lines = lines.map(lambda x: x.split('\t'))
	#get the header which is the column name
	header = lines.first()

	print("the column name are: ",str(header))

	# modify the dataset without the header row
	lines_without_header = lines.filter(lambda line: line != header)

	basic_information = []
	unique_value_each_column = []
	basic_column_information = dict()
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
		#plot_non_empty_empty(number_empty, number_non_empty)

	
		#Part one: question 3 --- number of distinct number
		number_distinct =lines_without_header.map(lambda x : (x[i], 1)).reduceByKey(lambda x,y : x+y).collect()
		#Part one: question 4 --- the most 5 frequency items in each column
		#the method we use is the same with the homework
		number_frequency = lines_without_header.map(lambda x:(x[i],1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False).take(5)
		#another way to find distinct
		#result = find_distinct(column_data)
		#print("number of distinct" + '\t'+str(result))
		print("using spark, number of distinct" + '\t' + str(len(number_distinct)))

		unique_value_each_column.append(len(number_distinct))

		five_freq = []	
		#TODO: try to find a more efficiency way to get the five freq,
		#in this method, we need to initial a list to store
		for j in range(len(number_frequency)):
			five_freq.append(number_frequency[j][0])
		#print("the top five frequency" + '\t' + str(five_freq)) 
		
		basic_column_information = {"column_name": header[i], "number_non_empty_cells":int(number_non_empty), "number_empty_cells": int(number_empty), "number_distinct_values":int(len(number_distinct)), "frequent_values": five_freq}
		basic_information.append(basic_column_information)
	most_unique_column = max(np.array(unique_value_each_column))
	index_key_column = unique_value_each_column.index(most_unique_column)

	basic_information = {"column" : basic_information}
	print(basic_information)  
	write_into_json(basic_information, "test.json", "user/xl2590/home/Project")
	# for esch data type write into data_infor
	data_infor = read_from_json("test.json")



	#Part one: question 5 --- get the data type
	#TODO: we need to count the different data type(actually done with this part, but need test)
	for i in range(len(header)):
		find_data_type = lines_without_header.map(lambda x:(data_type(x[i]),1)).reduceByKey(lambda x,y:	x+y).collect()
		print("the data type in this column is :" + '\t' + str(find_data_type)) 

	INT_result = dict()
	#Part oneL question with data type ---- 1 ---- get the max and min from datatype long and real
	for i in range(len(header)):	
		
		column_data_for_int = lines_without_header.map(lambda x: (data_with_type(x[i]),1)).filter(lambda x: x[0][1] == 'INTEGER' or x[0][1] == 'REAL')
		count = len(column_data_for_int.collect())
		if test_empty_RDD(column_data_for_int):
			max_data_int_long = column_data_for_int.sortBy(lambda x: x[0][0], False).map(lambda x: x[0][0]).take(1)
			min_data_int_long = column_data_for_int.sortBy(lambda x: x[0][0], True).map(lambda x: x[0][0]).take(1)
			print("the max data in the:" + '\t' + str(i) +'\t' + "column with data type int and long" + '\t' + str(max_data_int_long))
			print("the min data in the:" + '\t' + str(i) + '\t' + " column with data type int and long" + '\t' + str(min_data_int_long))
			
			#to compute the mean and std
			#the method is trans to the numpy for the column which contains INT FLOAT type data
			#TODO;find spark way to do that, or using the sql
			mean_data_int_long = column_data_for_int.map(lambda x: x[0][0]).collect()
			mean_data_int_long = np.array(mean_data_int_long).astype('float')
			mean_value = np.mean(mean_data_int_long)
			print("the mean of the:" + '\t'+ str(i) + '\t' + " column data is :" +'\t' + str(mean_value))
			std = np.std(mean_data_int_long)
			print("the std of the : " + '\t' + str(i) + " column data is :" +'\t' + str(std))
			#if we want to be more efficiency, not dealing with the column not in REAL and LONG type
			#TODO: find a way to not go those column --- done
			INT_result = {"type":"INTEGER","count": count,  "max_value": max_data_int_long, "min_value": min_data_int_long,"mean": mean_value, "stddev":std}
	#print(INT_result)
			data_infor = write_into_data(data_infor, INT_result, i)
	write_into_json(data_infor, "test.json","user/xl2590/home/Project")




	#Part one question with data type ---- 2 ---- get the maximum value and minumum value in DATE type
	#TODO:  test this one!! 
	DATE_result = dict()
	for i in range(len(header)):
		
		column_data_for_datetime = lines_without_header.map(lambda x: data_with_type(x[i])).filter(lambda x:x[1] == 'DATETIME')
		count = len(column_data_for_datetime.collect())
		if test_empty_RDD(column_data_for_datetime):	
			max_date_time = column_data_for_datetime.map(lambda x: x.strip('/')).sortBy(lambda x: x[2],False).sortBy(lambda x: x[1], False).sortBy(lambda x: x[0], False).take(1)
			min_date_time = column_data_for_datetime.map(lambda x: x.strip('/')).sortBy(lambda x: x[2], False).sortBy(lambda x: x[1], False).sortBy(lambda x: x[1], False).take(1)
			print("the max datetime in " + '\t' + str(i) + '\t' + " column is:" + '\t' + str(max_date_time))
			print("the min datetime in:" + '\t' + str(i) + '\t' + "column is:" + '\t' + str(min_date_time))
			DATE_result = {"type": "DATE/TYPE", "count": count,"max_value": max_date_time, "min_value":min_date_time}

			data_infor = write_into_data(data_infor, DATE_result, i)
	write_into_json(data_infor, "test.json", "user/xl2590/home/Project")



	#Part one question with data type ---- 3 ---- get the top 5 shortest value and top 5 longest value and average value length in TEXT type
	
	TEXT_result = dict()
	for i in range(len(header)):
	
		column_data_for_text = lines_without_header.map(lambda x: (data_with_type(x[i]),1)).filter(lambda x: x[0][1] == 'TEXT' and x[0][0] != "No Data")
		total_count = len(column_data_for_text.collect())

		if test_empty_RDD(column_data_for_text):
			text_data_with_length = column_data_for_text.map(lambda x:(x[0][0],len(x[0][0].strip())))
			#print(text_data_with_length)
			# fot the top 5 value, we need to distinct to get the distinct value
			# otherwise you will get 10 ELEMENT for many times
			#TODO: question is there are some space in data, but also count in --- done
			top_longest_length = text_data_with_length.sortBy(lambda x: x[1], False).distinct().take(5)
			top_shortest_length = text_data_with_length.sortBy(lambda x: x[1], True).distinct().take(5)
			print("the top shortest length data in " + '\t' + str(i) + "column is: " + '\t' + str(top_shortest_length))
			print("the top longest length data in: " + '\t' + str(i) + "column is: " + '\t' + str(top_longest_length))
			#average value length
			#or the same method with int data type, not sure which one is better
			length = text_data_with_length.map(lambda x: (x[1],1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0]*x[1],x[1])).collect()
			length = np.array(length)
			sum_ = 0
			count = 0
			for j in length:
				sum_ += j[0]
				count += j[1]
			if count != 0:	
				print("the average length of text data in:" + '\t' + str(i) + "column is:" + str(sum_/count))
				TEXT_result = {"type":"TEXT", " count" : total_count, "shortest_values": top_shortest_length, "longest_values": top_longest_length, "average_length": sum_/count}
	#TODO: write into json file
				data_infor = write_into_data(data_infor, TEXT_result, i)
	write_into_json(data_infor,"test.json","user/xl2590/home/Project")

	#print(TEXT_result)







sc = SparkContext()
sqlContext = SQLContext(sc)
profile_single_file(sc,"/user/hm74/NYCOpenData/vxxs-iyt2.tsv.gz")



