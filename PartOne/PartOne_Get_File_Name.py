import subprocess
import re

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
