import os
import json

path = "PartOne/part1Results/"
realCount = 0
intCount = 0
textCount = 0
dateTimeCount = 0
totalColumnCount = 0
totalFileCount = 0

results = os.listdir(path)
for result in results:
    totalFileCount += 1
    with open(path+result, 'r') as load_f:
            load_dict = json.load(load_f)
            for column in load_dict['columns']:
                totalColumnCount += 1
                dataType= column['data_type']
                for value in dataType:
                    if value['type'] == "INTEGER (LONG)":
                        intCount += 1
                    if value['type'] == "REAL":
                        realCount += 1
                    if value['type'] == "TEXT":
                        textCount += 1
                    if value['type'] == "DATE/TIME":
                        dateTimeCount += 1

print("Total number of files we processed: ", totalFileCount)
print("Total number of columns we have: ", totalColumnCount)
print("Columns contains REAL count: ", realCount)
print("Columns contains INTEGER(LONG) count: ", intCount)
print("Columns contains DATE/TIME Count: ", dateTimeCount)
print("Columns contains TEXT Count: ", textCount)

