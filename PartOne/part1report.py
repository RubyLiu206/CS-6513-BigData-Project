import os
import json

path = "PartOne/part1Results/"
realCount = 0
intCount = 0
textCount = 0
dateTimeCount = 0
totalColumnCount = 0
totalFileCount = 0
freqSet = {}
resultDictionary = []

results = os.listdir(path)
for result in results:
    totalFileCount += 1
    with open(path+result, 'r') as load_f:
        load_dict = json.load(load_f)
        resultDictionary.append(load_dict)
        for column in load_dict['columns']:
            totalColumnCount += 1
            dataType = column['data_type']
            key = ""
            keyLen = 0
            for value in dataType:
                if value['type'] == "INTEGER (LONG)":
                    key += "Integer(Long)_"
                    keyLen += 1
                    intCount += 1
                elif value['type'] == "REAL":
                    key += "REAL_"
                    keyLen += 1
                    realCount += 1
                elif value['type'] == "TEXT":
                    key += "TEXT_"
                    keyLen += 1
                    textCount += 1
                else:
                    key += "DATE/TIME_"
                    keyLen += 1
                    dateTimeCount += 1
            if keyLen > 1:
                freqSet[key] = freqSet.get(key, 0) + 1

print(freqSet)
print("Total number of files we processed: ", totalFileCount)
print("Total number of columns we have: ", totalColumnCount)
print("Columns contains REAL count: ", realCount)
print("Columns contains INTEGER(LONG) count: ", intCount)
print("Columns contains DATE/TIME Count: ", dateTimeCount)
print("Columns contains TEXT Count: ", textCount)

with open('PartOne/task1.json', 'w') as fp:
        json.dump(resultDictionary, fp)

with open('PartOne/task1.json', 'r') as fp:
        test = json.load(fp)
        with open('PartOne/task1test.json', 'w') as fp:
            json.dump(test[0], fp)

'''
    For task1, we used three python scripts to finish the task: One main script
    for profiling and two helper scripts. checkRemain.py reads all the result files
    we have so far and compares it with file names in dataset.tsv, which holds all the file names
    we need to process. The script will return a file called "secondRunFiles.txt",
    which can be used for the main script. "part1report.py" reads in all the result
    we have and does the evaluation for us. It will count for each data type, how many 
    columns contain that type and applying frequent itemsets. Therefore we can 
    have all the information we need for the evaluation part and visualize it.
    The main script "PartOne.py" will first get all the file that need to process,
    either by reading "dataset.tsv" under "/user/hm74/NYCOpenData/" or by
    "secondRunFile.txt" generated by the helper script. We used RDD as our structure
    since it is fast with tranformation operations and can save space by collecting
    the result at the end of the tranformation. 
    The main funtion, profile_single_file, first reads in the dataset using csv
    reader with the quotechar set to avoid unwanted split. Then it seperates the 
    header and data section. Then it has a loop to profile each column, count
    number of empty values, non-empty values ,distinct values, and top 5 frequent
    values by mapping each tuple and reduce them by key. And it classifies the 
    data into four given categories by using regular expression. Since a column
    may have all four types of data, it "splits" the original rdd to 4 by filtering.
    Then for each type of data, it calculates required attributes and sorts them
    either by built-in sorting method or custom sorting function(for DATE/TIME).
    Since we need to output the result into a json format, a dictionary structure
    is the most appropriate one. It finally writes all the calculated result to a dictionary
    and then writes it to the outoput json file. 

    One chanllege is that some files are extremely large, so in order to improve
    the performance, our script only calls collect() or take() when appending the
    result to the dictionary, everything else are transformative operation, which
    could save a lot of space. Despite our effort, some files still take hours
    to complete due to their size and the load of dumbo. On average, processing 1842 
    files takes around 90 hours, which means 3 minutes per file. Consider the heavy load
    on dumbo, we think it is a reasonable number.
    
    In addition, since we don't know how each data looks like, the script will raise exceptions 
    when processing those files, which significantly slows our process Therefore, our script has 
    a list that contains all file names that cause exception. The "problemFiles.txt" can be our 
    next "secondRunFiles.txt" after we modified our code to solve the problem.


'''
