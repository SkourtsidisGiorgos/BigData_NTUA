from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from io import StringIO
import csv
import time

'''
movies.csv                      |   ratings.csv     |  movie_genres.csv
0 - movie id                    | 0 - user_id       | 0 - movie_id
1 - title                       | 1 - movie_id      | 1 - genre
2 - summary                     | 2 - rating        |
3 - Ημερομηνία κυκλοφορίας      | 3 - timestamp     |
4 - duration (min)              |
5 - Κοστος παραγωγής            |
6 - profit                      |
7 - popularity                  |
'''
def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

def checkYear(year) :
        if year == '':
                return float("-inf")
        return int(year)

def max(a,b):
        if a>b: return a
        return b

def getData(arg):
    line = split_complex(arg)
    year = checkYear(split_complex(arg)[3][0:4])
    cost = float(line[5])
    turnover = float(line[6])
    title = line[1]
    if(cost!=0 and turnover!=0):
        profit = 100 * ((turnover - cost) / cost) 
    else:
        profit = float("-Inf")
    return (year, title, profit)
    				
spark = SparkSession.builder.appName("q1_rdd").getOrCreate()
sc = spark.sparkContext

# Q1 using Map-Reduce
start_time_mapreduce = time.time()

result = sc.textFile("hdfs://master:9000/movies.csv") \
    .map(lambda x : ( getData(x) ))\
    .filter(lambda x: (x[0]>1999 and x[2]>0))\
    .map(lambda x: (x[0],(x[2],x[1])))\
    .reduceByKey(lambda x,y: max(x,y))\
    .sortByKey(ascending=True)\
    .collect()

    
print("Year    |   Profit   |  Title ")
for i in result:
    print(i)
print("Time of Q1 using Map-Reduce with csv is: %s seconds" % (time.time() - start_time_mapreduce)) 
