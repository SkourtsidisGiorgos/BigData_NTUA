from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from io import StringIO
import csv, time
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

spark = SparkSession.builder.appName("Q4_RDD").getOrCreate()
sc = spark.sparkContext

def check(year) :
        if year == '':
                return float("-inf")
        return int(year)


def map1(arg):
    line  = split_complex(arg)
    movie_id = line[0]
    year = check(split_complex(arg)[3][0:4])
    if  year>=2000 and year<=2004: year = "2000-2004"
    elif year>=2005 and year<=2009: year = "2005-2009"
    elif year>=2010 and year<=2014: year = "2010-2014"
    elif year>=2015 and year<=2019: year = "2015-2019"
    else: year = None
    words = line[2].split(' ')
    count=0
    for _ in words:
        count+=1
    return (movie_id,(year, (count, 1)))

start = time.time()

movies = sc.textFile("hdfs://master:9000/movies.csv"). \
            map(lambda x: (map1(x))). \
            filter(lambda x: x[1][0]!=None)

genres = sc.textFile("hdfs://master:9000/movie_genres.csv"). \
         map(lambda x : (split_complex(x)[0], split_complex(x)[1]))

result = genres.join(movies). \
        filter(lambda x : (x[1][0] == "Drama")). \
        map(lambda x : (x[1][1][0], x[1][1][1])). \
        reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])). \
        map(lambda x: (x[0], x[1][0]/x[1][1])). \
        sortByKey(True).\
        collect()

print("Year    |   Average Summary Length ")
for i in result:
    print(i)
print("Q1_RDD Execution time: %s seconds" % (time.time() - start)) 

