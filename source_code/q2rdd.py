from pyspark import  SparkContext
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import csv
import time

spark = SparkSession.builder.appName("q2_rdd").getOrCreate()
sc = spark.sparkContext

def split_complex(x):
     return list(csv.reader(StringIO(x), delimiter=','))[0]

def getData(arg):
    line = split_complex(arg)
    user_id = line[0]
    rating = float(line[2])
    return (user_id, (rating,1))

# Q2 using Map-Reduce
start_time_mapreduce = time.time()

# out -> (user_id, mean_rating)
mean_rating = sc.textFile("hdfs://master:9000/ratings.csv")\
            .map(lambda x: getData(x))\
			.reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1]))\
			.map(lambda x: (x[0], x[1][0]/x[1][1]))\
			.collect()

count3 = 0
count_all = 0
for i in mean_rating:
    count_all+=1
    if(i[1]>3):
        count3+=1

print("Number of users with mean ratings > 3 :",count3/count_all)
print("Time of Q1 using Map-Reduce with csv is: %s seconds" % (time.time() - start_time_mapreduce)) 

