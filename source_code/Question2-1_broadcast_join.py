from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from io import StringIO
import csv, time
'''
   ratings.csv     |  movie_genres.csv
 0 - user_id       | 0 - movie_id
 1 - movie_id      | 1 - genre
 2 - rating        |
 3 - timestamp     |
'''
#-----------------------------------------------------------#
# We perform a test broadcast join on movies_genres,ratings #
#-----------------------------------------------------------#
# smallRDD -> movie_genres
# bigRDD   -> ratings

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

spark = SparkSession.builder.appName("bcast_join").getOrCreate()
sc = spark.sparkContext
start = time.time()

# (movie_id, genre) 
genres  = sc.textFile("hdfs://master:9000/movie_genres-short.csv")\
            .map(lambda x: (split_complex(x)[0], split_complex(x)[1]))

# (movie_id,   rating) 
ratings  = sc.textFile("hdfs://master:9000/ratings.csv")\
             .map(lambda x: (split_complex(x)[1],  split_complex(x)[2]))


# Collect,groupByKey(for multiple equal keys)
# and broadcast smallRDD
genresDict = genres\
              .groupByKey()\
              .map(lambda x: (x[0],(x[1])))\
              .collectAsMap()
genresBroad = sc.broadcast(genresDict)
  
def func(partition):
  for i in partition: # partition is lines of ratings, i is a line
    if(i[0] in genresBroad.value): # i = (movie_id, ratings), i[0]=key
      values = genresBroad.value[i[0]] # (movie_id,(genres))
      for j in values:
              temp=[]
              temp.append(i[1])      # i[1] = rating
              temp.append(j)         # j = one genre
              it = []
              it.append(i[0])        # movie_id
              it.append(temp)        # movie_id,(rating,genre)
              yield it

result = ratings.mapPartitions(func).collect()

print("Genres-short + Ratings " )
for i in result:
    print(i)
print("BroadCast Join of Genres,Ratings.Execution time: %s seconds" % (time.time() - start)) 

