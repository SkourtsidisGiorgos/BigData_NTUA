from pyspark.sql import SparkSession
import csv
from io import StringIO

import time
'''
   ratings.csv     |  movie_genres.csv
 0 - user_id       | 0 - movie_id
 1 - movie_id      | 1 - genre
 2 - rating        |
 3 - timestamp     |
'''
#-------------------------------------------------------------#
# We perform a test repartition join on movies_genres,ratings #
#-------------------------------------------------------------#

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]


spark=SparkSession.builder.appName("rep_join").getOrCreate()
sc=spark.sparkContext
start = time.time()

def combines(key, list_of_values):
        length = int(len(list_of_values))
        b_r = []
        b_l = []
        for i in range(length):
                elem = list_of_values[i]
                if(elem[0] == 'R'):
                        b_r.append(elem[1:])
                elif(elem[0] == 'L'):
                        b_l.append(elem[1:])
        combined=[]
        for r in b_r:
                for l in b_l:
                        combined.append((key,(r+l)))
        return combined

# reads R input file, creates (movie_id, ('R', genre)) pairs (tagged with 'R')
R = \
        sc.textFile("hdfs://master:9000/data/movie_genres-short.csv"). \
        map(lambda row: split_complex(row)). \
        map(lambda x : (x[0],[('R', x[1])] ) )

# reads L input file, creates (movie_id, ('L', rating )) pairs (tagged with 'L')
L = \
        sc.textFile("hdfs://master:9000/data/ratings.csv"). \
        map(lambda row: split_complex(row)). \
        map(lambda x: (x[1],[('L', x[2] )]))

# unions both datasets, reduces by key to (key, list of values) pairs, uses "combines" function on them,
# adding each returned result to one list as join operation would.
res = L.union(R). \
        reduceByKey(lambda x,y: x + y ). \
        flatMap(lambda x: combines(x[0], x[1]))

print("Genres-short + Ratings " )
print(res.collect())
print("Repartition Join of Genres,Ratings. Execution time: %s seconds" % (time.time() - start)) 
