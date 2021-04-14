from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from io import StringIO
import csv, time
from operator import gt, lt
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

def max_count(a,b):
        if a>b: return a
        return b
    				
spark = SparkSession.builder.appName("q5_rdd").getOrCreate()
sc = spark.sparkContext

start = time.time()

def compare(a,b,sign):
    if(sign(float(a[0][0]),float(b[0][0]))):
        return a
    elif(float(a[0][0])==float(b[0][0])):
        if(float(a[0][1]) > float(b[0][1])):
            return a
    return b

# (movie_id, genre)
genres = sc.textFile("hdfs://master:9000/movie_genres.csv").\
         map(lambda x : (split_complex(x)[0], split_complex(x)[1]))
# (movie_id, (user_id, 1))
ratings = sc.textFile("hdfs://master:9000/ratings.csv")\
            .map(lambda x: (split_complex(x)[1], (split_complex(x)[0], 1)))\

# (movie_id, (rating, user_id))
ratings2 = sc.textFile("hdfs://master:9000/ratings.csv")\
                .map(lambda x : ( split_complex(x)[1], (split_complex(x)[2], split_complex(x)[0])))

# (movie_id, (title, popularity))
movies = sc.textFile("hdfs://master:9000/movies.csv")\
         .map(lambda x : (split_complex(x)[0], (split_complex(x)[1], split_complex(x)[7]) ))

# input: (movie_id, (genre, (user_id, 1))) -> 
# map1:  ((genre, user_id), count) 
# map2:  (genre,(count,user_id))
# map3:  ((user_id,genre), max_count)
result1 = genres.join(ratings)\
                .map(lambda x :( (x[1][0], x[1][1][0]),x[1][1][1]))\
                .reduceByKey(lambda x,y: x + y )\
                .map(lambda x: (x[0][0], (x[1], x[0][1])))\
                .reduceByKey(lambda x,y: max_count(x,y))\
                .map(lambda x: ((x[1][1], x[0]), x[1][0]) )

# (movie_id,((title,popularity),genre))
result2 = movies.join(genres)

# join: (movie_id, (((title,popularity),genre), (rating, user_id)))
#          0        1000      1001       101     110       111
# map:  ((user_id,genre), ((rating,popularity),(movie_id,title)))
result3 = result2.join(ratings2)\
           .map(lambda x: ((x[1][1][1], x[1][0][1]), ((x[1][1][0], x[1][0][0][1]),(x[0], x[1][0][0][0] ))))

best_mov = result3.reduceByKey(lambda x,y: compare(x, y, gt))
worst_mov = result3.reduceByKey(lambda x,y: compare(x, y, lt))

# ((user_id,genre), (max_count, ( (best_rating,best_popularity),(best_movie_id,best_title)))
#   00       01         10          1100    1101        1110    1111    
result4 = result1.join(best_mov)

# join: ((user_id,genre), ((max_count, ( (b_rat, b_pop),(b_mov_id,b_title)), ((w_rat,w_pop),(w_mov_id,w_title))))
# map : (Genre , (User Id , Ratings , Favorite Movie , Rating , Least Favorite , Rating))
final_result = result4.join(worst_mov)\
                      .map(lambda x: (x[0][1], (x[0][0], x[1][0][0],x[1][0][1][1][1], x[1][0][1][0][0], x[1][1][1][1], x[1][1][0][0])))\
	                  .sortByKey(lambda x: x[0])\
                      .collect()

print("Genre | User Id | Ratings | Favorite Movie | Rating | Least Favorite | Rating")
for i in final_result:
    print(i)
print("Q5_RDD Execution time: %s seconds" % (time.time() - start)) 
