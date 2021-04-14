from pyspark.sql import SparkSession
from io import StringIO
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys,time,csv
from pyspark import SparkContext
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
start = time.time()

def split_complex(x):
     return list(csv.reader(StringIO(x), delimiter=','))[0]

spark = SparkSession.builder.appName("Q3_RDD").getOrCreate()
sc = spark.sparkContext
# 1) map -> (id, (rating, 1))
# 2) reduce -> (id, total_rating, count)
# 3) map -> (id, total_rating / count)
ratings = sc.textFile("hdfs://master:9000/ratings.csv").\
        map(lambda x: (split_complex(x)[1], (float(split_complex(x)[2]), 1))).\
        reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])). \
        map(lambda x: (x[0], x[1][0]/x[1][1]))

# map ->(id,genre)
genres = sc.textFile("hdfs://master:9000/movie_genres.csv"). \
         map(lambda x : (split_complex(x)[0], split_complex(x)[1]))

# (id,(genre, mean_rating))
res = genres.join(ratings). \
        map(lambda x : (x[1][0],( x[1][1],  1))). \
        reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])). \
        map(lambda x: (x[0],(x[1][0]/x[1][1], x[1][1]))).\
        collect()


for i in res:
        print(i)

print("Q3_RDD execution time: %s seconds" % (time.time() - start))
