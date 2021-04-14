from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from io import StringIO
import csv
import sys, time
from os.path import join

table1 = "movies"
table2 = "ratings"
table3 = "movie_genres"

def query(df1, df2, df3):
	df1.createOrReplaceTempView(table1)
	df2.createOrReplaceTempView(table2)
	df3.createOrReplaceTempView(table3)

	sqldf = spark.sql("SELECT genre, AVG(rating), COUNT(*) FROM (SELECT genre, ratings.movieid, ratings.rating as rating \
		FROM ratings\
		INNER JOIN movie_genres\
		ON movie_genres.movieid = ratings.movieid )\
		GROUP BY genre")
	sqldf.show()


host = "hdfs://master:9000"
case = sys.argv[1]

	
spark = SparkSession.builder.appName("q3_sql").getOrCreate()
sc = spark.sparkContext

#start timer

if case == 'P':
	df1 = spark.read.format("parquet")
	df1 = df1.load( join(host, table1) + ".parquet" ).toDF('movieid', 'title', 'summary', 'realeasedate', 'duration', 'budget', 'boxoffice', 'popularity')
	df2 = spark.read.format("parquet")
	df2 = df2.load( join(host, table2) + ".parquet" ).toDF('userid', 'movieid', 'rating', 'time' )
	df3 = spark.read.format("parquet")
	df3 = df3.load( join(host, table3) + ".parquet" ).toDF('movieid', 'genre' )
elif case == 'C':
	df1 = spark.read.csv( join( host, table1)+".csv", 
		inferSchema='true', header='false').toDF('movieid', 'title', 'summary', 'realeasedate', 'duration', 'budget', 'boxoffice', 'popularity')
	df2 = spark.read.csv( join( host, table2)+".csv", 
		inferSchema='true', header='false').toDF('userid', 'movieid', 'rating', 'time' )
	df3 = spark.read.csv( join( host, table3)+".csv", 
		inferSchema='true', header='false').toDF('movieid', 'genre' )
#	df = spark.read.csv("hdfs://master:9000/movies.csv", inferSchema=True, header=True)
else:
	print('No such option')
	exit()
	

print( "*"*35)
print( "*"*15+"START"+"*"*15)
print( "*"*35)
df1.printSchema()
df2.printSchema()

start_time = time.time()
query(df1, df2, df3)
fin_time = time.time()

msg="Time elapsed for %s using %s is %.4f sec.\n" % (sys.argv[0].split('/')[-1], "csv" if case =='C' else "parquet", fin_time-start_time) 
print(msg)
with open("times-sql.txt", "a") as outfile:
	outfile.write(msg)
	
print( "*"*35)
print( "*"*16+"END"+"*"*16)
print( "*"*35)