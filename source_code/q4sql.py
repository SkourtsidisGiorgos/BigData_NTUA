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

	sqldf = spark.sql("SELECT ROUND(AVG( LENGTH(summary) ), 2) AS avg_len_sum , \
		CASE \
		    WHEN YEAR(realeasedate) >= 2000 AND YEAR(realeasedate)<2005 THEN '2000-2004'\
		    WHEN YEAR(realeasedate) >= 2005 AND YEAR(realeasedate)<2010 THEN '2005-2009'\
		    WHEN YEAR(realeasedate) >= 2010 AND YEAR(realeasedate)<2015 THEN '2010-2014'\
		    WHEN YEAR(realeasedate) >= 2015 AND YEAR(realeasedate)<2019 THEN '2015-2019'\
		END AS quinquennial\
		FROM ( SELECT summary, realeasedate \
		       FROM ratings \
		       INNER JOIN movies \
		       ON movies.movieid = ratings.movieid\
		       INNER JOIN movie_genres \
		       ON movie_genres.movieid = ratings.movieid\
		       WHERE YEAR(realeasedate) >= 2000 AND YEAR(realeasedate) < 2020 AND genre = 'Drama' )\
		GROUP BY quinquennial\
		ORDER BY quinquennial\
		")
	sqldf.show()

host = "hdfs://master:9000"
case = sys.argv[1]

	
spark = SparkSession.builder.appName("q4_sql").getOrCreate()
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
# df1.printSchema()
# df2.printSchema()

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