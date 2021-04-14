from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from io import StringIO
import csv
import sys, time
from os.path import join

filename = "movies"

def query(df):
	df.createOrReplaceTempView(filename)
	sqldf = spark.sql("	SELECT title, year(realeasedate) AS realeseyear \
		FROM movies\
		WHERE (boxoffice, year(realeasedate) ) IN \
		        ( SELECT max(boxoffice), YEAR(realeasedate) \
		            FROM movies \
		            WHERE boxoffice > 0 AND budget >0 AND YEAR(realeasedate) >= 2000 \
		            GROUP BY YEAR(realeasedate) ) \
		ORDER BY realeasedate ASC")

	sqldf.show()


host = "hdfs://master:9000"
case = sys.argv[1]

	
spark = SparkSession.builder.appName("q1_sql").getOrCreate()
sc = spark.sparkContext

#start timer

if case == 'P':
	df = spark.read.format("parquet")
	df = df.load( join(host, filename) + ".parquet" ).toDF('movieid', 'title', 'summary', 'realeasedate', 'duration', 'budget', 'boxoffice', 'popularity')

elif case == 'C':
	df = spark.read.csv( join( host, filename)+".csv", 
		inferSchema='true', header='false').toDF('movieid', 'title', 'summary', 'realeasedate', 'duration', 'budget', 'boxoffice', 'popularity')

#	df = spark.read.csv("hdfs://master:9000/movies.csv", inferSchema=True, header=True)
else:
	print('No such option')
	exit()
	

print( "*"*35)
print( "*"*15+"START"+"*"*15)
print( "*"*35)
# df.printSchema()
start_time = time.time()
query(df)
fin_time = time.time()

msg="Time elapsed for %s using %s is %.4f sec.\n" % (sys.argv[0].split('/')[-1], "csv" if case =='C' else "parquet", fin_time-start_time) 
print(msg)
with open("times-sql.txt", "a") as outfile:
	outfile.write(msg)

print( "*"*35)
print( "*"*16+"END"+"*"*16)
print( "*"*35)

