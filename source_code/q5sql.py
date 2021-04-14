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


	# sqldf = spark.sql("WITH t AS ( SELECT count(*) as bodycount, genre, userid FROM  ratings\
	# 	INNER JOIN movie_genres \
	#  	ON ratings.movieid = movie_genres.movieid \
	#  	GROUP BY genre, userid ) \
	# SELECT bodycount, genre, userid FROM t\
	# WHERE bodycount = ( SELECT MAX(bodycount) FROM t) \
 # ")
 	#

	# sqldf = spark.sql("WITH t AS ( SELECT count(*) as bodycount, genre, userid FROM  ratings\
	# 	INNER JOIN movie_genres \
	#  	ON ratings.movieid = movie_genres.movieid \
	#  	GROUP BY genre, userid )\
	# SELECT t.bodycount, t.genre, t.userid FROM t\
	# INNER JOIN (SELECT genre, MAX(bodycount) AS maxbodycount FROM t GROUP BY genre) AS s \
	# ON t.genre = s.genre AND t.bodycount = s.maxbodycount \
 # ")

	# sqldf = spark.sql("WITH t AS (SELECT count(*) as bodycount, genre, userid FROM  ratings\
	# 		INNER JOIN movie_genres \
	#  		ON ratings.movieid = movie_genres.movieid \
	#  		GROUP BY genre, userid ),\
	# 	z AS (SELECT ratings.movieid as movieid, title , genre, rating , userid, popularity FROM ratings \
	# 		INNER JOIN movies \
	# 		ON movies.movieid=ratings.movieid \
	# 		INNER JOIN movie_genres \
	# 		ON movie_genres.movieid=ratings.movieid )\
	# 	SELECT * FROM z WHERE (rating, USE > 1.2  \
	# 		")

	# sqldf = spark.sql( "SELECT rating, userid, genre \
	# 	FROM ratings \
	# 	INNER JOIN (SELECT MAX(rating) as maxr, userid, genre FROM ratings GROUP BY userid, genre) AS q\
	# 	ΟΝ q.userid = ratings.userid AND q.genre = rating.genre AND q.maxr = ratings.rating" )

	# sqldf = spark.sql("WITH t AS (SELECT count(*) as bodycount, genre, userid FROM  ratings\
	# 		INNER JOIN movie_genres \
	#  		ON ratings.movieid = movie_genres.movieid \
	#  		GROUP BY genre, userid )\
	#  		SELECT * FROM t LIMIT 5")

	# sqldf = spark.sql("WITH t AS (SELECT genre, userid, MAX(rating) AS r1, MIN(rating) AS r2\
	# 	FROM ratings\
	# 	INNER JOIN movie_genres\
	# 	ON movie_genres.movieid = ratings.movieid\
	# 	GROUP BY userid, genre), \
	# 	p AS (SELECT genre, userid, ratings.movieid as movieid, rating \
	# 		  FROM ratings\
	# 		  INNER JOIN movie_genres\
	# 		  ON movie_genres.movieid = ratings.movieid )\
	# 	SELECT p.movieid as m1, pp.movieid as m2, r1, r2, t.userid  FROM t \
	# 	LEFT JOIN p\
	# 	ON t.genre = p.genre AND t.userid = p.userid AND t.r1 = p.rating \
	# 	LEFT JOIN p AS pp\
	# 	ON t.genre = pp.genre AND t.userid = pp.userid AND t.r2 = pp.rating \
	# 	LIMIT 5\
	# 	")

	# sqldf = spark.sql("WITH t AS ( SELECT count(*) as bodycount, genre, userid FROM  ratings\
	# 	INNER JOIN movie_genres \
	#  	ON ratings.movieid = movie_genres.movieid \
	#  	GROUP BY genre, userid )\
	# SELECT t.bodycount, t.genre, t.userid FROM t\
	# INNER JOIN (SELECT genre, MAX(bodycount) AS maxbodycount FROM t GROUP BY genre) AS s \
	# ON t.genre = s.genre AND t.bodycount = s.maxbodycount \
 # # ")
	# sqldf = spark.sql("WITH q1 AS (SELECT genre, ratings.userid, rating, title, ratings.movieid\
	# 								FROM  ratings\
	# 								INNER JOIN movie_genres\
	# 								ON movie_genres.movieid = ratings.movieid\
	# 								INNER JOIN movies \
	# 								ON movies.movieid \
	# 								WHERE ratings.userid = (SELECT MAX() ) \
	# 	)")


	sqldf = spark.sql("WITH t AS ( SELECT count(*) as bodycount, genre, userid FROM  ratings\
									INNER JOIN movie_genres \
								 	ON ratings.movieid = movie_genres.movieid \
						 			GROUP BY genre, userid \
						 		 )\
						 	,intusers AS (SELECT t.bodycount AS num_ratings, t.genre, t.userid FROM t\
									INNER JOIN (SELECT genre, MAX(bodycount) AS maxbodycount FROM t GROUP BY genre) AS s \
									ON t.genre = s.genre AND t.bodycount = s.maxbodycount\
								)\
							,intratings AS ( SELECT intusers.userid AS userid, intusers.genre AS genre, rating, movieid, num_ratings \
											FROM (SELECT rating, ratings.userid AS userid, movie_genres.genre AS genre, ratings.movieid AS movieid \
													FROM movie_genres \
													INNER JOIN  ratings \
													ON ratings.movieid=movie_genres.movieid ) AS rg\
											INNER JOIN intusers\
											ON intusers.userid = rg.userid AND intusers.genre = rg.genre \
								)\
							,maxintratings AS( SELECT popularity, title, intratings.userid AS userid, intratings.rating AS rating, movies.movieid AS movieid, intratings.genre AS genre FROM intratings\
												INNER JOIN ( SELECT MAX(rating) as rating, userid, genre FROM intratings GROUP BY userid, genre ) as r\
												ON intratings.userid =  r.userid AND intratings.rating=r.rating AND intratings.genre = r.genre\
												INNER JOIN movies\
												ON movies.movieid=intratings.movieid ) \
							,minintratings AS( SELECT popularity, title, intratings.userid AS userid, intratings.rating AS rating, movies.movieid AS movieid, intratings.genre AS genre FROM intratings\
												INNER JOIN ( SELECT MIN(rating) as rating, userid, genre FROM intratings GROUP BY userid, genre ) as r\
												ON intratings.userid =  r.userid AND intratings.rating=r.rating AND intratings.genre = r.genre\
												INNER JOIN movies\
												ON movies.movieid=intratings.movieid )\
												\
						SELECT q1.genre, q1.userid as userid, num_ratings, q1.title1, q1.rating1, q2.title2, q2.rating2 \
						FROM ( SELECT maxintratings.popularity as pop, title as title1, maxintratings.rating as rating1, maxintratings.userid as userid, maxintratings.genre AS genre FROM maxintratings \
							INNER JOIN ( SELECT MAX(popularity) AS pop, userid, genre FROM maxintratings GROUP BY userid, genre ) as m1 \
							ON m1.pop = maxintratings.popularity AND m1.userid = maxintratings.userid AND m1.genre = maxintratings.genre ) AS q1\
						INNER JOIN ( SELECT minintratings.popularity as pop, title as title2, minintratings.rating as rating2, minintratings.userid as userid, minintratings.genre AS genre FROM minintratings \
							INNER JOIN ( SELECT MAX(popularity) AS pop, userid, genre FROM minintratings GROUP BY userid, genre ) as m2 \
							ON m2.pop = minintratings.popularity AND m2.userid = minintratings.userid AND m2.genre = minintratings.genre ) AS q2 \
						ON q1.userid = q2.userid AND q1.genre = q2.genre \
						INNER JOIN (SELECT DISTINCT userid, num_ratings, genre FROM intratings) as lastpanayamu\
						ON (lastpanayamu.userid = q1.userid) AND (lastpanayamu.genre = q1.genre) AND (lastpanayamu.genre= q2.genre) AND (lastpanayamu.userid = q2.userid) \
						ORDER BY q1.genre\
			")
	# sqldf = spark.sql("WITH t AS ( SELECT count(*) as bodycount, genre, userid FROM  ratings\
	# 								INNER JOIN movie_genres \
	# 							 	ON ratings.movieid = movie_genres.movieid \
	# 					 			GROUP BY genre, userid \
	# 					 		 )\
	# 					 	,intusers AS (SELECT t.bodycount AS num_ratings, t.genre, t.userid FROM t\
	# 								INNER JOIN (SELECT genre, MAX(bodycount) AS maxbodycount FROM t GROUP BY genre) AS s \
	# 								ON t.genre = s.genre AND t.bodycount = s.maxbodycount\
	# 							)\
	# 						,intratings AS ( SELECT intusers.userid AS userid, intusers.genre AS genre, rating, movieid, num_ratings \
	# 										FROM (SELECT rating, ratings.userid AS userid, movie_genres.genre AS genre, ratings.movieid AS movieid \
	# 												FROM movie_genres \
	# 												INNER JOIN  ratings \
	# 												ON ratings.movieid=movie_genres.movieid ) AS rg\
	# 										INNER JOIN intusers\
	# 										ON intusers.userid = rg.userid AND intusers.genre = rg.genre \
	# 							)\
	# 						,maxintratings AS( SELECT popularity, title, intratings.userid AS userid, intratings.rating AS rating, movies.movieid AS movieid, intratings.genre AS genre FROM intratings\
	# 											INNER JOIN ( SELECT MAX(rating) as rating, userid, genre FROM intratings GROUP BY userid, genre ) as r\
	# 											ON intratings.userid =  r.userid AND intratings.rating=r.rating AND intratings.genre = r.genre\
	# 											INNER JOIN movies\
	# 											ON movies.movieid=intratings.movieid ) \
	# 						,minintratings AS( SELECT popularity, title, intratings.userid AS userid, intratings.rating AS rating, movies.movieid AS movieid, intratings.genre AS genre FROM intratings\
	# 											INNER JOIN ( SELECT MIN(rating) as rating, userid, genre FROM intratings GROUP BY userid, genre ) as r\
	# 											ON intratings.userid =  r.userid AND intratings.rating=r.rating AND intratings.genre = r.genre\
	# 											INNER JOIN movies\
	# 											ON movies.movieid=intratings.movieid )\
	# 					SELECT * FROM intusers\
	# 				")
	sqldf.show(30)

host = "hdfs://master:9000"
case = sys.argv[1]

	
spark = SparkSession.builder.appName("q5_sql").getOrCreate()
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
