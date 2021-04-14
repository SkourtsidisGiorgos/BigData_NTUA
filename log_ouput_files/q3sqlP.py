Script started on Sun 21 Mar 2021 09:55:22 PM EET
]0;user@master: ~/ergasia/meros1user@master:~/ergasia/meros1$ exitspark-submit q2sql.py P[1P[1@3
21/03/21 21:55:29 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
***********************************
***************START***************
***********************************
root
 |-- movieid: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- summary: string (nullable = true)
 |-- realeasedate: timestamp (nullable = true)
 |-- duration: double (nullable = true)
 |-- budget: integer (nullable = true)
 |-- boxoffice: long (nullable = true)
 |-- popularity: double (nullable = true)

root
 |-- userid: integer (nullable = true)
 |-- movieid: integer (nullable = true)
 |-- rating: double (nullable = true)
 |-- time: integer (nullable = true)

+---------------+------------------+--------+
|          genre|       avg(rating)|count(1)|
+---------------+------------------+--------+
|          Crime|3.5724008455276626| 1968948|
|        Romance|3.5504344768936207| 2104830|
|       TV Movie| 3.595226232157285|   74260|
|       Thriller|3.5629230581049898| 2894106|
|      Adventure| 3.503404281631307| 1601072|
|        Foreign|3.4957520992341258|  151722|
|          Drama| 3.535949153524536| 6592158|
|            War|3.5018081990913696|  242783|
|    Documentary| 3.485778225572068|  309666|
|         Family|3.3964312687197538|  581271|
|        Fantasy|3.5071166295530922|  860871|
|        History|3.4262988516956385|  451187|
|        Mystery| 3.653242827089667| 1170306|
|      Animation|3.5973733575788422|  243619|
|          Music|3.4411217105181717|  403901|
|Science Fiction| 3.525669760212952| 1388482|
|         Horror|3.4853683175590993|  925560|
|        Western| 3.658921396573223|  259836|
|         Comedy|3.5369001945980565| 3247720|
|         Action| 3.558276709938397| 2269380|
+---------------+------------------+--------+

Time elapsed for q3sql.py using parquet is 11.0631 sec.

***********************************
****************END****************
***********************************
]0;user@master: ~/ergasia/meros1user@master:~/ergasia/meros1$ spark-submit q3sql.py P[Kexit
exit

Script done on Sun 21 Mar 2021 09:56:24 PM EET
