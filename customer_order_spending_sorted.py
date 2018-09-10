
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession 
from os import getcwd

conf = SparkConf().setMaster("local").setAppName("First")

conf.set('spark.logConf', 'true')  

sc = SparkContext(conf=conf)

# displaying logs only when error is generated in the code.
sc.setLogLevel("ERROR")

# Reading the csv file and making an RDD with name rows.
rows = sc.textFile(r'{}/customer-orders.csv'.format(getcwd()))

# Making an array of the string.
rows = rows.map(lambda x:x.split(','))

# Making key-value pair of the formed array.
rows = rows.map(lambda x:(x[0], (x[1], x[2])))

# Removing the duplicates key
rows = rows.reduceByKey(lambda x, y:x[1] + y[1])

# swapping keys and values, so that transformation can be applied directly.
rows = rows.map(lambda x : (x[1], x[0]))

