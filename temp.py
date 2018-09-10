
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("First")

conf.set('spark.logConf', 'true')  

sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")

rows = sc.parallelize([1,2,3,4])

print(rows.take(rows.count()))

