import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, col
from pyspark import SparkContext, SparkConf
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

conf = SparkConf().setAppName("Assignment2 Question 1")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
# YOUR CODE GOES BELOW

df = spark.read.option("header", True).csv(
    "hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn)
)
df.printSchema()
original_count = df.count()
rating_filter = df.filter(df.Rating > 1)
full_filter = rating_filter.filter(col("Number of Reviews").isNotNull())
full_filter.show()
full_filtered_count = full_filter.count()


full_filter.write.format("csv").save("/assignment2/output/question1/")
