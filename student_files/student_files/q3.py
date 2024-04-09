import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, col,lit
from pyspark import SparkContext, SparkConf
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header", True).csv(
    "hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn)
).withColumn("Rating",col("Rating").cast('int'))
original_count = df.count()
filter_rating = df.filter(col('Rating').isNotNull()).cache()
df.printSchema()
avg = filter_rating.groupBy('City').avg('Rating').withColumnRenamed('avg(Rating)','AverageRating').cache()
lowest = avg.sort(col('AverageRating').asc()).limit(3).withColumn('RatingGroup',lit("Bottom")).cache()
highest = avg.sort(col('AverageRating').desc()).withColumn('RatingGroup',lit("Top")).limit(3).cache()
Result = highest.union(lowest)
Result.show()
Result.write.format('csv').save("/assignment2/output/question3/")