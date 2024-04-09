import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col,lit,split,from_json,explode
from pyspark.sql.types import StringType, ArrayType
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
array_schema = ArrayType(StringType())
df = spark.read.option("header", True).csv(
    "hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn)
).withColumn("Cuisine",explode(from_json(col('Cuisine Style'),array_schema))).cache()
Result = df.groupBy('City','Cuisine').count()
Result.show()
Result.write.format('csv').save("/assignment2/output/question4/")
