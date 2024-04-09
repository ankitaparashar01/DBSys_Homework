import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, col
from pyspark import SparkContext, SparkConf
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header", True).csv(
    "hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn)
)
df.printSchema()
original_count = df.count()
non_null_price = df.filter(col('Rating').isNotNull()).filter(col("Price Range").isNotNull()).cache()
rdd = non_null_price.rdd
non_null_price.show()

city_to_rating = rdd.map(lambda x:((x[2],x[6]),x)).cache()
worst_city_restruants = city_to_rating.reduceByKey(lambda a,b:a if a[5]<b[5]else b).cache()
best_city_restruants = city_to_rating.reduceByKey(lambda a,b:a if a[5]>b[5]else b).cache()

# combine the best and worst in a single table
all_selected_restruants = worst_city_restruants.union(best_city_restruants).map(lambda x:x[1])
all_selected_restruants.toDF().write.format('csv').save("/assignment2/output/question2/")