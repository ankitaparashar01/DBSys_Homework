import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col,lit,split,from_json,explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,ArrayType
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
schema = ArrayType(StructType([
    StructField("cast_id", IntegerType(), True),
    StructField("character", StringType(), True),
    StructField("credit_id", StringType(), True),
    StructField("gender", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("order", IntegerType(), True)
]))
df = spark.read.option("header",True)\
.parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn)).withColumn("cast",from_json(col('cast'),schema)).cache()
df.printSchema()

df_exploded = df.select("movie_id", "title", explode("cast").alias("cast_info"))
df_actors = df_exploded.select(
    "movie_id",
    "title",
    df_exploded.cast_info['name'].name('actor')
)

df_actors.show()

df1 = df_actors.alias('a1')
df2 = df_actors.alias('a2')
df2.printSchema()
# self join actor one and actor 2 if they are different (unique pair)
df_pairs = df_actors.alias("a1").join(df_actors.alias("a2"),
    (col("a1.movie_id") == col("a2.movie_id")) & (col("a1.actor") < col("a2.actor"))
).select(
    col("a1.movie_id"),
    col("a1.title"),
    col("a1.actor").alias("actor1"),
    col("a2.actor").alias("actor2")
)

grouped_actors = df_pairs.groupBy('actor1','actor2').count()
filtered_actors = grouped_actors.filter(grouped_actors['count']>=2)

filtered_actors.alias('a').join(df_pairs.alias('b'),\
                  (col('a.actor1')==col('b.actor1'))&\
                   (col('a.actor2')==col('b.actor2'))
                   ).select(
                       col('movie_id'),
                       col('title'),
                       col('a.actor1'),
                       col('a.actor2')
                   ).show()

filtered_actors.write.format('csv').save("/assignment2/output/question5/")