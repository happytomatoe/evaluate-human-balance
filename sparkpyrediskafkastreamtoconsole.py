from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

v = StructType([
    StructField("element", StringType()),
    StructField("score", DecimalType(10, 1)),

])

schema = StructType([
    StructField("key", StringType()),
    StructField("value", StringType()),
    StructField("expiredType", StringType()),
    StructField("expiredValue", StringType()),
    StructField("existType", StringType()),
    StructField("ch", StringType()),
    StructField("incr", StringType()),
    StructField("zSetEntries", ArrayType(v)),
])
df = df.select(df.key.cast("string").alias("key"), df.value.cast("string").alias("value"))

# df=df.selectExpr("root2.*")
df.createOrReplaceTempView("RedisSortedSet")

df = df.select(from_json(df.value, schema).alias("value"))
df = df.selectExpr("value.*")
# df=df.select(unbase64(df.key).cast("string").alias("key"),df.value[0].alias("s"))

# df=df.select("s.*")
# df=df.withColumn("element",unbase64(df.element).cast("string"))

customer_schema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType()),

])
elSchema = StructType([
    StructField("customer", customer_schema)
])

df = df.withColumn("key", unbase64(df.key).cast("string"))
df = df.filter(df.key == 'RapidStepTest')

df2 = df.withColumn("element", unbase64(df.zSetEntries[0].element).cast("string"))
df2 = df2.withColumn("element", from_json(df2.element, elSchema))
df2 = df2.selectExpr("element.customer.*")

df2.createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF = df2.selectExpr("email as email", "split(birthDay,'-')[0] as birthYear")
emailAndBirthDayStreamingDF.createOrReplaceTempView("q")

q = spark.sql(f"SELECT * FROM q") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("numRows", "1000") \
    .start() \
    .awaitTermination()