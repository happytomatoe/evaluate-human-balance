from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, unbase64
from pyspark.sql.types import *

KAFKA_URL = "localhost:9092"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

redisRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_URL) \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

zSenEntrySchema = StructType([StructField("element", StringType()), StructField("score", DecimalType(10, 1)), ])
schema = StructType([
    StructField("key", StringType()),
    StructField("value", StringType()),
    StructField("expiredType", StringType()),
    StructField("expiredValue", StringType()),
    StructField("existType", StringType()),
    StructField("ch", StringType()),
    StructField("incr", StringType()),
    StructField("zSetEntries", ArrayType(zSenEntrySchema)),
])
redisRawStreamingDF = redisRawStreamingDF.select(redisRawStreamingDF.key.cast("string").alias("key"),
                                                 redisRawStreamingDF.value.cast("string").alias("value"))

redisRawStreamingDF.createOrReplaceTempView("RedisSortedSet")

customerEventsStreamingDF = redisRawStreamingDF.select(from_json(redisRawStreamingDF.value, schema).alias("value"))
customerEventsStreamingDF = customerEventsStreamingDF.selectExpr("value.*")

customer_schema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType()),

])
elementSchema = StructType([
    StructField("customer", customer_schema)
])

customerEventsStreamingDF = customerEventsStreamingDF.withColumn("key", unbase64(
    customerEventsStreamingDF.key).cast("string"))
customerEventsStreamingDF = customerEventsStreamingDF.filter(
    customerEventsStreamingDF.key == 'RapidStepTest')

customerEventsStreamingDF = customerEventsStreamingDF.withColumn("element", unbase64(
    customerEventsStreamingDF.zSetEntries[0].element).cast("string"))
customerEventsStreamingDF = customerEventsStreamingDF.withColumn("element", from_json(
    customerEventsStreamingDF.element, elementSchema))
customerEventsStreamingDF = customerEventsStreamingDF.selectExpr("element.customer.*")
customerEventsStreamingDF = customerEventsStreamingDF.filter('email is not null and birthDay is not null')

customerEventsStreamingDF.createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF = customerEventsStreamingDF.selectExpr("email as email",
                                                                   "split(birthDay,'-')[0] as birthYear")
emailAndBirthDayStreamingDF.createOrReplaceTempView("q")

q = spark.sql(f"SELECT * FROM q") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("numRows", "1000") \
    .start() \
    .awaitTermination()
