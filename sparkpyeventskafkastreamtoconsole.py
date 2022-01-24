from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, DecimalType

spark = SparkSession \
    .builder \
    .appName("test") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


stediEventsRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_URL) \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

stediEventsRawStreamingDF = stediEventsRawStreamingDF.select(stediEventsRawStreamingDF.key.cast("string").alias("key"),
                                                             stediEventsRawStreamingDF.value.cast("string").alias(
                                                                 "value"))
# {
#   "customer": "Jason.Staples@test.com",
#   "score": 1.5,
#   "riskDate": "2022-01-23T22:22:37.110Z"
# }
stediEventsValueSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", DecimalType(10, 1)),
])
stediEventsRawStreamingDF = stediEventsRawStreamingDF.select(
    from_json(stediEventsRawStreamingDF.value, stediEventsValueSchema).alias("value"))
emailAndRiskScoreDF = stediEventsRawStreamingDF.selectExpr("value.*")

emailAndRiskScoreDF.createOrReplaceTempView("CustomerRisk")

spark.sql(f"SELECT customer,score FROM CustomerRisk") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
