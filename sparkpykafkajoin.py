from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_URL = "localhost:9092"

spark = SparkSession \
    .builder \
    .appName("kafka-join") \
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

emailAndBirthYearDF = customerEventsStreamingDF.selectExpr("email as email", "split(birthDay,'-')[0] as birthYear")
emailAndBirthYearDF.createOrReplaceTempView("Customers")

riskScoreByBirthYearDF = emailAndBirthYearDF.join(emailAndRiskScoreDF,
                                                  emailAndBirthYearDF.email == emailAndRiskScoreDF.customer)

riskScoreByBirthYearDF = riskScoreByBirthYearDF.withColumn("value", to_json(
    struct([col(x) for x in riskScoreByBirthYearDF.columns])))
riskScoreByBirthYearDF = riskScoreByBirthYearDF.withColumn("key", riskScoreByBirthYearDF.email)

riskScoreByBirthYearDF.selectExpr("key", "value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_URL) \
    .option("topic", "customer-risk") \
    .option("checkpointLocation", "/tmp/risk-join") \
    .start() \
    .awaitTermination()
