from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, DecimalType

spark = SparkSession \
    .builder \
    .appName("test") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

df = df.select(df.key.cast("string").alias("key"), df.value.cast("string").alias("value"))
# {
#   "customer": "Jason.Staples@test.com",
#   "score": 1.5,
#   "riskDate": "2022-01-23T22:22:37.110Z"
# }
schema = StructType([
    StructField("customer", StringType()),
    StructField("score", DecimalType(10, 1)),
])
df = df.select(from_json(df.value, schema).alias("value"))
df = df.selectExpr("value.*")

df.createOrReplaceTempView("CustomerRisk")

q = spark.sql(f"SELECT customer,score FROM CustomerRisk") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream

# TO-DO: cast the value column in the streaming dataframe as a STRING 

# TO-DO: parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
# TO-DO: execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
# TO-DO: sink the customerRiskStreamingDF dataframe to the console in append mode
# 
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct
