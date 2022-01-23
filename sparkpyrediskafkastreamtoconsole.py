from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import *



spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
#

# TO-DO: create a StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic
VIEW_NAME = "RedisSortedSet"

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

schema = StructType([
    StructField("key", StringType()),
    StructField("value", StringType()),
    StructField("existType", StringType()),
    StructField("Ch", StringType()),
    StructField("zSetEntries", ArrayType(StringType(), False)),
])


df = df.select(from_json(df.value.cast("string"), schema).alias("root2"))
df.createOrReplaceTempView(VIEW_NAME)

q=spark.sql(f"SELECT * FROM {VIEW_NAME}") \
    .writeStream \
    .format("memory") \
    .queryName("q") \
    .start()

v = StructType([
    StructField("element", StringType()),
    StructField("score", DecimalType()),

])

raw = spark.sql("select root2.zSetEntries[0].element  from q")
raw.show(truncate=False)
# TO-DO: execute a sql statement against a temporary view, which statement takes the element field from the 0th
# element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe,
# and it makes it easy to select the nth element of an array in a sql column

# TO-DO: take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
# +--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}

# TO-DO: parse the JSON in the Customer record and store in a temporary view called CustomerRecords

# TO-DO: JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF

# TO-DO: from the emailAndBirthDayStreamingDF dataframe select the email and the birth year (using the split function)

# TO-DO: Split the birth year as a separate field from the birthday
# TO-DO: Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF

# TO-DO: sink the emailAndBirthYearStreamingDF dataframe to the console in append mode
# 
# The output should look like this:
# +--------------------+-----               
# | email         |birthYear|
# +--------------------+-----
# |Gail.Spencer@test...|1963|
# |Craig.Lincoln@tes...|1962|
# |  Edward.Wu@test.com|1961|
# |Santosh.Phillips@...|1960|
# |Sarah.Lincoln@tes...|1959|
# |Sean.Howard@test.com|1958|
# |Sarah.Clark@test.com|1957|
# +--------------------+-----

# Run the python script by running the command from the terminal:
# /home/workspace/submit-redis-kafka-streaming.sh
# Verify the data looks correct
