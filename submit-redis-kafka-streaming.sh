#!/bin/bash
docker exec -it  cd0036-data-streaming-api-development-and-documentation-spark-1 /opt/bitnami/spark/bin/spark-submit  \
--conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/opt/bitnami/spark/jars" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
 /home/workspace/project/starter/sparkpyrediskafkastreamtoconsole.py | tee ../../spark/logs/redis-kafka.log