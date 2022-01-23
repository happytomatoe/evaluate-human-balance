#!/bin/bash
docker cp  sparkpyrediskafkastreamtoconsole.py spark-master:/home/workspace/sparkpyrediskafkastreamtoconsole.py
docker exec -it  spark-master /opt/bitnami/spark/bin/spark-submit  \
--conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/opt/bitnami/spark/jars" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
 /home/workspace/sparkpyrediskafkastreamtoconsole.py
