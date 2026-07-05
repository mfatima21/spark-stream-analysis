import os
from pyspark.sql import functions as F
from pyspark.sql.functions import window
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import date_format

from pyspark.sql.functions import col, to_timestamp, date_trunc
from pyspark.sql.functions import from_json, col, count
# from pyspark.sql import OutputMode
from elasticsearch.helpers import bulk
from pyspark.sql.functions import monotonically_increasing_id

from pyspark.sql.functions import dayofweek, hour, year, month, first

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from datetime import datetime
from elasticsearch import Elasticsearch

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import *
from udfs.udf import *


def processBatch(batch_df, batch_id, business_dstream):
    batch_df = batch_df.withColumn('year', year("date"))
    batch_df = batch_df.withColumn('month', month("date"))
    batch_df = batch_df.withColumn('checkin_time', col('date'))
    batch_df = batch_df.withColumn('day_of_week', dayofweek("date"))
    batch_df = batch_df.withColumn('day_of_week', date_format('date', 'EEEE'))
    batch_df = batch_df.withColumn('event_time', date_trunc('day', col('date')).cast(TimestampType()))

    batch_df = batch_df.withWatermark('event_time', '365 days') \
        .repartition('business_id') \
        .groupBy(window('event_time', '365 days', '365 days'), 'business_id', 'day_of_week') \
        .agg(count('*').alias('checkin_count'),
            first('year').alias('year'), 
            first('month').alias('month'), 
            first('checkin_time').alias('checkin_time'))

    result = batch_df.join(business_dstream, "business_id") 
    result = result.select("year", "month", "checkin_time", "day_of_week", "name", "checkin_count", "business_id", "category", "city")
    # print(result)
    query = result \
    .writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes.wan.only", "true") \
    .option("es.net.http.auth.user", os.getenv("ES_USER")) \
    .option("es.net.http.auth.pass", os.getenv("ES_PASS")) \
    .option("es.port", os.getenv("ES_PORT")) \
    .option("es.nodes", os.getenv("ES_NODES")) \
    .option("es.resource", "business_checkin_matrics"+"/"+"json") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
    
    query.awaitTermination()

def calculateBusiestDay(sparkSession, business_dstream, checkin_dstream):
    business_dstream = business_dstream.select("name", "city", "business_id", "categories", "stars", "review_count", "is_open") \
                        .withColumn('category', categorize_udf(business_dstream['categories']))
    joinedStream = checkin_dstream.writeStream.foreachBatch(lambda batch_df, batch_id: processBatch(batch_df, batch_id, business_dstream)).start().awaitTermination()


calculateBusiestDay