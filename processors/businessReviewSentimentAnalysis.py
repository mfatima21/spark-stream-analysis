import os
from udfs.udf import sentiment_udf, weighted_average, categorize_udf
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import *
import pandas as pd
from tabulate import tabulate

# from pyspark.sql import OutputMode
from elasticsearch.helpers import bulk
from pyspark.sql.functions import monotonically_increasing_id


from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from datetime import datetime
from elasticsearch import Elasticsearch
alpha = 0.5

def calculateReviewSentiment(batch_df, batch_id, businessStream):
    batch_df = batch_df.select("business_id", "date", 'user_id', "text") \
        .withColumn("sentiment_score", sentiment_udf(batch_df['text'])) \
        .withColumn('year', year("date")) \
        .withColumn('month', year("date")) \
        .withColumn('day_of_week', dayofweek("date")) \
        .withColumn('day_of_week', date_format('date', 'EEEE'))

    result = batch_df.join(businessStream, "business_id")
    # result = result.select("business_id", "name", "date", 'user_id', 'sentiment_score', 'year', 'month', 'day_of_week', 'event_time')

    print("---------------------------------------------- this is my batch df ---------------------------------------------")
    # batch_df.writeStream.format("console").start().awaitTermination()
    query = result \
    .writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes.wan.only", "true") \
    .option("es.net.http.auth.user", os.getenv("ES_USER")) \
    .option("es.net.http.auth.pass", os.getenv("ES_PASS")) \
    .option("es.port", os.getenv("ES_PORT")) \
    .option("es.nodes", os.getenv("ES_NODES")) \
    .option("es.resource", "review_sentimental"+"/"+"json") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
    
    query.awaitTermination()

    # .mode("overwrite").save()
    # return

    # print(query)



# def process(batch_df, batch_id):
#     print(batch_df.show())

def calulateSentimentMatrics(sparkSession, businessStream, reviewStream):
    businessStream = businessStream.select("name", "business_id", "review_count")
    reviewStream.writeStream.foreachBatch(lambda batch_df, batch_id: calculateReviewSentiment(batch_df, batch_id, businessStream)).start().awaitTermination()
















    # processed_stream = reviewStream.select("business_id", "date", 'user_id') \
    #     .withColumn("sentiment_score", sentiment_udf(reviewStream['text']))

# calulateCheckinMatrics