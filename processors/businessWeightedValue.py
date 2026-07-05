import os
from udfs.udf import sentiment_udf, weighted_average, categorize_udf
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import *
import pandas as pd
from tabulate import tabulate
from pyspark.sql.window import Window

# from pyspark.sql import OutputMode
from elasticsearch.helpers import bulk
from pyspark.sql.functions import monotonically_increasing_id


from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from datetime import datetime
from elasticsearch import Elasticsearch
alpha = 0.5
decay_rate = 0.01
max_days = 30
def calculateReviewSentiment(batch_df, batch_id, businessStream):
    batch_df = batch_df.select("text", "business_id", "user_id", "review_id", "useful", "date")
    result = batch_df.join(businessStream, "business_id")
    result = result.filter(businessStream.state == "LA")
    final = result.withColumn("sentiment_score", sentiment_udf(batch_df['text'])) \
        .withColumn('year', year("date")) \
        .withColumn('month', month("date")) \
        .withColumn('day_of_week', dayofweek("date")) \
        .withColumn('day_of_week', date_format('date', 'EEEE'))

    weighted_avg_stream = final.withColumn("decay_factor", expr(f"EXP(-{decay_rate} * DATEDIFF(CURRENT_TIMESTAMP(), date))").cast(FloatType())) \
        .withColumn("weighted_score", expr("(sentiment_score * (useful + 0.1) * decay_factor)"))

    normalizedScores = weighted_avg_stream.withColumn("normalized_score", col("weighted_score") / col("review_count"))
    normalizedScores = normalizedScores.select("business_id", "user_id", "date", "name", "stars", "sentiment_score", "year", "month", "day_of_week", "weighted_score", "normalized_score" )

    normalizedScores \
        .write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes.wan.only", "true") \
        .option("es.net.http.auth.user", os.getenv("ES_USER")) \
        .option("es.net.http.auth.pass", os.getenv("ES_PASS")) \
        .option("es.port", os.getenv("ES_PORT")) \
        .option("es.nodes", os.getenv("ES_NODES")) \
        .option("es.resource", "business_weighted_averge"+"/"+"json") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .mode("overwrite") \
        .save()
    
def calulateWeightedMatrics(sparkSession, businessStream, reviewStream):
    # print(reviewStream)
    # businessStream.show()
    businessStream = businessStream.select("name", "business_id", "review_count", "stars", "state")
    # reviewStream.writeStream.format("console").start().awaitTermination()
    reviewStream.writeStream.foreachBatch(lambda batch_df, batch_id: calculateReviewSentiment(batch_df, batch_id, businessStream)).start().awaitTermination()
    # reviewStream.writeStream.foreachBatch(lambda batch_df, batch_id: calculateReviewSentiment(batch_df, batch_id, businessStream)).start().awaitTermination()
















    # processed_stream = reviewStream.select("business_id", "date", 'user_id') \
    #     .withColumn("sentiment_score", sentiment_udf(reviewStream['text']))

# calulateCheckinMatrics