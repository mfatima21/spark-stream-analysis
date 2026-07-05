import os
from schemas.reviewSchema import reviewSchema
from schemas.businessSchema import businessSchema
from schemas.checkinSchema import checkinSchema
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import from_json, col

from kafka_utils import create_direct_stream

from pyspark.sql import SparkSession


def getReviewStreamFromKafka(sparkSession, kafkaServers, reviewTopic):
    kafka_df = create_direct_stream(sparkSession, kafkaServers, reviewTopic)
    reviewStream = kafka_df.select(from_json(col("value").cast("string"), reviewSchema).alias("data")).select("data.*")
    return reviewStream


def getStreamedData(sparkSession):

    dataDir = os.getenv("DATA_DIR", "file:///home/ubuntu/data/mycsv/")
    kafkaServers = os.getenv("KAFKA_SERVERS")
    reviewTopic = os.getenv("REVIEW_TOPIC", "reviews")

    businessStream = sparkSession.read.format('csv').option("header", "true").schema(businessSchema).options(pathGlobFilter='business.csv').load(dataDir)

    if kafkaServers:
        reviewStream = getReviewStreamFromKafka(sparkSession, kafkaServers, reviewTopic)
    else:
        reviewStream = sparkSession.readStream.format('csv').option("header", "true").schema(reviewSchema).options(pathGlobFilter='review.csv').load(dataDir)
    # reviewStream.writeStream.format("console").start().awaitTermination()

    # checkinStream = sparkSession.readStream.format('json').schema(checkinSchema).options(pathGlobFilter='preprocessed_checkin_data2.json').load("file:///home/ubuntu/data/")
    # collected_business_data = businessStream.writeStream.collect()

    # broadcasted_business_df = sparkSession.sparkContext.broadcast(collected_business_data)
    return businessStream, reviewStream
    # return checkinStream

getStreamedData