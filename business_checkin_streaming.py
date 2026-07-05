from kafka_utils import create_direct_stream
# from sentiment_score import sentiment_udf

from config.sparkConfig import getSparkSession
from config.sparkStreams import getStreamedData
from processors.businessCheckinMatrics import *
from processors.businessReviewSentimentAnalysis import calulateSentimentMatrics
from preprocessing.preprocessCheckin import preprocessCheckinData

if __name__ == "__main__":
    # preprocessCheckinData()
    sparkSession = getSparkSession()
    businessStream, reviewStream, checkinStream = getStreamedData(sparkSession)
    calculateBusiestDay(sparkSession, businessStream, checkinStream)


