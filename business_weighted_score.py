from kafka_utils import create_direct_stream
# from sentiment_score import sentiment_udf

from config.sparkConfig import getSparkSession
from config.sparkStreams import getStreamedData
from processors.businessCheckinMatrics import *
from processors.businessWeightedValue import calulateWeightedMatrics
# from preprocessing.preprocessCheckin import preprocessCheckinData, saveParqueFile

if __name__ == "__main__":
    sparkSession = getSparkSession()
    # saveParqueFile()
    businessStream, reviewStream = getStreamedData(sparkSession)
    # print(reviewStream)
    calulateWeightedMatrics(sparkSession, businessStream, reviewStream)


