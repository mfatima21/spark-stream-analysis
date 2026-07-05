from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

def calculate_sentiment_score(text):
    print(text)
    # perform sentiment analysis and return a score
    return "sentiment_score"

sentiment_udf = udf(calculate_sentiment_score, FloatType())