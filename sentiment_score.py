import nltk
from pyspark.sql.types import DoubleType
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark.sql.functions import expr, udf

nltk.download('vader_lexicon')
sid = SentimentIntensityAnalyzer()

def get_sentiment(text):
    """
    Returns the sentiment score for a given text
    """
    score = sid.polarity_scores(text)
    return score['compound']

sentiment_udf = udf(get_sentiment, DoubleType())

print(get_sentiment("I love this. it is really really nice. one of the best places to go"))