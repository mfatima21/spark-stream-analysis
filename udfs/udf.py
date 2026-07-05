import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from pyspark.sql.types import DoubleType
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark.sql.functions import expr, udf
from pyspark.sql.functions import dayofweek


# nltk.download('vader_lexicon')
# nltk.download('punkt')
# nltk.download('stopwords')
sid = SentimentIntensityAnalyzer()


category_mapping = {
    'restaurant': 'Restaurant',
    'food': 'Restaurant',
    'diner': 'Restaurant',
    'Entertainment': 'Restaurant',
    'Bars': 'Restaurant',
    'Amusement Parks': 'Entertainment',
    'Amusement': 'Entertainment',
    'Shopping': 'Shopping',
    'Fashion': 'Shopping',
    'Stores': 'Shopping',
    'Clothing': 'Shopping',
    'medical': 'Hospital',
    'clinic': 'Hospital',
    'hospital': 'Hospital',
    'spa': 'Spa',
    'Home': 'Home improvements',
    'other': 'Other'

}

def get_sentiment(text):
    """
    Returns the sentiment score for a given text
    """
    # print(text)
    # if text.empty():
    score = sid.polarity_scores(text)

    return score['compound']


# Define the weighted average UDF
def weighted_average(new_sentiment, review_count, alpha, prev_score):
    # Calculate the weighted average score
    weighted_score = (alpha * new_sentiment + (1 - alpha) * prev_score) / (review_count + 1)
    return weighted_score


def categorize_categories(categories):

    if (categories != None):
        categories = word_tokenize(categories)
        stop_words = set(stopwords.words('english'))

        for category in categories:
            # print(category)
            if category in category_mapping:
                return category_mapping[category]
            else:
                return category_mapping['other']


# Register the UDF
weighted_average_udf = udf(weighted_average, DoubleType())
sentiment_udf = udf(get_sentiment, DoubleType())
categorize_udf = udf(categorize_categories)

weighted_average_udf
sentiment_udf
categorize_udf