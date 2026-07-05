from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, FloatType, StructType, StructField

reviewSchema = StructType([
    StructField("review_id", StringType(), nullable=True),
    StructField("user_id", StringType(), nullable=True),
    StructField("business_id", StringType(), nullable=True),
    StructField("stars", FloatType(), nullable=True),
    StructField("useful", FloatType(), nullable=True),
    StructField("funny", FloatType(), nullable=True),
    StructField("cool", FloatType(), nullable=True),
    StructField("text", StringType(), nullable=False),
    StructField("date", DateType(), nullable=False),
])

reviewSchema