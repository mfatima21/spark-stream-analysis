from pyspark.sql.types import StringType, BooleanType, ArrayType, IntegerType, DoubleType, DateType, FloatType, StructType, StructField

businessSchema = StructType([
    StructField("business_id", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("address", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("postal_code", StringType(), nullable=True),
    StructField("latitude", DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),
    StructField("stars", DoubleType(), nullable=True),
    StructField("review_count", DoubleType(), nullable=True),
    StructField("is_open", BooleanType(), nullable=True),
    StructField("attributes", StringType(), nullable=True),  # Update the type if it's a nested structure
    StructField("categories", StringType(), nullable=True),
    StructField("hours", StringType(), nullable=True),  # Update the type if it's a nested structure
])

businessSchema