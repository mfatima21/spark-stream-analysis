from pyspark.sql.types import StringType, DoubleType, TimestampType, StructType, StructField

checkinSchema = StructType([
    StructField("business_id", StringType(), nullable=False),
    StructField("date", StringType(), nullable=False),
])

checkinSchema