from pyspark.sql import SparkSession

def create_direct_stream(spark, kafka_params, topics):
    print(kafka_params)
    print(spark)
    print(topics)
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_params) \
        .option("subscribe", topics) \
        .load()
    return kafka_df