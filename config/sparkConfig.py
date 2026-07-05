
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
load_dotenv()


def getSparkConfig():
    conf = SparkConf()
    conf.set("spark.master", os.getenv("SPARK_MASTER", "local[*]"))
    conf.set("spark.driver.memory", "6g")
    conf.set("spark.executor.memory", "6g")
    conf.set("spark.memory.offHeap.enabled",True)
    conf.set("spark.memory.offHeap.size","6g")
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.ansi.enabled", "false")
    conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    conf.set("spark.network.timeout", "600s")
    return conf

def getSparkSession():
    conf = getSparkConfig()
    print(conf)
    spark = SparkSession.builder.config(conf=conf) \
        .getOrCreate()
    print(spark)
    # spark.sparkContext.setLogLevel("WARN")

    # spark.sparkContext.setLogLevel("DEBUG")
    # logger = spark._jvm.org.apache.log4j.Logger.getRootLogger()
    # logger.setLevel(spark._jvm.org.apache.log4j.Level.DEBUG)
    return spark
