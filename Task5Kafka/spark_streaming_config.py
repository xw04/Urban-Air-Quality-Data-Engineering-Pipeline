# Author: Yoo Xin Wei

from pyspark.sql import SparkSession
from pyspark.sql.types import *

class SparkStreamingConfig:
    def __init__(self, app_name="AirQualityStreaming"):
        self.app_name = app_name
        self.spark = None
        self.schema = None
        
    def create_spark_session(self):
        self.spark = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.sql.shuffle.partitions", "5") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        return self.spark
    
    def get_data_schema(self):
        self.schema = StructType([
            StructField("ts", TimestampType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("AQI", DoubleType(), True),
            StructField("co_ppm", DoubleType(), True),
            StructField("humidity_pct", DoubleType(), True),
            StructField("no2_ppb", DoubleType(), True),
            StructField("o3_ppb", DoubleType(), True),
            StructField("pm10_ugm3", DoubleType(), True),
            StructField("pm25_ugm3", DoubleType(), True),
            StructField("so2_ppb", DoubleType(), True),
            StructField("temp_c", DoubleType(), True),
            StructField("wind_ms", DoubleType(), True),
            StructField("event_date", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("day_of_week", StringType(), True),
            StructField("hemisphere", StringType(), True),
            StructField("season", StringType(), True),
            StructField("pm_ratio", DoubleType(), True),
            StructField("aqi_category", StringType(), True),
            StructField("aqi_bucket_pm25", StringType(), True),
            StructField("aqi_bucket_pm10", StringType(), True),
            StructField("aqi_bucket_no2", StringType(), True),
            StructField("aqi_bucket_so2", StringType(), True),
            StructField("aqi_bucket_co", StringType(), True),
            StructField("aqi_bucket_o3", StringType(), True),
            StructField("pm25_exceeds_who", StringType(), True),
            StructField("pm10_exceeds_who", StringType(), True),
            StructField("no2_exceeds_who", StringType(), True),
            StructField("so2_exceeds_who", StringType(), True),
            StructField("co_exceeds_who", StringType(), True),
            StructField("o3_exceeds_who", StringType(), True),
            StructField("who_compliant_all", StringType(), True)
        ])
        return self.schema
    
    def stop_spark(self):
        if self.spark:
            self.spark.stop()