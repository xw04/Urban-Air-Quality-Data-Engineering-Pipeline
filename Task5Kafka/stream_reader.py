# Author: Yoo Xin Wei

from pyspark.sql import DataFrame
from pyspark.sql.functions import *

class StreamReader:
    def __init__(self, spark_session, schema):
        self.spark = spark_session
        self.schema = schema
        self.stream_df = None
        
    def read_hdfs_stream(self, hdfs_path, max_files_per_trigger=1):
        self.stream_df = self.spark.readStream \
            .schema(self.schema) \
            .option("maxFilesPerTrigger", max_files_per_trigger) \
            .json(hdfs_path)
        
        print(f" Stream reader initialized from: {hdfs_path}")
        print(f" Schema applied with {len(self.schema.fields)} fields")
        
        return self.stream_df
    
    def add_processing_time(self):
        if self.stream_df is not None:
            self.stream_df = self.stream_df.withColumn(
                "processing_time", 
                current_timestamp()
            )
        return self.stream_df
    
    def filter_critical_aqi(self, threshold=150):
        if self.stream_df is not None:
            self.stream_df = self.stream_df.filter(col("AQI") >= threshold)
        return self.stream_df
    
    def get_stream(self):
        return self.stream_df