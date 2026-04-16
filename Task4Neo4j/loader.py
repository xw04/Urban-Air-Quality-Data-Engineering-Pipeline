# Author: Lee Qian Hui
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

class Loader:

    def __init__(self, hdfs_path: str = "hdfs://localhost:9000/user/student/air_quality_cleaned/valid_json/part-00000-*.json"):
        self.hdfs_path = hdfs_path
        self.spark = SparkSession.builder.appName("AirQualityLoader").getOrCreate()

    def load_from_hdfs(self) -> DataFrame:
        df = self.spark.read.json(self.hdfs_path, multiLine=False)
        return df
