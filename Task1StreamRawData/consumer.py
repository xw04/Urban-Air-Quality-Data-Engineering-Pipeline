# Author: Yaw Wei Ying

import argparse
from datetime import datetime
from pyspark.sql import SparkSession, functions as F, types as T

class KafkaStreamConsumer:
    RAW_COLUMNS = [
        "Date", "City", "Country", "AQI",
        "PM2.5 (µg/m³)", "PM10 (µg/m³)", "NO2 (ppb)", "SO2 (ppb)",
        "CO (ppm)", "O3 (ppb)", "Temperature (°C)", "Humidity (%)",
        "Wind Speed (m/s)"
    ]
    RAW_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in RAW_COLUMNS])
    STRUCT_SCHEMA = T.StructType([
        T.StructField("Date", T.StringType()),
        T.StructField("City", T.StringType()),
        T.StructField("Country", T.StringType()),
        T.StructField("AQI", T.DoubleType()),
        T.StructField("PM2.5 (µg/m³)", T.DoubleType()),
        T.StructField("PM10 (µg/m³)", T.DoubleType()),
        T.StructField("NO2 (ppb)", T.DoubleType()),
        T.StructField("SO2 (ppb)", T.DoubleType()),
        T.StructField("CO (ppm)", T.DoubleType()),
        T.StructField("O3 (ppb)", T.DoubleType()),
        T.StructField("Temperature (°C)", T.DoubleType()),
        T.StructField("Humidity (%)", T.DoubleType()),
        T.StructField("Wind Speed (m/s)", T.DoubleType()),
    ])

    def __init__(self, bootstrap, topic, hdfs_path, checkpoint, raw_path, checkpoint_raw, trigger):
        self.bootstrap = bootstrap
        self.topic = topic
        self.hdfs_path = hdfs_path
        self.checkpoint = checkpoint
        self.raw_path = raw_path
        self.checkpoint_raw = checkpoint_raw
        self.trigger = trigger
        self.spark = self.get_spark()

    def get_spark(self):
        spark = (SparkSession.builder
                 .appName("Kafka->HDFS-Combined")
                 .config("spark.sql.codegen.wholeStage", "false")
                 .config("spark.sql.codegen.fallback", "true")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def write_raw_batch(self, df, batch_id):
        date_str = datetime.utcnow().strftime("%Y%m%d")
        (df.coalesce(1)
           .write.mode("append")
           .option("header", True)
           .csv(f"{self.raw_path}/ingest_dt={date_str}/batch_id={batch_id}"))

    def run(self):
        kdf = (self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load())
        
        kdf_print = (
            kdf.selectExpr("CAST(value AS STRING) AS json_str", "topic")
               .select(
                   F.col("topic"),
                   F.from_json("json_str", self.RAW_SCHEMA).alias("r")
               )
               .select("topic", "r.*")
               .withColumnRenamed("PM2.5 (µg/m³)", "pm25")
               .withColumnRenamed("PM10 (µg/m³)",  "pm10")
               .withColumnRenamed("NO2 (ppb)",     "no2")
               .withColumnRenamed("SO2 (ppb)",     "so2")
               .withColumnRenamed("CO (ppm)",      "co")
               .withColumnRenamed("O3 (ppb)",      "o3")
               .withColumnRenamed("Temperature (°C)", "temp")
               .withColumnRenamed("Humidity (%)",  "humidity")
               .withColumnRenamed("Wind Speed (m/s)", "wind")
               .withColumn("AQI", F.when(F.trim(F.col("AQI")).isin("", "N/A","n/a","na","NaN","null","—","-"), None)
                                   .otherwise(F.regexp_replace(F.col("AQI"), r"[^0-9+\-\.]", "")).cast("double"))
               .withColumn("pm25", F.when(F.trim(F.col("pm25")).isin("", "N/A","n/a","na","NaN","null","—","-"), None)
                                   .otherwise(F.regexp_replace(F.col("pm25"), r"[^0-9+\-\.]", "")).cast("double"))
               .withColumn("pm10", F.when(F.trim(F.col("pm10")).isin("", "N/A","n/a","na","NaN","null","—","-"), None)
                                   .otherwise(F.regexp_replace(F.col("pm10"), r"[^0-9+\-\.]", "")).cast("double"))
               .withColumn("no2",  F.when(F.trim(F.col("no2")).isin("", "N/A","n/a","na","NaN","null","—","-"), None)
                                   .otherwise(F.regexp_replace(F.col("no2"), r"[^0-9+\-\.]", "")).cast("double"))
               .withColumn("so2",  F.when(F.trim(F.col("so2")).isin("", "N/A","n/a","na","NaN","null","—","-"), None)
                                   .otherwise(F.regexp_replace(F.col("so2"), r"[^0-9+\-\.]", "")).cast("double"))
               .withColumn("co",   F.when(F.trim(F.col("co")).isin("", "N/A","n/a","na","NaN","null","—","-"), None)
                                   .otherwise(F.regexp_replace(F.col("co"), r"[^0-9+\-\.]", "")).cast("double"))
               .withColumn("o3",   F.when(F.trim(F.col("o3")).isin("", "N/A","n/a","na","NaN","null","—","-"), None)
                                   .otherwise(F.regexp_replace(F.col("o3"), r"[^0-9+\-\.]", "")).cast("double"))
               .withColumn("temp", F.when(F.trim(F.col("temp")).isin("", "N/A","n/a","na","NaN","null","—","-"), None)
                                   .otherwise(F.regexp_replace(F.col("temp"), r"[^0-9+\-\.]", "")).cast("double"))
               .withColumn("humidity", F.when(F.trim(F.col("humidity")).isin("", "N/A","n/a","na","NaN","null","—","-"), None)
                                   .otherwise(F.regexp_replace(F.col("humidity"), r"[^0-9+\-\.]", "")).cast("double"))
               .withColumn("wind", F.when(F.trim(F.col("wind")).isin("", "N/A","n/a","na","NaN","null","—","-"), None)
                                   .otherwise(F.regexp_replace(F.col("wind"), r"[^0-9+\-\.]", "")).cast("double"))
               .withColumn("processed_at", F.current_timestamp())
               .select("topic","Date","City","Country","AQI","pm25","pm10",
                       "no2","so2","co","o3","temp","humidity","wind","processed_at")
        )

        console_query = (kdf_print.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("numRows", "50")
            .trigger(processingTime=self.trigger)
            .queryName("console_preview")
            .start())
        
        raw_parsed = (kdf
            .select(F.from_json(F.col("value").cast("string"), self.RAW_SCHEMA).alias("r"))
            .select([F.col(f"r.`{c}`").alias(c) for c in self.RAW_COLUMNS])
        )

        raw_query = (raw_parsed.writeStream
            .foreachBatch(self.write_raw_batch)
            .option("checkpointLocation", self.checkpoint_raw)
            .trigger(processingTime=self.trigger)
            .start())

        structured_df = (kdf
            .selectExpr("CAST(value AS STRING) AS json_str")
            .select(F.from_json("json_str", self.STRUCT_SCHEMA).alias("r"))
            .select("r.*")
        )

        struct_query = (structured_df.writeStream
            .format("json")
            .option("path", self.hdfs_path)
            .option("checkpointLocation", self.checkpoint)
            .trigger(processingTime=self.trigger)
            .start())

        raw_query.awaitTermination()
        struct_query.awaitTermination()
        console_query.awaitTermination()

def parse_args():
    p = argparse.ArgumentParser("Kafka stream to HDFS (RAW CSV + Structured JSON)")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="air_quality")
    p.add_argument("--hdfs_path", default="hdfs://localhost:9000/air_quality_json_struct")
    p.add_argument("--checkpoint", default="hdfs://localhost:9000/checkpoints/air_quality_json_struct_v2")
    p.add_argument("--raw_path", default="hdfs://localhost:9000/global_air_quality/raw_csv")
    p.add_argument("--checkpoint_raw", default="hdfs://localhost:9000/checkpoints/air_quality_raw_v2")
    p.add_argument("--trigger", default="10 seconds")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    app = KafkaStreamConsumer(**vars(args))
    app.run()
