# Author: Yoo Xin Wei

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql import Window

class StreamAggregator:
    def __init__(self, spark_session):
        self.spark = spark_session
        
    def aggregate_city_metrics(self, stream_df):
        city_agg = stream_df \
            .withWatermark("ts", "2 minutes") \
            .groupBy(
                window(col("ts"), "2 minutes", "1 minute"),  # Shorter window
                col("city"),
                col("country")
            ) \
            .agg(
                avg("AQI").alias("avg_aqi"),
                max("AQI").alias("max_aqi"),
                min("AQI").alias("min_aqi"),
                avg("pm25_ugm3").alias("avg_pm25"),
                avg("pm10_ugm3").alias("avg_pm10"),
                avg("temp_c").alias("avg_temp"),
                avg("humidity_pct").alias("avg_humidity"),
                count("*").alias("measurement_count"),
                sum(when(col("who_compliant_all") == "No", 1).otherwise(0)).alias("who_violations")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("city"),
                col("country"),
                round("avg_aqi", 2).alias("avg_aqi"),
                col("max_aqi"),
                col("min_aqi"),
                round("avg_pm25", 2).alias("avg_pm25"),
                round("avg_pm10", 2).alias("avg_pm10"),
                round("avg_temp", 1).alias("avg_temp"),
                round("avg_humidity", 1).alias("avg_humidity"),
                col("measurement_count"),
                col("who_violations")
            )
            
        return city_agg
    
    def aggregate_country_trends(self, stream_df):
        country_agg = stream_df \
            .withWatermark("ts", "2 minutes") \
            .groupBy(
                window(col("ts"), "3 minutes", "1 minute"),  # Shorter window
                col("country")
            ) \
            .agg(
                avg("AQI").alias("country_avg_aqi"),
                stddev("AQI").alias("aqi_stddev"),
                percentile_approx("AQI", 0.5).alias("median_aqi"),
                count(when(col("AQI") > 200, True)).alias("hazardous_count"),
                count(when(col("AQI") > 150, True)).alias("unhealthy_count"),
                count("*").alias("total_readings")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("country"),
                round("country_avg_aqi", 2).alias("country_avg_aqi"),
                round("aqi_stddev", 2).alias("aqi_stddev"),
                round("median_aqi", 2).alias("median_aqi"),
                col("hazardous_count"),
                col("unhealthy_count"),
                col("total_readings"),
                round((col("unhealthy_count") / col("total_readings")) * 100, 2).alias("unhealthy_percentage")
            )
            
        return country_agg
    
    def aggregate_pollutant_correlations(self, stream_df):
        pollutant_agg = stream_df \
            .withWatermark("ts", "2 minutes") \
            .groupBy(
                window(col("ts"), "5 minutes", "2 minutes")  # Shorter window
            ) \
            .agg(
                corr("pm25_ugm3", "AQI").alias("pm25_aqi_corr"),
                corr("pm10_ugm3", "AQI").alias("pm10_aqi_corr"),
                corr("no2_ppb", "AQI").alias("no2_aqi_corr"),
                avg("pm25_ugm3").alias("global_avg_pm25"),
                avg("pm10_ugm3").alias("global_avg_pm10"),
                avg("AQI").alias("global_avg_aqi")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                round("pm25_aqi_corr", 3).alias("pm25_aqi_corr"),
                round("pm10_aqi_corr", 3).alias("pm10_aqi_corr"),
                round("no2_aqi_corr", 3).alias("no2_aqi_corr"),
                round("global_avg_pm25", 2).alias("global_avg_pm25"),
                round("global_avg_pm10", 2).alias("global_avg_pm10"),
                round("global_avg_aqi", 2).alias("global_avg_aqi")
            )
            
        return pollutant_agg