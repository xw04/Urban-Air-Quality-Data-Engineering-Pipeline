# Author: Yoo Xin Wei

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.ml.feature import Bucketizer

class StreamTransformer:
    def __init__(self, spark_session):
        self.spark = spark_session
        
    def transform_health_risk_scores(self, stream_df):
        transformed = stream_df.withColumn(
            "health_risk_score",
            when(col("AQI") <= 50, lit(1))
            .when((col("AQI") > 50) & (col("AQI") <= 100), lit(2))
            .when((col("AQI") > 100) & (col("AQI") <= 150), lit(3))
            .when((col("AQI") > 150) & (col("AQI") <= 200), lit(4))
            .when((col("AQI") > 200) & (col("AQI") <= 300), lit(5))
            .otherwise(lit(6))
        ).withColumn(
            "health_risk_label",
            when(col("health_risk_score") == 1, lit("Good"))
            .when(col("health_risk_score") == 2, lit("Moderate"))
            .when(col("health_risk_score") == 3, lit("Unhealthy for Sensitive"))
            .when(col("health_risk_score") == 4, lit("Unhealthy"))
            .when(col("health_risk_score") == 5, lit("Very Unhealthy"))
            .otherwise(lit("Hazardous"))
        )
        
        return transformed
    
    def transform_weather_impact(self, stream_df):
        transformed = stream_df.withColumn(
            "weather_impact_score",
            (
                when(col("wind_ms") > 5, lit(-0.3)).otherwise(lit(0)) +
                when(col("humidity_pct") > 70, lit(0.2)).otherwise(lit(0)) +
                when(col("temp_c") > 30, lit(0.1)).otherwise(lit(0)) +
                when(col("temp_c") < 10, lit(0.1)).otherwise(lit(0))
            )
        ).withColumn(
            "adjusted_aqi",
            round(col("AQI") * (1 + col("weather_impact_score")), 2)
        ).withColumn(
            "weather_condition",
            when(col("wind_ms") > 8, lit("Windy"))
            .when(col("humidity_pct") > 80, lit("Humid"))
            .when(col("temp_c") > 35, lit("Hot"))
            .when(col("temp_c") < 5, lit("Cold"))
            .otherwise(lit("Normal"))
        )
        
        return transformed
    
    def transform_pollution_sources(self, stream_df):
        transformed = stream_df.withColumn(
            "primary_pollutant",
            when(
                (col("pm25_ugm3") > col("pm10_ugm3")) & 
                (col("pm25_ugm3") > col("no2_ppb")) & 
                (col("pm25_ugm3") > col("so2_ppb")),
                lit("PM2.5")
            ).when(
                (col("pm10_ugm3") > col("pm25_ugm3")) & 
                (col("pm10_ugm3") > col("no2_ppb")) & 
                (col("pm10_ugm3") > col("so2_ppb")),
                lit("PM10")
            ).when(
                col("no2_ppb") > col("so2_ppb"),
                lit("NO2")
            ).otherwise(lit("SO2"))
        ).withColumn(
            "pollution_source",
            when(col("primary_pollutant") == "PM2.5", lit("Vehicle/Industrial Emissions"))
            .when(col("primary_pollutant") == "PM10", lit("Construction/Dust"))
            .when(col("primary_pollutant") == "NO2", lit("Traffic Emissions"))
            .otherwise(lit("Industrial Activity"))
        ).withColumn(
            "seasonal_factor",
            when(col("season") == "Winter", lit(1.2))
            .when(col("season") == "Summer", lit(0.9))
            .otherwise(lit(1.0))
        ).withColumn(
            "seasonal_adjusted_aqi",
            round(col("AQI") * col("seasonal_factor"), 2)
        )
        
        return transformed
    
    def transform_alert_levels(self, stream_df):
        transformed = stream_df.withColumn(
            "alert_priority",
            when(col("AQI") > 300, lit(1))
            .when((col("AQI") > 200) & (col("who_compliant_all") == "No"), lit(2))
            .when((col("AQI") > 150) & (col("pm25_exceeds_who") == "Yes"), lit(3))
            .when(col("AQI") > 100, lit(4))
            .otherwise(lit(5))
        ).withColumn(
            "alert_message",
            when(col("alert_priority") == 1, 
                 concat(lit(" HAZARDOUS: "), col("city"), lit(" - Immediate action required!")))
            .when(col("alert_priority") == 2, 
                  concat(lit(" VERY UNHEALTHY: "), col("city"), lit(" - WHO standards violated")))
            .when(col("alert_priority") == 3, 
                  concat(lit(" UNHEALTHY: "), col("city"), lit(" - PM2.5 exceeds limits")))
            .when(col("alert_priority") == 4, 
                  concat(lit(" MODERATE: "), col("city"), lit(" - Monitor conditions")))
            .otherwise(concat(lit(" GOOD: "), col("city"), lit(" - Air quality acceptable")))
        ).withColumn(
            "alert_timestamp",
            current_timestamp()
        )
        
        return transformed