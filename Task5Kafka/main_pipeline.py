# Author: Yoo Xin Wei

import sys
import argparse
from spark_streaming_config import SparkStreamingConfig
from stream_reader import StreamReader
from stream_aggregator import StreamAggregator
from stream_transformer import StreamTransformer
from stream_writer import StreamWriter
from pyspark.sql.functions import *
import time
import os

class AirQualityStreamingPipeline:
    def __init__(self, hdfs_path, parquet_base="/tmp/streaming_parquet"):
        self.hdfs_path = hdfs_path
        self.parquet_base = parquet_base
        
        self.config = SparkStreamingConfig()
        self.spark = self.config.create_spark_session()
        self.schema = self.config.get_data_schema()
        
        self.reader = StreamReader(self.spark, self.schema)
        self.aggregator = StreamAggregator(self.spark)
        self.transformer = StreamTransformer(self.spark)
        self.writer = StreamWriter(self.spark)
        
        os.makedirs(self.parquet_base, exist_ok=True)
        
    def run_streaming_operation(self):
        print("\n" + "="*60)
        print("Starting Air Quality Streaming Pipeline")
        print("="*60)
        
        print("\nOPERATION 1: DATA STREAMING")
        base_stream = self.reader.read_hdfs_stream(self.hdfs_path)
        enhanced_stream = self.reader.add_processing_time()
        
        print("\nOPERATION 2: AGGREGATION")
        city_aggregations = self.aggregator.aggregate_city_metrics(enhanced_stream)
        country_aggregations = self.aggregator.aggregate_country_trends(enhanced_stream)
        pollutant_correlations = self.aggregator.aggregate_pollutant_correlations(enhanced_stream)
        
        print("\nOPERATION 3: TRANSFORMATION")
        health_risk_stream = self.transformer.transform_health_risk_scores(enhanced_stream)
        weather_impact_stream = self.transformer.transform_weather_impact(health_risk_stream)
        pollution_source_stream = self.transformer.transform_pollution_sources(weather_impact_stream)
        alert_stream = self.transformer.transform_alert_levels(pollution_source_stream)
        
        print("\nStarting streaming queries...")
        
        queries = []
        
        query1 = self.writer.write_to_memory(
            city_aggregations, 
            "city_metrics", 
            output_mode="complete",
            trigger_interval="10 seconds"
        )
        queries.append(query1)
        
        query2 = self.writer.write_to_memory(
            country_aggregations,
            "country_trends",
            output_mode="complete",
            trigger_interval="10 seconds"
        )
        queries.append(query2)
        
        query3 = self.writer.write_to_memory(
            pollutant_correlations,
            "pollutant_correlations",
            output_mode="complete",
            trigger_interval="15 seconds"
        )
        queries.append(query3)
        
        query4 = self.writer.write_to_memory(
            alert_stream,
            "health_alerts",
            output_mode="append",
            trigger_interval="5 seconds"
        )
        queries.append(query4)
        
        parquet_query1 = self.writer.write_batch_to_parquet(
            city_aggregations,
            f"{self.parquet_base}/city_metrics",
            f"{self.parquet_base}/checkpoints/city_metrics",
            trigger_interval="10 seconds"
        )
        queries.append(parquet_query1)
        
        parquet_query2 = self.writer.write_batch_to_parquet(
            country_aggregations,
            f"{self.parquet_base}/country_trends",
            f"{self.parquet_base}/checkpoints/country_trends",
            trigger_interval="10 seconds"
        )
        queries.append(parquet_query2)
        
        parquet_query3 = self.writer.write_append_to_parquet(
            alert_stream.filter(col("alert_priority") <= 3),
            f"{self.parquet_base}/alerts",
            f"{self.parquet_base}/checkpoints/alerts",
            trigger_interval="5 seconds"
        )
        queries.append(parquet_query3)
        
        query5 = self.writer.write_to_console(
            alert_stream.select(
                "city", "country", "AQI", "health_risk_label", 
                "primary_pollutant", "alert_priority", "alert_message"
            ).filter(col("alert_priority") <= 3),
            output_mode="append",
            trigger_interval="10 seconds",
            num_rows=10
        )
        queries.append(query5)
        
        print("\nAll streaming queries started successfully!")
        print(f"Parquet files: {self.parquet_base}")
        print("\n" + "="*60)
        print("Monitoring Streaming Queries")
        print("="*60)
        
        return queries
    
    def display_sample_results(self, duration_minutes=2):
        interval = 15
        iterations = (duration_minutes * 60) // interval
        
        for i in range(iterations):
            print(f"\nIteration {i+1}/{iterations} - {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 60)
            
            try:
                print("\nCity Metrics (Top 5 by AQI):")
                city_df = self.spark.sql("""
                    SELECT city, country, avg_aqi, max_aqi, who_violations, measurement_count
                    FROM city_metrics 
                    ORDER BY avg_aqi DESC 
                    LIMIT 5
                """)
                city_df.show(truncate=False)
                
                print("\nCountry Trends:")
                country_df = self.spark.sql("""
                    SELECT country, country_avg_aqi, unhealthy_percentage, hazardous_count
                    FROM country_trends 
                    ORDER BY country_avg_aqi DESC 
                    LIMIT 5
                """)
                country_df.show(truncate=False)
                
                print("\nCritical Alerts:")
                alert_df = self.spark.sql("""
                    SELECT city, AQI, alert_priority, alert_message
                    FROM health_alerts 
                    WHERE alert_priority <= 2
                    ORDER BY alert_timestamp DESC 
                    LIMIT 5
                """)
                alert_df.show(truncate=False)
                
            except Exception as e:
                print(f"Waiting for data: {str(e)}")
            
            if i < iterations - 1:
                time.sleep(interval)
    
    def stop_pipeline(self):
        print("\nStopping all queries...")
        self.writer.stop_all_queries()
        self.config.stop_spark()
        print("Pipeline stopped successfully!")

def main():
    parser = argparse.ArgumentParser(description='Air Quality Streaming Pipeline')
    parser.add_argument('--hdfs-path', 
                       default='hdfs://localhost:9000/user/student/air_quality_cleaned/valid_json',
                       help='HDFS path to cleaned data')
    parser.add_argument('--duration', 
                       type=int, 
                       default=2,
                       help='Duration to run pipeline (minutes)')
    parser.add_argument('--mode', 
                       choices=['demo', 'continuous'], 
                       default='demo',
                       help='Run mode: demo or continuous')
    
    args = parser.parse_args()
    
    pipeline = AirQualityStreamingPipeline(args.hdfs_path)
    
    try:
        queries = pipeline.run_streaming_operation()
        
        if args.mode == 'demo':
            pipeline.display_sample_results(args.duration)
            pipeline.stop_pipeline()
        else:
            print("\nRunning in continuous mode. Press Ctrl+C to stop...")
            for query in queries:
                if query and query.isActive:
                    query.awaitTermination()
                    
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        pipeline.stop_pipeline()
    except Exception as e:
        print(f"\nError: {str(e)}")
        pipeline.stop_pipeline()
        sys.exit(1)

if __name__ == "__main__":
    main()