# Author: Yoo Xin Wei

from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import *
import time
import os
import shutil

class StreamWriter:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.queries = {}
        
    def write_to_memory(self, stream_df, query_name, output_mode="append", trigger_interval="10 seconds"):
        query = stream_df.writeStream \
            .queryName(query_name) \
            .format("memory") \
            .outputMode(output_mode) \
            .trigger(processingTime=trigger_interval) \
            .start()
        
        self.queries[query_name] = query
        print(f"Started streaming query: {query_name}")
        return query
    
    def write_batch_to_parquet(self, stream_df, output_path, checkpoint_path, trigger_interval="10 seconds"):
        os.makedirs(output_path, exist_ok=True)
        os.makedirs(checkpoint_path, exist_ok=True)
        
        def process_batch(df, epoch_id):
            if df.count() > 0:
                temp_path = f"{output_path}/temp_{epoch_id}"
                final_path = f"{output_path}/current"
                
                df.coalesce(1).write.mode("overwrite").parquet(temp_path)
                
                if os.path.exists(final_path):
                    shutil.rmtree(final_path)
                os.rename(temp_path, final_path)
                
                print(f"Wrote batch {epoch_id} to {output_path} ({df.count()} records)")
        
        query = stream_df.writeStream \
            .foreachBatch(process_batch) \
            .trigger(processingTime=trigger_interval) \
            .option("checkpointLocation", checkpoint_path) \
            .start()
        
        self.queries[f"parquet_{output_path}"] = query
        print(f"Writing to Parquet: {output_path}")
        return query
    
    def write_append_to_parquet(self, stream_df, output_path, checkpoint_path, trigger_interval="5 seconds"):
        os.makedirs(output_path, exist_ok=True)
        os.makedirs(checkpoint_path, exist_ok=True)
        
        query = stream_df.writeStream \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode("append") \
            .trigger(processingTime=trigger_interval) \
            .start()
        
        self.queries[f"append_{output_path}"] = query
        print(f"Appending to Parquet: {output_path}")
        return query
    
    def write_to_console(self, stream_df, output_mode="append", trigger_interval="10 seconds", num_rows=20):
        query = stream_df.writeStream \
            .format("console") \
            .outputMode(output_mode) \
            .option("truncate", False) \
            .option("numRows", num_rows) \
            .trigger(processingTime=trigger_interval) \
            .start()
        
        return query
    
    def monitor_queries(self, duration_seconds=60, check_interval=5):
        start_time = time.time()
        
        while time.time() - start_time < duration_seconds:
            print("\n" + "="*50)
            print(f"Query Status at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*50)
            
            for name, query in self.queries.items():
                if query.isActive:
                    print(f"\n{name}:")
                    print(f"   Active: {query.isActive}")
                    if query.lastProgress:
                        print(f"   BatchId: {query.lastProgress.get('batchId', 'N/A')}")
                    
            time.sleep(check_interval)
            
        return True
    
    def stop_all_queries(self):
        for name, query in self.queries.items():
            if query.isActive:
                query.stop()
                print(f"Stopped query: {name}")
                
    def await_termination(self, query_name=None):
        if query_name and query_name in self.queries:
            self.queries[query_name].awaitTermination()
        else:
            for query in self.queries.values():
                if query.isActive:
                    query.awaitTermination()