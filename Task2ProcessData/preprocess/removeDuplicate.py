# Author: 
from pyspark.sql import functions as F
class Deduplicator:

    @staticmethod
    def dedup_daily(df, watermark: str = "1 day"):
        if "event_date" not in df.columns:
            df = df.withColumn("event_date", F.to_date("ts"))
        else:
            df = df.withColumn("event_date", F.coalesce(F.col("event_date"), F.to_date("ts")))
        return (
            df
            .withWatermark("ts", watermark)  
            .dropDuplicates(["country", "city", "event_date"])
        )
