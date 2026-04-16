# Author: Kam Win Ni
import argparse
from pyspark.sql import SparkSession, functions as F
from pymongo import MongoClient, UpdateOne


class TransformAndLoad:
    def __init__(self, in_path: str, mongo_uri: str, db: str, coll: str, batch_size: int = 1000):
        self.in_path = in_path
        self.mongo_uri = mongo_uri
        self.db = db
        self.coll = coll
        self.batch_size = batch_size

    def run(self):
        spark = (
            SparkSession.builder
            .appName("AQI-HDFS-to-Mongo")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        df = spark.read.json(self.in_path)
        df = df.withColumn(
            "ts_utc",
            F.coalesce(F.to_timestamp("ts"), F.to_timestamp("event_date"))
        )

        keep = [
            "ts_utc", "city", "country",
            "AQI", "pm25_ugm3", "pm10_ugm3", "no2_ppb", "so2_ppb", "co_ppm", "o3_ppb",
            "temp_c", "humidity_pct", "wind_ms", "pm_ratio",
            "aqi_category", "aqi_bucket_pm25", "aqi_bucket_pm10", "aqi_bucket_no2",
            "aqi_bucket_so2", "aqi_bucket_co", "aqi_bucket_o3",
            "pm25_exceeds_who", "pm10_exceeds_who", "no2_exceeds_who",
            "so2_exceeds_who", "co_exceeds_who", "o3_exceeds_who",
            "who_compliant_all",
            "year", "month", "day", "season", "day_of_week", "hemisphere"
        ]
        df = df.select(*[c for c in keep if c in df.columns])

        mongo_uri, db_name, coll_name, batch_size = (
            self.mongo_uri, self.db, self.coll, self.batch_size
        )

        def write_partition(iter_rows):

            client = MongoClient(mongo_uri)
            coll = client[db_name][coll_name]
            ops = []

            for r in iter_rows:
                row = r.asDict(recursive=True)

                ts = row.get("ts_utc")
                if ts is None:
                    continue
                
                city = row.get("city")
                country = row.get("country")
                
                location = {
                    "city":city,
                    "country": country
                }
                
                if "hemisphere" in row and row["hemisphere"] is not None:
                    location["hemisphere"] = row["hemisphere"]

                dedup = f"{city}|{country}|{ts}"

                doc = {
                    "ts_utc": ts,
                    "location": location,
                    "measurements": {
                        "aqi": row.get("AQI"),
                        "pollutants": {
                            "pm25_ugm3": row.get("pm25_ugm3"),
                            "pm10_ugm3": row.get("pm10_ugm3"),
                            "no2_ppb": row.get("no2_ppb"),
                            "so2_ppb": row.get("so2_ppb"),
                            "co_ppm": row.get("co_ppm"),
                            "o3_ppb": row.get("o3_ppb"),
                        },
                        "weather": {
                            "temp_c": row.get("temp_c"),
                            "humidity_pct": row.get("humidity_pct"),
                            "wind_ms": row.get("wind_ms"),
                        },
                        "derived": {
                            "pm_ratio": row.get("pm_ratio"),
                        }
                    },
                    "categories": {
                        "aqi_category": row.get("aqi_category"),
                        "aqi_bucket_pm25": row.get("aqi_bucket_pm25"),
                        "aqi_bucket_pm10": row.get("aqi_bucket_pm10"),
                        "aqi_bucket_no2": row.get("aqi_bucket_no2"),
                        "aqi_bucket_so2": row.get("aqi_bucket_so2"),
                        "aqi_bucket_co": row.get("aqi_bucket_co"),
                        "aqi_bucket_o3": row.get("aqi_bucket_o3"),
                    },
                    "compliance": {
                        "who_pm25_exceed": row.get("pm25_exceeds_who"),
                        "who_pm10_exceed": row.get("pm10_exceeds_who"),
                        "who_no2_exceed":  row.get("no2_exceeds_who"),
                        "who_so2_exceed":  row.get("so2_exceeds_who"),
                        "who_co_exceed":   row.get("co_exceeds_who"),
                        "who_o3_exceed":   row.get("o3_exceeds_who"),
                        "who_all_compliant": row.get("who_compliant_all"),
                    },
                    "calendar": {
                        "year": row.get("year"),
                        "month": row.get("month"),
                        "day": row.get("day"),
                        "season": row.get("season"),
                        "dow_str": row.get("day_of_week"),
                    },
                        "dedup": dedup,
                }


                ops.append(UpdateOne({"dedup": dedup}, {"$set": doc}, upsert=True))
                if len(ops) >= batch_size:
                    coll.bulk_write(ops, ordered=False)
                    ops.clear()

            if ops:
                coll.bulk_write(ops, ordered=False)
            client.close()

        df.foreachPartition(write_partition)
        spark.stop()

    @staticmethod
    def from_cli():
        ap = argparse.ArgumentParser()
        ap.add_argument("--in", dest="in_path", required=True)
        ap.add_argument("--mongo-uri", required=True)
        ap.add_argument("--db", required=True)
        ap.add_argument("--coll", required=True)
        ap.add_argument("--batch-size", type=int, default=1000)
        args = ap.parse_args()
        return TransformAndLoad(args.in_path, args.mongo_uri, args.db, args.coll, args.batch_size)


if __name__ == "__main__":
    TransformAndLoad.from_cli().run()
