# Author: 
import sys
from pyspark.sql import SparkSession

sys.path.append("/home/student/de-ass")

from Task2ProcessData.preprocess.standardizer import Standardizer
from Task2ProcessData.preprocess.fillers import Fillers
from Task2ProcessData.preprocess.imputers import Imputers
from Task2ProcessData.preprocess.fill_AQI import AQI
from Task2ProcessData.preprocess.rangeCheck import RangeGuards
from Task2ProcessData.preprocess.removeDuplicate import Deduplicator
from Task2ProcessData.preprocess.enrich import Enricher
from Task2ProcessData.validate.validator_pipeline import ValidatorPipeline
def main():
    spark = (SparkSession.builder
             .appName("AQI-Pipeline")
             .config("spark.sql.session.timeZone","UTC")
             .getOrCreate())

    in_path  = "hdfs://localhost:9000/global_air_quality/raw_copy/air_quality.jsonl"

    out_base = "hdfs://localhost:9000/user/student/air_quality_cleaned"

    df_raw = (spark.read.option("mode","PERMISSIVE").json(in_path))
    
    df = Standardizer.apply(df_raw)
    df = Fillers.fill_city_country(df)
    df = Fillers.fill_missing_date(df)
    df = Deduplicator.dedup_daily(df)
    df = RangeGuards.column_ranges(df, mode="nullify")
    df = Imputers.numeric(df)
    df = AQI.impute_aqi(df)
    df = Enricher.apply(df, buckets=True, who_flags=True)

    df_valid, df_invalid, df_invalid_labeled = ValidatorPipeline.run(df)
    
    (df_valid.coalesce(1)
             .write.mode("overwrite")
             .json(out_base + "/valid_json"))

    (df_invalid_labeled.coalesce(1)
                       .write.mode("overwrite")
                       .json(out_base + "/invalid_labeled_json"))


    print("Valid rows   :", df_valid.count())
    print("Invalid rows :", df_invalid_labeled.count())
    print("Output base  :", out_base)

if __name__ == "__main__":
    main()
