# Author: 
from pyspark.sql import functions as F
from pyspark.sql import Window as W

class Fillers:
    @staticmethod
    def city2country_map():
        return F.create_map(
            F.lit("New York"),F.lit("USA"), F.lit("Los Angeles"),F.lit("USA"),
            F.lit("Delhi"),F.lit("India"), F.lit("Paris"),F.lit("France"),
            F.lit("São Paulo"),F.lit("Brazil"), F.lit("Cairo"),F.lit("Egypt"),
        )
    @staticmethod
    def country2city_map():
        return F.create_map(
            F.lit("USA"),F.lit("New York"),
            F.lit("India"),F.lit("Delhi"),
            F.lit("France"),F.lit("Paris"),
            F.lit("Brazil"),F.lit("São Paulo"),
            F.lit("Egypt"),F.lit("Cairo"),
        )

    @staticmethod
    def fill_missing_date(df):
        df = df.withColumn("_idx", F.monotonically_increasing_id())
        w_ord = W.partitionBy("country","city").orderBy(F.col("_idx"))

        w_prev = w_ord.rowsBetween(W.unboundedPreceding, 0)
        w_next = w_ord.rowsBetween(0, W.unboundedFollowing)

        df = (df
              .withColumn("_rn", F.row_number().over(w_ord))
              .withColumn("_prev_ts", F.last("ts", ignorenulls=True).over(w_prev))
              .withColumn("_prev_rn", F.last(F.when(F.col("ts").isNotNull(), F.col("_rn")),
                                             ignorenulls=True).over(w_prev))
              .withColumn("_next_ts", F.first("ts", ignorenulls=True).over(w_next))
              .withColumn("_next_rn", F.first(F.when(F.col("ts").isNotNull(), F.col("_rn")),
                                              ignorenulls=True).over(w_next))
        )
        fill_from_prev = F.to_timestamp(
            F.date_add(F.to_date(F.col("_prev_ts")), F.col("_rn") - F.col("_prev_rn"))
        )
        fill_from_next = F.to_timestamp(
            F.date_add(F.to_date(F.col("_next_ts")), -(F.col("_next_rn") - F.col("_rn")))
        )

        df = df.withColumn(
            "ts",
            F.when(F.col("ts").isNull(), F.coalesce(fill_from_prev, fill_from_next)).otherwise(F.col("ts"))
        )
        return df.drop("_idx","_rn","_prev_ts","_prev_rn","_next_ts","_next_rn")

    @staticmethod
    def fill_city_country(df):
        # normalize key for map
        city_for_map = F.initcap(F.regexp_replace(F.trim(F.col("city")), r"\s+", " "))
        return (
            df
            .withColumn("country",
                F.coalesce(F.col("country"),
                           F.element_at(Fillers.city2country_map(), city_for_map)))
            .withColumn("city",
                F.coalesce(F.col("city"),
                           F.element_at(Fillers.country2city_map(), F.col("country"))))
        )
